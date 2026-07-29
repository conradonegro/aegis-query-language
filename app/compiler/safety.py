from sqlglot import exp

from app.compiler.models import SQLAst, ValidatedAST


class SafetyViolationError(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message

class UnsafeExpressionError(SafetyViolationError):
    """Raised when an explicitly allowed AST node violates semantic structural
    rules during translation."""
    pass

class SafetyPolicyViolationError(SafetyViolationError):
    """Raised when a column is referenced in a SQL clause its SafetyClassification
    prohibits."""

    def __init__(self, message: str) -> None:
        # Call Exception.__init__ directly to set args so str(exc) == message.
        # Cannot use @dataclass here because its generated __init__ never calls
        # super().__init__(message), leaving self.args empty and breaking
        # pytest.raises(match=...) and any caller doing str(exc).
        Exception.__init__(self, message)
        self.message = message

class SafetyEngine:
    """
    Implements a strict allow-list based structural validation on SQL.
    - Ensures root node is SELECT or UNION (of SELECTs).
    - Enforces explicit DENY-LIST on dangerous AST nodes.
    - Allows only approved scalar functions.
    """

    # These must NEVER exist anywhere in the AST
    DENY_LIST = (
        exp.Anonymous,
        exp.Command,
        exp.Execute,
        exp.Transaction,
        exp.Hint,
        exp.Pragma,
        exp.Drop,
        exp.Create,
        exp.Alter,
        exp.Insert,
        exp.Update,
        exp.Delete,
        exp.Grant,
        # Subqueries and CTEs were blocked in v1 but are now permitted.
        # Resource exhaustion (the original concern for recursive CTEs) is
        # mitigated by the statement_timeout enforced at execution time.
        # LLM output must never contain bind parameters or placeholders.
        # The translator parameterizes literals itself after safety validation;
        # a pre-translated Parameter/Placeholder is always LLM-injected and
        # would produce an unbound parameter at execution time.
        exp.Parameter,
        exp.Placeholder,
    )

    # Only these structural nodes are allowed beyond column lookups/literals
    # Note: the exact list may need tweaking during fuzzer tests, but we start strict.
    ALLOW_LIST = (
        exp.Select,
        exp.From,
        exp.Where,
        exp.Group,
        exp.Having,
        exp.Order,
        exp.Ordered,
        exp.Column,
        exp.Identifier,
        exp.Literal,
        exp.Boolean,
        exp.Var,
        exp.Table,
        exp.Join,
        exp.OnCondition,
        exp.Limit,
        exp.Offset,
        exp.Star,
        # Logical / Operators
        exp.And, exp.Or, exp.Not,
        exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE,
        exp.Like, exp.ILike, exp.In, exp.Between, exp.Is, exp.Null,
        exp.Paren,
        # Aggregations / Math
        exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max,
        exp.Distinct,
        exp.Coalesce, exp.Cast,
        # Arithmetic
        exp.Add, exp.Sub, exp.Mul, exp.Div, exp.Mod, exp.Neg,
        # CASE / conditional
        exp.Case, exp.If,
        # String functions
        exp.Upper, exp.Lower, exp.Trim, exp.Concat, exp.Substring, exp.Length,
        # Math functions
        exp.Round, exp.Floor, exp.Ceil, exp.Abs,
        # Null / comparison utilities
        exp.Nullif, exp.Greatest, exp.Least,
        # Subqueries and CTEs — resource limits enforced by statement_timeout
        exp.Subquery, exp.Exists,
        exp.With, exp.CTE, exp.Union,
        # Types
        exp.DataType,
        exp.Interval,
        # Functions
        exp.Extract,
        exp.StrToDate,       # TO_DATE(text, format) — PostgreSQL date parsing
        exp.TimestampTrunc,  # DATE_TRUNC('part', temporal) — PostgreSQL truncation
        exp.StrPosition,     # STRPOS(text, substring)
        exp.SplitPart,       # SPLIT_PART(text, delimiter, n)
        exp.DPipe,           # || string concatenation
        exp.CurrentDate,     # CURRENT_DATE
        exp.CurrentTimestamp,
        exp.ArrayAgg,        # ARRAY_AGG — read-only aggregation
        exp.GroupConcat,     # GROUP_CONCAT — renders as STRING_AGG in PG
        exp.RegexpLike,      # ~ regex match
        exp.NullSafeNEQ,     # IS DISTINCT FROM
        exp.Bracket,         # array subscript
        # Window functions — read-only analytics
        exp.Window,
        exp.RowNumber, exp.Rank, exp.DenseRank, exp.PercentRank,
        exp.Lag, exp.Lead, exp.Ntile,
        exp.FirstValue, exp.LastValue, exp.CumeDist,
        # Aggregate FILTER (WHERE ...) clause
        exp.Filter,
        # Alias
        exp.Alias, exp.ColumnPosition, exp.TableAlias, exp.Tuple
    )

    # exp.Anonymous is the parser's catch-all for functions sqlglot has no
    # dedicated node for. It stays in the DENY_LIST, with these specific
    # read-only functions exempted by name.
    ALLOWED_ANONYMOUS_FUNCTIONS = frozenset({"AGE"})

    def validate(self, ast: SQLAst) -> ValidatedAST:
        """Runs the validation rules. Raises SafetyViolationError if blocked."""
        tree = ast.tree
        if tree is None:
            raise SafetyViolationError("AST tree is empty.")

        if not isinstance(tree, (exp.Select, exp.Union)):
            raise SafetyViolationError(
                f"Root node must be SELECT or UNION, found {type(tree).__name__}"
            )

        # Walk the entire tree and check every node
        for node in tree.walk():
            # Extract just the specific node instance from the walk
            # tuple (yields (node, parent, key))
            if isinstance(node, tuple):
               node_inst = node[0]
            else:
               node_inst = node

            node_type = type(node_inst)

            if issubclass(node_type, self.DENY_LIST):
                 if (
                     isinstance(node_inst, exp.Anonymous)
                     and str(node_inst.this).upper()
                     in self.ALLOWED_ANONYMOUS_FUNCTIONS
                 ):
                     continue
                 raise SafetyViolationError(
                     f"Explicitly denied node type found: {node_type.__name__}"
                 )

            # Every node MUST explicitly be in our strict allow list
            if not issubclass(node_type, self.ALLOW_LIST):
                 raise SafetyViolationError(
                     f"Node type not in strict allow-list: {node_type.__name__}"
                 )

        # Implicit-join detection: a JOIN without ON/USING that was not
        # written as an explicit CROSS JOIN is almost always a missing join
        # condition (`FROM a, b`) and produces a silently wrong Cartesian
        # product. Explicit CROSS JOIN is deliberate (typically against a
        # single-row aggregate CTE) and its cost is bounded by
        # statement_timeout at execution.
        self._validate_joins(tree)

        return ValidatedAST(tree=tree)

    def _validate_joins(self, tree: exp.Expression) -> None:
        """Reject implicit joins, except provably single-row CTE pairings."""
        single_row_ctes = self._single_row_cte_aliases(tree)
        for join_node in tree.find_all(exp.Join):
            has_on = join_node.args.get("on") is not None
            has_using = join_node.args.get("using") is not None
            is_explicit_cross = join_node.kind == "CROSS"
            if has_on or has_using or is_explicit_cross:
                continue
            if self._joins_only_single_row_ctes(join_node, single_row_ctes):
                continue
            raise SafetyViolationError(
                "Implicit or cross JOIN detected: every JOIN must have an "
                "explicit ON or USING condition."
            )

    @staticmethod
    def _single_row_cte_aliases(tree: exp.Expression) -> set[str]:
        """Aliases of CTEs that provably return exactly one row.

        A SELECT whose projections are all aggregates and which has no GROUP
        BY returns exactly one row. Anything less certain is excluded, so the
        caller can only ever relax the join rule on a bounded shape.
        """
        aliases: set[str] = set()
        # sqlglot spells these args "with_"/"from_" in some versions.
        with_node = tree.args.get("with") or tree.args.get("with_")
        if not isinstance(with_node, exp.With):
            return aliases
        for cte in with_node.expressions:
            inner = cte.this
            if not isinstance(inner, exp.Select):
                continue
            if inner.args.get("group") is not None:
                continue
            projections = inner.expressions
            if not projections:
                continue
            if all(
                bool(list(proj.find_all(exp.AggFunc))) for proj in projections
            ):
                aliases.add(cte.alias_or_name)
        return aliases

    @staticmethod
    def _joins_only_single_row_ctes(
        join_node: exp.Join, single_row_ctes: set[str]
    ) -> bool:
        """True when this comma-join combines only single-row CTEs.

        The ratio pattern ("what is the difference between X and Y") pairs two
        scalar CTEs with a comma. Both sides being single-row makes the
        Cartesian product exactly one row, so none of the silent row
        explosion that makes comma joins dangerous can occur. Base tables are
        never eligible.
        """
        if not single_row_ctes:
            return False
        parent = join_node.parent
        if not isinstance(parent, exp.Select):
            return False
        sources: list[exp.Expression] = []
        from_node = parent.args.get("from") or parent.args.get("from_")
        if isinstance(from_node, exp.From):
            sources.append(from_node.this)
        for other in parent.args.get("joins") or []:
            sources.append(other.this)
        if not sources:
            return False
        return all(
            isinstance(src, exp.Table) and src.name in single_row_ctes
            for src in sources
        )
