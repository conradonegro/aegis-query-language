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
    - Ensures root node is Select.
    - Blocks Subqueries and CTEs.
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
        exp.Subquery,     # v1 Blocks subqueries
        exp.CTE,          # v1 Blocks CTEs (With clauses)
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
        # Types
        exp.DataType,
        exp.Interval,
        # Functions
        exp.Extract,
        # Alias
        exp.Alias, exp.ColumnPosition, exp.TableAlias, exp.Tuple
    )

    def validate(self, ast: SQLAst) -> ValidatedAST:
        """Runs the validation rules. Raises SafetyViolationError if blocked."""
        tree = ast.tree
        if tree is None:
            raise SafetyViolationError("AST tree is empty.")

        if not isinstance(tree, exp.Select):
            raise SafetyViolationError(
                f"Root node must be SELECT, found {type(tree).__name__}"
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
                 raise SafetyViolationError(
                     f"Explicitly denied node type found: {node_type.__name__}"
                 )

            # Every node MUST explicitly be in our strict allow list
            if not issubclass(node_type, self.ALLOW_LIST):
                 raise SafetyViolationError(
                     f"Node type not in strict allow-list: {node_type.__name__}"
                 )

        # Implicit cross-join detection: any JOIN without an explicit ON or USING
        # condition is a cross-product (either `FROM a, b` parsed as a comma join,
        # or `... CROSS JOIN b`). These are blocked categorically because they
        # bypass relationship graph validation.
        for join_node in tree.find_all(exp.Join):
            has_on = join_node.args.get("on") is not None
            has_using = join_node.args.get("using") is not None
            if not has_on and not has_using:
                raise SafetyViolationError(
                    "Implicit or cross JOIN detected: every JOIN must have an "
                    "explicit ON or USING condition."
                )

        return ValidatedAST(tree=tree)
