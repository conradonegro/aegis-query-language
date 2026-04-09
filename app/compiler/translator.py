import re
from dataclasses import dataclass, field
from typing import Any

from sqlglot import exp

from app.api.models import TranslationRepair
from app.compiler.models import ExecutableQuery, ValidatedAST
from app.compiler.safety import SafetyPolicyViolationError, UnsafeExpressionError
from app.steward import AbstractRelationshipDef, RegistrySchema, SafetyClassification


class TranslationError(Exception):
    pass


@dataclass
class _SchemaLookupMaps:
    alias_to_physical_table: dict[str, str]
    alias_to_physical_col: dict[str, str]
    column_ownership: dict[str, set[str]]
    alias_to_datatype: dict[str, str]
    alias_to_safety: dict[str, SafetyClassification]


@dataclass
class _TableScope:
    dynamic_table_aliases: dict[str, str]
    tables_in_scope: set[str]
    table_runtime_prefixes: dict[str, set[str]] = field(
        default_factory=dict
    )


class DeterministicTranslator:
    """
    Translates an abstract validated AST into a parameterized physics execution
    query using copy-on-write to preserve the original AST.
    """

    def translate(
        self,
        ast: ValidatedAST,
        schema: RegistrySchema,
        abstract_query_hash: str = "default_hash",
        safety_version: str = "v1.0.0",
        row_limit: int = 1000,
        relationships: list[AbstractRelationshipDef] | None = None,
    ) -> ExecutableQuery:
        """Translates abstract AST into parameterized physical SQL."""
        tree = ast.tree.copy()
        repairs: list[TranslationRepair] = []

        cte_aliases = self._collect_cte_aliases(tree)
        cte_col_aliases = self._collect_cte_column_aliases(tree)
        cte_col_aliases |= self._collect_select_output_aliases(tree)

        if relationships:
            self._validate_join_graph(tree, relationships, cte_aliases)

        self._repair_where_aggregations(tree, repairs)

        maps = self._build_schema_maps(schema)
        scope = self._collect_table_scope(tree)
        literals, column_datatypes = self._walk_tree_nodes(
            tree, maps, scope, repairs, cte_aliases, cte_col_aliases
        )
        self._validate_temporal_expressions(tree, column_datatypes)
        temporal_literal_ids = self._collect_temporal_literal_ids(
            tree, column_datatypes
        )
        parameters = self._parameterize_literals(
            tree, literals, temporal_literal_ids
        )
        row_limit_applied = self._apply_row_limit(tree, row_limit, parameters)

        final_sql = tree.sql(dialect="postgres")
        # sqlglot renders positional params as $p1 in postgres dialect;
        # SQLAlchemy text() binding requires :p1 universally.
        final_sql = re.sub(r"\$(p\d+)\b", r":\1", final_sql)

        return ExecutableQuery(
            sql=final_sql,
            parameters=parameters,
            registry_version=schema.version,
            safety_engine_version=safety_version,
            abstract_query_hash=abstract_query_hash,
            row_limit_applied=row_limit_applied,
            translation_repairs=repairs,
        )

    # ------------------------------------------------------------------
    # Schema map construction
    # ------------------------------------------------------------------

    def _build_schema_maps(self, schema: RegistrySchema) -> _SchemaLookupMaps:
        """Builds O(1) lookup dictionaries from the registry schema."""
        alias_to_physical_table: dict[str, str] = {}
        alias_to_physical_col: dict[str, str] = {}
        column_ownership: dict[str, set[str]] = {}
        alias_to_datatype: dict[str, str] = {}
        alias_to_safety: dict[str, SafetyClassification] = {}

        for table in schema.tables:
            table_alias = table.alias.lower()
            alias_to_physical_table[table_alias] = table.physical_target
            for col in table.columns:
                col_alias = col.alias.lower()
                full_alias = f"{table_alias}.{col_alias}"
                alias_to_physical_col[full_alias] = col.physical_target
                alias_to_physical_col[col_alias] = col.physical_target
                alias_to_datatype[full_alias] = col.data_type.lower()
                alias_to_safety[full_alias] = col.safety
                if col_alias not in column_ownership:
                    column_ownership[col_alias] = set()
                column_ownership[col_alias].add(table_alias)

        return _SchemaLookupMaps(
            alias_to_physical_table=alias_to_physical_table,
            alias_to_physical_col=alias_to_physical_col,
            column_ownership=column_ownership,
            alias_to_datatype=alias_to_datatype,
            alias_to_safety=alias_to_safety,
        )

    def _collect_table_scope(self, tree: exp.Expression) -> _TableScope:
        """Extracts runtime table aliases and scope from the copied AST."""
        dynamic_table_aliases: dict[str, str] = {}
        tables_in_scope: set[str] = set()
        table_runtime_prefixes: dict[str, set[str]] = {}

        for table_node in tree.find_all(exp.Table):
            t_name = table_node.name.lower()
            tables_in_scope.add(t_name)
            if t_name not in table_runtime_prefixes:
                table_runtime_prefixes[t_name] = set()
            if table_node.alias:
                dynamic_table_aliases[table_node.alias.lower()] = t_name
                table_runtime_prefixes[t_name].add(table_node.alias.lower())

        return _TableScope(
            dynamic_table_aliases=dynamic_table_aliases,
            tables_in_scope=tables_in_scope,
            table_runtime_prefixes=table_runtime_prefixes,
        )

    # ------------------------------------------------------------------
    # AST tree walk and node resolution
    # ------------------------------------------------------------------

    def _walk_tree_nodes(
        self,
        tree: exp.Expression,
        maps: _SchemaLookupMaps,
        scope: _TableScope,
        repairs: list[TranslationRepair],
        cte_aliases: set[str],
        cte_col_aliases: set[str],
    ) -> tuple[list[exp.Literal], dict[int, str]]:
        """Walks the copied AST, mutating table/column nodes in-place."""
        literals_to_replace: list[exp.Literal] = []
        column_datatypes: dict[int, str] = {}

        for node in tree.walk():
            node_inst = node[0] if isinstance(node, tuple) else node
            if isinstance(node_inst, (exp.Parameter, exp.Placeholder)):
                # Belt-and-suspenders: SafetyEngine should have already rejected
                # these, but guard here in case the engine is bypassed in the future.
                raise TranslationError(
                    f"Pre-translation bind parameter found in AST"
                    f" ({type(node_inst).__name__}). LLM output must not contain"
                    f" placeholders; literals are parameterized by the translator."
                )
            if isinstance(node_inst, exp.Table):
                self._resolve_table_node(node_inst, maps, cte_aliases)
            elif isinstance(node_inst, exp.Column):
                c_name = node_inst.name.lower()
                t_prefix = node_inst.table.lower() if node_inst.table else ""
                if t_prefix:
                    self._resolve_column_with_prefix(
                        node_inst, c_name, t_prefix,
                        maps, scope, repairs, column_datatypes, cte_aliases,
                    )
                else:
                    self._resolve_column_without_prefix(
                        node_inst, c_name, maps, scope, column_datatypes,
                        cte_col_aliases,
                    )
            elif isinstance(node_inst, exp.Literal):
                literals_to_replace.append(node_inst)

        return literals_to_replace, column_datatypes

    def _resolve_table_node(
        self, node_inst: exp.Table, maps: _SchemaLookupMaps, cte_aliases: set[str]
    ) -> None:
        t_name = node_inst.name.lower()
        if t_name in cte_aliases:
            # CTE virtual table — leave identifier as-is; no physical resolution.
            return
        if t_name in maps.alias_to_physical_table:
            node_inst.set(
                "this",
                exp.Identifier(this=maps.alias_to_physical_table[t_name]),
            )
        else:
            raise TranslationError(
                f"Table '{t_name}' does not exist in schema context."
            )

    def _resolve_runtime_prefix(
        self,
        t_prefix: str,
        resolved_table: str,
        assigned_aliases: set[str],
        physical_table: str,
        c_name: str,
    ) -> str:
        """Returns the correct runtime table prefix for a column reference."""
        if t_prefix in assigned_aliases:
            return t_prefix
        if not assigned_aliases:
            return physical_table
        if len(assigned_aliases) == 1:
            return next(iter(assigned_aliases))
        raise TranslationError(
            f"Ambiguous target prefix '{t_prefix}' for column '{c_name}'"
            f" from self-joined table '{resolved_table}'"
            f" with aliases {list(assigned_aliases)}."
        )

    def _resolve_column_with_prefix(
        self,
        node_inst: exp.Column,
        c_name: str,
        t_prefix: str,
        maps: _SchemaLookupMaps,
        scope: _TableScope,
        repairs: list[TranslationRepair],
        column_datatypes: dict[int, str],
        cte_aliases: set[str],
    ) -> None:
        if t_prefix in scope.dynamic_table_aliases or t_prefix in scope.tables_in_scope:
            resolved_table = scope.dynamic_table_aliases.get(t_prefix, t_prefix)
            if resolved_table in cte_aliases:
                # CTE virtual table — column was validated inside the CTE body;
                # leave identifier as-is, no physical resolution or safety checks.
                return
            if resolved_table not in maps.alias_to_physical_table:
                raise TranslationError(
                    f"Table '{resolved_table}' does not exist in schema context."
                )
            full_alias = f"{resolved_table}.{c_name}"
            if full_alias not in maps.alias_to_physical_col:
                raise TranslationError(
                    f"Column '{full_alias}' does not exist in schema context."
                )
            node_inst.set(
                "this",
                exp.Identifier(this=maps.alias_to_physical_col[full_alias]),
            )
            column_datatypes[id(node_inst)] = maps.alias_to_datatype.get(
                full_alias, ""
            )
            assigned_aliases = scope.table_runtime_prefixes.get(
                resolved_table, set()
            )
            runtime_prefix = self._resolve_runtime_prefix(
                t_prefix,
                resolved_table,
                assigned_aliases,
                maps.alias_to_physical_table[resolved_table],
                c_name,
            )
            node_inst.set("table", exp.Identifier(this=runtime_prefix))
            self._check_column_safety(
                c_name,
                resolved_table,
                maps.alias_to_safety[full_alias],
                node_inst,
            )
        else:
            self._resolve_orphaned_prefix(
                node_inst, c_name, t_prefix, maps, scope, repairs, column_datatypes
            )

    def _resolve_orphaned_prefix(
        self,
        node_inst: exp.Column,
        c_name: str,
        t_prefix: str,
        maps: _SchemaLookupMaps,
        scope: _TableScope,
        repairs: list[TranslationRepair],
        column_datatypes: dict[int, str],
    ) -> None:
        """Resolves a column reference whose table prefix is not in scope."""
        owning_tables = maps.column_ownership.get(c_name, set())
        scoped_owning_tables = owning_tables.intersection(scope.tables_in_scope)
        if not scoped_owning_tables:
            raise TranslationError(
                f"Orphaned prefix '{t_prefix}' refers to column '{c_name}',"
                f" which does not belong to any table formally declared in scope"
                f" {list(scope.tables_in_scope)}."
            )
        if len(scoped_owning_tables) > 1:
            raise TranslationError(
                f"Ambiguous orphaned prefix '{t_prefix}' for column '{c_name}'."
                f" Exists in multiple scoped tables: {list(scoped_owning_tables)}."
            )
        unique_owning_table = scoped_owning_tables.pop()
        real_physical_table = maps.alias_to_physical_table[unique_owning_table]
        assigned_aliases = scope.table_runtime_prefixes.get(
            unique_owning_table, set()
        )
        if not assigned_aliases:
            runtime_prefix = real_physical_table
        elif len(assigned_aliases) == 1:
            runtime_prefix = next(iter(assigned_aliases))
        else:
            raise TranslationError(
                f"Cannot auto-heal orphaned prefix '{t_prefix}' for column '{c_name}'"
                f" due to ambiguous multiple aliases for table '{unique_owning_table}'"
                f" ({list(assigned_aliases)})."
            )
        owning_full_alias = f"{unique_owning_table}.{c_name}"
        repairs.append(
            TranslationRepair(
                type="orphaned_alias",
                original=f"{t_prefix}.{c_name}",
                resolved_to=(
                    f"{real_physical_table}"
                    f".{maps.alias_to_physical_col[owning_full_alias]}"
                ),
                reason=(
                    "Unique column ownership logically inferred over mapped structure."
                ),
            )
        )
        node_inst.set(
            "this",
            exp.Identifier(this=maps.alias_to_physical_col[owning_full_alias]),
        )
        node_inst.set("table", exp.Identifier(this=runtime_prefix))
        column_datatypes[id(node_inst)] = maps.alias_to_datatype.get(
            owning_full_alias, ""
        )
        self._check_column_safety(
            c_name,
            unique_owning_table,
            maps.alias_to_safety[owning_full_alias],
            node_inst,
        )

    def _resolve_column_without_prefix(
        self,
        node_inst: exp.Column,
        c_name: str,
        maps: _SchemaLookupMaps,
        scope: _TableScope,
        column_datatypes: dict[int, str],
        cte_col_aliases: set[str],
    ) -> None:
        if c_name in cte_col_aliases:
            # CTE-derived output column (declared with AS inside a CTE's SELECT).
            # It was validated when the CTE body was processed; no physical
            # resolution or safety check applies in the outer query.
            return
        owning_tables = maps.column_ownership.get(c_name, set())
        scoped_owning_tables = owning_tables.intersection(scope.tables_in_scope)
        if len(scoped_owning_tables) > 1:
            raise TranslationError(
                f"Ambiguous naked column '{c_name}'. Belongs to multiple scoped"
                f" tables: {list(scoped_owning_tables)}. Explicit aliasing required."
            )
        if len(scoped_owning_tables) == 1:
            unique_owning_table = scoped_owning_tables.pop()
            full_alias = f"{unique_owning_table}.{c_name}"
            node_inst.set(
                "this",
                exp.Identifier(this=maps.alias_to_physical_col[full_alias]),
            )
            column_datatypes[id(node_inst)] = maps.alias_to_datatype.get(
                full_alias, ""
            )
            self._check_column_safety(
                c_name,
                unique_owning_table,
                maps.alias_to_safety[full_alias],
                node_inst,
            )
        elif c_name in maps.alias_to_physical_col:
            raise TranslationError(
                f"Column '{c_name}' exists in the schema but its owning table"
                f" is not referenced in this query."
                f" Explicit table qualification required."
            )
        else:
            raise TranslationError(
                f"Column '{c_name}' does not exist in the schema context."
            )

    # ------------------------------------------------------------------
    # Post-resolution validation
    # ------------------------------------------------------------------

    def _validate_temporal_expressions(
        self, tree: exp.Expression, column_datatypes: dict[int, str]
    ) -> None:
        """Validates EXTRACT and INTERVAL nodes after physical resolution.

        EXTRACT requires a temporal *expression*, which may be either a bare
        column (whose datatype was recorded during the AST walk) or a CAST
        whose target type is temporal. Other forms are rejected at compile
        time so the LLM gets a useful error rather than a runtime crash.
        """
        for extract_node in tree.find_all(exp.Extract):
            source = extract_node.expression
            if any(extract_node.find_all(exp.Subquery, exp.Select, exp.Window)):
                raise UnsafeExpressionError(
                    "Nested subqueries or window constructs are strictly blocked"
                    " inside EXTRACT."
                )
            if not self._resolves_to_temporal(source, column_datatypes):
                raise UnsafeExpressionError(
                    f"EXTRACT requires a temporal expression; got"
                    f" '{type(source).__name__}' that does not resolve to a"
                    f" temporal type."
                )
        for interval_node in tree.find_all(exp.Interval):
            if any(interval_node.find_all(exp.Subquery, exp.Select, exp.Window)):
                raise UnsafeExpressionError(
                    "Nested subqueries or window constructs are strictly blocked"
                    " inside INTERVAL."
                )

    def _resolves_to_temporal(
        self, expr: exp.Expression, column_datatypes: dict[int, str]
    ) -> bool:
        """Returns True iff `expr` is guaranteed to evaluate to a temporal value.

        Recognized forms:
          - exp.Column whose resolved datatype contains a temporal type token
          - exp.Cast whose target DataType is one of the temporal types

        Not recognized (returns False — caller raises): arithmetic, anonymous
        function calls, literals, parameters, anything else.
        """
        if isinstance(expr, exp.Column):
            dtype = column_datatypes.get(id(expr), "")
            return any(t in dtype for t in self._TEMPORAL_TYPES)
        if isinstance(expr, exp.Cast):
            target = expr.to
            if not isinstance(target, exp.DataType):
                return False
            target_name = target.this.name.lower() if target.this else ""
            return target_name in self._TEMPORAL_TYPES
        return False

    # ------------------------------------------------------------------
    # Literal parameterization and row-limit enforcement
    # ------------------------------------------------------------------

    _TEMPORAL_TYPES: frozenset[str] = frozenset(
        {"date", "time", "timestamp", "timestamptz", "datetime", "interval"}
    )

    def _collect_temporal_literal_ids(
        self, tree: exp.Expression, column_datatypes: dict[int, str]
    ) -> set[int]:
        """Returns id()s of literals that must be left inline due to temporal context.

        asyncpg infers bind parameter types from the column being compared. When
        a string literal like '2012/8/24' is parameterized and compared to a DATE
        column, asyncpg expects a Python datetime.date object and crashes with
        ``AttributeError: 'str' object has no attribute 'toordinal'``.

        This pass runs after _walk_tree_nodes has populated column_datatypes, so
        the datatype of every resolved Column node is already known. Skipping
        parameterization for temporal literals is safe for the same reason
        numeric literals are left inline: the values are LLM-generated and
        PostgreSQL will reject malformed date strings at parse time.

        Covered comparison forms: binary (EQ/NEQ/GT/GTE/LT/LTE), BETWEEN, IN.
        """
        ids: set[int] = set()
        ids |= self._temporal_ids_from_binary(tree, column_datatypes)
        ids |= self._temporal_ids_from_between(tree, column_datatypes)
        ids |= self._temporal_ids_from_in(tree, column_datatypes)
        ids |= self._temporal_ids_from_cast(tree, column_datatypes)
        return ids

    def _col_is_temporal(
        self, col: exp.Column, column_datatypes: dict[int, str]
    ) -> bool:
        dtype = column_datatypes.get(id(col), "")
        return any(t in dtype for t in self._TEMPORAL_TYPES)

    def _expr_is_temporal(
        self, expr: exp.Expression, column_datatypes: dict[int, str]
    ) -> bool:
        """True if `expr` resolves to a temporal type — bare column OR CAST."""
        if isinstance(expr, exp.Column):
            return self._col_is_temporal(expr, column_datatypes)
        return self._resolves_to_temporal(expr, column_datatypes)

    def _temporal_ids_from_binary(
        self, tree: exp.Expression, column_datatypes: dict[int, str]
    ) -> set[int]:
        """Marks literals in binary comparisons against temporal expressions."""
        result: set[int] = set()
        for cmp in tree.find_all(exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE):
            left, right = cmp.left, cmp.right
            if isinstance(right, exp.Literal):
                if self._expr_is_temporal(left, column_datatypes):
                    result.add(id(right))
            elif isinstance(left, exp.Literal):
                if self._expr_is_temporal(right, column_datatypes):
                    result.add(id(left))
        return result

    def _temporal_ids_from_between(
        self, tree: exp.Expression, column_datatypes: dict[int, str]
    ) -> set[int]:
        """Marks literals in BETWEEN bounds against temporal expressions."""
        result: set[int] = set()
        for between in tree.find_all(exp.Between):
            subject = between.this
            if not self._expr_is_temporal(subject, column_datatypes):
                continue
            low = between.args.get("low")
            high = between.args.get("high")
            if isinstance(low, exp.Literal):
                result.add(id(low))
            if isinstance(high, exp.Literal):
                result.add(id(high))
        return result

    def _temporal_ids_from_in(
        self, tree: exp.Expression, column_datatypes: dict[int, str]
    ) -> set[int]:
        """Marks literals inside IN (...) lists against temporal expressions."""
        result: set[int] = set()
        for in_node in tree.find_all(exp.In):
            subject = in_node.this
            if not self._expr_is_temporal(subject, column_datatypes):
                continue
            for expr in in_node.expressions:
                if isinstance(expr, exp.Literal):
                    result.add(id(expr))
        return result

    def _temporal_ids_from_cast(
        self, tree: exp.Expression, column_datatypes: dict[int, str]
    ) -> set[int]:
        """Marks literals wrapped in CAST(literal AS temporal_type).

        When the LLM writes ``CAST('2012-01-01' AS DATE)``, the literal is
        a child of the Cast node, not a direct operand of a comparison.
        asyncpg infers the Cast target type for the parameter and crashes
        with 'toordinal' when given a plain string. Leaving the literal
        inline lets PostgreSQL handle the cast safely.
        """
        result: set[int] = set()
        for cast_node in tree.find_all(exp.Cast):
            if not self._resolves_to_temporal(cast_node, column_datatypes):
                continue
            inner = cast_node.this
            if isinstance(inner, exp.Literal):
                result.add(id(inner))
        return result

    def _parameterize_literals(
        self,
        tree: exp.Expression,
        literals: list[exp.Literal],
        temporal_literal_ids: set[int],
    ) -> dict[str, Any]:
        """Replaces string literal values with named query parameters.

        Two categories of literals are intentionally left inline:

        - Numeric literals: binding integers as parameters causes asyncpg
          DataError when PostgreSQL infers a TEXT parameter type from context
          (e.g. ``THEN 1`` inside a CASE expression).
        - Temporal literals: string literals compared against DATE/TIME/TIMESTAMP
          columns cause asyncpg to infer the parameter type as the column's type
          and call ``.toordinal()`` on a Python str, crashing with AttributeError.
          Leaving them inline lets PostgreSQL parse and validate them directly.

        Both categories carry no SQL-injection risk: numerics cannot contain
        injection syntax, and temporal literals that fail PostgreSQL's date parser
        simply raise a DataError rather than executing.
        """
        parameters: dict[str, Any] = {}
        param_counter = 1
        for node_inst in literals:
            if node_inst.is_number:
                continue
            if id(node_inst) in temporal_literal_ids:
                continue
            param_name = f"p{param_counter}"
            parameters[param_name] = node_inst.this
            node_inst.replace(exp.Parameter(this=exp.var(param_name)))
            param_counter += 1
        return parameters

    def _apply_row_limit(
        self,
        tree: exp.Expression,
        row_limit: int,
        parameters: dict[str, Any] | None = None,
    ) -> bool:
        """Enforces a hard row-limit cap on every query.

        Injects LIMIT row_limit when none exists, and clamps any existing
        LIMIT that exceeds row_limit. Applies to all query types including
        aggregations and GROUP BY — LIMIT is semantically valid on any SELECT
        and is the only way to guarantee the documented cap.

        parameters: the already-built parameter dict from _parameterize_literals.
            When provided, an existing LIMIT that was already parameterized
            (e.g. :p1) is clamped by updating the parameter value in-place
            rather than replacing the AST node.
        """
        existing_limit_node = tree.args.get("limit")
        if existing_limit_node is None:
            tree.set(
                "limit",
                exp.Limit(expression=exp.Literal.number(row_limit)),
            )
            return True

        # Clamp an existing LIMIT that exceeds the configured maximum.
        # The limit expression may be a Literal (not yet parameterized) or an
        # exp.Parameter (already replaced by _parameterize_literals).
        limit_expr = existing_limit_node.expression
        try:
            if isinstance(limit_expr, exp.Literal):
                supplied = int(limit_expr.this)
                if supplied > row_limit:
                    tree.set(
                        "limit",
                        exp.Limit(expression=exp.Literal.number(row_limit)),
                    )
                    return True
            elif (
                isinstance(limit_expr, exp.Parameter)
                and parameters is not None
            ):
                # Already parameterized — clamp the bound value in-place.
                param_name = limit_expr.this.name
                supplied = int(parameters[param_name])
                if supplied > row_limit:
                    parameters[param_name] = row_limit
                    return True
            else:
                # Unknown expression form — overwrite conservatively.
                tree.set(
                    "limit",
                    exp.Limit(expression=exp.Literal.number(row_limit)),
                )
                return True
        except (AttributeError, KeyError, ValueError):
            # Cannot parse the limit expression — overwrite with the safe cap.
            tree.set(
                "limit",
                exp.Limit(expression=exp.Literal.number(row_limit)),
            )
            return True

        return False

    # ------------------------------------------------------------------
    # JOIN hallucination guard
    # ------------------------------------------------------------------

    def _validate_join_graph(
        self,
        tree: exp.Expression,
        relationships: list[AbstractRelationshipDef],
        cte_aliases: set[str],
    ) -> None:
        """
        Validates that every explicit JOIN ON condition references a declared
        relationship edge. Runs on the abstract AST before physical substitution.
        JOIN conditions referencing CTE virtual tables are skipped — their
        column safety was already validated inside the CTE body.
        """
        sql_alias_to_table: dict[str, str] = {}
        for table_node in tree.find_all(exp.Table):
            t_name = table_node.name.lower()
            sql_alias_to_table[t_name] = t_name
            if table_node.alias:
                sql_alias_to_table[table_node.alias.lower()] = t_name

        declared_edges = self._build_edge_index(relationships)

        for join_node in tree.find_all(exp.Join):
            on_clause = join_node.args.get("on")
            if on_clause is None:
                raise TranslationError(
                    "JOIN without an explicit ON clause is not permitted."
                    " All JOINs must reference a declared relationship"
                    " via a column-equality condition."
                )
            found = self._validate_join_on_clause(
                on_clause, sql_alias_to_table, declared_edges, cte_aliases
            )
            if not found:
                raise TranslationError(
                    "JOIN ON clause contains no column-equality condition that"
                    " matches a declared relationship. Non-equality, literal,"
                    " or unqualified JOIN predicates are not permitted."
                )

    def _validate_join_on_clause(
        self,
        on_clause: exp.Expression,
        sql_alias_to_table: dict[str, str],
        declared_edges: set[frozenset[str]],
        cte_aliases: set[str],
    ) -> bool:
        """Checks every EQ node in a JOIN ON clause against the declared edge index.

        Returns True if at least one valid column-equality condition was found.
        Raises TranslationError for undeclared non-CTE edge pairs.
        """
        found = False
        for eq_node in on_clause.find_all(exp.EQ):
            left, right = eq_node.left, eq_node.right
            if not isinstance(left, exp.Column) or not isinstance(right, exp.Column):
                continue
            left_ref = self._resolve_col_join_ref(left, sql_alias_to_table)
            right_ref = self._resolve_col_join_ref(right, sql_alias_to_table)
            if left_ref is None or right_ref is None:
                continue
            left_table = left_ref.split(".")[0]
            right_table = right_ref.split(".")[0]
            if left_table in cte_aliases or right_table in cte_aliases:
                found = True
                continue
            pair: frozenset[str] = frozenset({left_ref, right_ref})
            if pair not in declared_edges:
                raise TranslationError(
                    f"JOIN condition '{left_ref} = {right_ref}' does not"
                    f" correspond to any declared relationship in the schema."
                    f" Hallucinated JOIN blocked."
                )
            found = True
        return found

    @staticmethod
    def _collect_cte_aliases(tree: exp.Expression) -> set[str]:
        """Returns the lowercased alias names of all CTEs defined in the query."""
        aliases: set[str] = set()
        for cte in tree.find_all(exp.CTE):
            alias = cte.alias
            if alias:
                aliases.add(alias.lower())
        return aliases

    @staticmethod
    def _collect_cte_column_aliases(tree: exp.Expression) -> set[str]:
        """Returns the lowercased AS-declared column aliases from CTE SELECT lists.

        Only top-level expressions in each CTE's SELECT are collected — nested
        aliases inside subexpressions are intentionally excluded to avoid
        masking real schema column names.

        These names must be left unresolved in the outer query: they are virtual
        columns produced by the CTE and have no physical counterpart in the schema.
        """
        aliases: set[str] = set()
        for cte in tree.find_all(exp.CTE):
            body = cte.this
            if not isinstance(body, exp.Select):
                continue
            for expr in body.expressions:
                if isinstance(expr, exp.Alias):
                    aliases.add(expr.alias.lower())
        return aliases

    @staticmethod
    def _collect_select_output_aliases(tree: exp.Expression) -> set[str]:
        """Returns the lowercased AS-declared aliases from all SELECT projections.

        In SQL, ORDER BY and HAVING may reference a SELECT alias (e.g.
        ``ORDER BY total_consumption`` where ``SUM(x) AS total_consumption``
        is in the SELECT list). These are virtual columns with no physical
        counterpart; the translator must bypass them the same way it bypasses
        CTE output aliases.

        This covers ALL Select nodes in the tree (top-level, CTEs, subqueries)
        so the set may overlap with _collect_cte_column_aliases. That is safe —
        duplicates in a set are idempotent and the bypass logic is identical.
        """
        aliases: set[str] = set()
        for select_node in tree.find_all(exp.Select):
            for expr in select_node.expressions:
                if isinstance(expr, exp.Alias):
                    aliases.add(expr.alias.lower())
        return aliases

    @staticmethod
    def _build_edge_index(
        relationships: list[AbstractRelationshipDef],
    ) -> set[frozenset[str]]:
        """Builds a frozenset edge index for O(1) pair lookup."""
        declared_edges: set[frozenset[str]] = set()
        for rel in relationships:
            if rel.source_column and rel.target_column:
                declared_edges.add(
                    frozenset({
                        f"{rel.source_table}.{rel.source_column}",
                        f"{rel.target_table}.{rel.target_column}",
                    })
                )
        return declared_edges

    @staticmethod
    def _resolve_col_join_ref(
        col: exp.Column, sql_alias_to_table: dict[str, str]
    ) -> str | None:
        """Resolves a JOIN ON column reference to its abstract table-qualified name."""
        c_name = col.name.lower()
        t_prefix = col.table.lower() if col.table else ""
        if not t_prefix:
            return None
        abstract_table = sql_alias_to_table.get(t_prefix, t_prefix)
        return f"{abstract_table}.{c_name}"

    # ------------------------------------------------------------------
    # Column SQL context inspection
    # ------------------------------------------------------------------

    def _get_column_sql_context(self, col_node: exp.Column) -> set[str]:
        """
        Returns the set of SQL contexts a column participates in.
        Uses sqlglot's native find_ancestor for transparent traversal.
        """
        contexts: set[str] = set()

        if col_node.find_ancestor(exp.AggFunc):
            contexts.add("aggregation")

        clause = col_node.find_ancestor(
            exp.Where, exp.Group, exp.Having, exp.Join, exp.Order, exp.Select
        )
        if isinstance(clause, exp.Where):
            contexts.add("where")
        elif isinstance(clause, exp.Group):
            contexts.add("group_by")
        elif isinstance(clause, exp.Having):
            contexts.add("having")
        elif isinstance(clause, exp.Join):
            contexts.add("join")
        elif isinstance(clause, exp.Order):
            contexts.add("order_by")
        elif isinstance(clause, exp.Select):
            contexts.add("select")

        return contexts

    def _check_column_safety(
        self,
        col_alias: str,
        table_alias: str,
        safety: SafetyClassification,
        col_node: exp.Column,
    ) -> None:
        """
        Raises SafetyPolicyViolationError if the column is used in a SQL clause
        its SafetyClassification does not permit.
        """
        contexts = self._get_column_sql_context(col_node)
        label = f"'{table_alias}.{col_alias}'"

        if "aggregation" in contexts:
            if not safety.aggregation_allowed:
                raise SafetyPolicyViolationError(
                    message=(
                        f"Column {label} is not permitted inside aggregation functions."
                    )
                )
        else:
            if "select" in contexts and not safety.allowed_in_select:
                raise SafetyPolicyViolationError(
                    message=f"Column {label} is not permitted in SELECT."
                )
            if "order_by" in contexts and not safety.allowed_in_select:
                raise SafetyPolicyViolationError(
                    message=f"Column {label} is not permitted in ORDER BY."
                )
            if "where" in contexts and not safety.allowed_in_where:
                raise SafetyPolicyViolationError(
                    message=f"Column {label} is not permitted in WHERE conditions."
                )
            if "group_by" in contexts and not safety.allowed_in_group_by:
                raise SafetyPolicyViolationError(
                    message=f"Column {label} is not permitted in GROUP BY."
                )
            if "join" in contexts and not safety.join_participation_allowed:
                raise SafetyPolicyViolationError(
                    message=f"Column {label} is not permitted in JOIN conditions."
                )
            if "having" in contexts and not safety.allowed_in_where:
                raise SafetyPolicyViolationError(
                    message=f"Column {label} is not permitted in HAVING conditions."
                )

    # ------------------------------------------------------------------
    # WHERE → HAVING repair for aggregate conditions
    # ------------------------------------------------------------------

    def _extract_conjunctions(
        self, node: exp.Expression
    ) -> list[exp.Expression]:
        """Flattens an AND boolean tree into a list of its leaf expressions."""
        if isinstance(node, exp.And):
            return (
                self._extract_conjunctions(node.left)
                + self._extract_conjunctions(node.right)
            )
        return [node]

    @staticmethod
    def _column_is_inside_agg(col: exp.Column) -> bool:
        """Returns True if the column is nested inside any aggregation function."""
        current: exp.Expression | None = col.parent
        while current:
            if isinstance(current, exp.AggFunc):
                return True
            current = current.parent
        return False

    def _classify_conjunctions(
        self, conjunctions: list[exp.Expression]
    ) -> tuple[list[exp.Expression], list[exp.Expression]]:
        """Splits conjunctions into WHERE-safe and HAVING-bound groups."""
        where_conditions: list[exp.Expression] = []
        having_conditions: list[exp.Expression] = []
        for c in conjunctions:
            has_agg = any(c.find_all(exp.AggFunc))
            has_window = any(c.find_all(exp.Window))
            has_subquery = any(c.find_all(exp.Select, exp.Subquery))
            if has_window or has_subquery:
                where_conditions.append(c)
                continue
            if has_agg:
                has_naked = any(
                    not self._column_is_inside_agg(col)
                    for col in c.find_all(exp.Column)
                )
                if has_naked:
                    where_conditions.append(c)
                else:
                    having_conditions.append(c)
            else:
                where_conditions.append(c)
        return where_conditions, having_conditions

    def _repair_where_aggregations(
        self, tree: exp.Expression, repairs: list[TranslationRepair]
    ) -> None:
        """
        Safely extracts pure aggregate conditions from the WHERE clause and
        relocates them to the HAVING clause.
        """
        where_node = tree.args.get("where")
        if not where_node:
            return

        condition = where_node.this

        # Security: any OR in the WHERE clause makes the rewrite unsafe.
        if any(condition.find_all(exp.Or)):
            return

        conjunctions = self._extract_conjunctions(condition)
        where_conditions, having_conditions = self._classify_conjunctions(
            conjunctions
        )

        if not having_conditions:
            return

        if where_conditions:
            tree.set("where", exp.Where(this=exp.and_(*where_conditions)))
        else:
            tree.set("where", None)

        existing_having = tree.args.get("having")
        new_having_expr = exp.and_(*having_conditions)
        if existing_having:
            new_having_expr = exp.and_(existing_having.this, new_having_expr)

        tree.set("having", exp.Having(this=new_having_expr))
        repairs.append(
            TranslationRepair(
                type="where_aggregation_relocation",
                original=condition.sql(dialect="postgres"),
                resolved_to="Split into WHERE and HAVING",
                reason=(
                    "Pure aggregate condition found in WHERE clause."
                    " Securely relocated to HAVING."
                ),
            )
        )
