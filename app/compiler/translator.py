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

        if relationships:
            self._validate_join_graph(tree, relationships)

        self._repair_where_aggregations(tree, repairs)

        maps = self._build_schema_maps(schema)
        scope = self._collect_table_scope(tree)
        literals, column_datatypes = self._walk_tree_nodes(
            tree, maps, scope, repairs
        )
        self._validate_temporal_expressions(tree, column_datatypes)
        parameters = self._parameterize_literals(tree, literals)
        row_limit_applied = self._apply_row_limit(tree, row_limit)

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
    ) -> tuple[list[exp.Literal], dict[int, str]]:
        """Walks the copied AST, mutating table/column nodes in-place."""
        literals_to_replace: list[exp.Literal] = []
        column_datatypes: dict[int, str] = {}

        for node in tree.walk():
            node_inst = node[0] if isinstance(node, tuple) else node
            if isinstance(node_inst, exp.Table):
                self._resolve_table_node(node_inst, maps)
            elif isinstance(node_inst, exp.Column):
                c_name = node_inst.name.lower()
                t_prefix = node_inst.table.lower() if node_inst.table else ""
                if t_prefix:
                    self._resolve_column_with_prefix(
                        node_inst, c_name, t_prefix,
                        maps, scope, repairs, column_datatypes,
                    )
                else:
                    self._resolve_column_without_prefix(
                        node_inst, c_name, maps, scope, column_datatypes
                    )
            elif isinstance(node_inst, exp.Literal):
                literals_to_replace.append(node_inst)

        return literals_to_replace, column_datatypes

    def _resolve_table_node(
        self, node_inst: exp.Table, maps: _SchemaLookupMaps
    ) -> None:
        t_name = node_inst.name.lower()
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
    ) -> None:
        if t_prefix in scope.dynamic_table_aliases or t_prefix in scope.tables_in_scope:
            resolved_table = scope.dynamic_table_aliases.get(t_prefix, t_prefix)
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
    ) -> None:
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
        """Validates EXTRACT and INTERVAL nodes after physical resolution."""
        temporal_types = {"timestamp", "date", "datetime", "time", "interval"}
        for extract_node in tree.find_all(exp.Extract):
            source = extract_node.expression
            if not isinstance(source, exp.Column):
                raise UnsafeExpressionError(
                    f"EXTRACT numeric target must be natively bound to a column,"
                    f" found '{type(source).__name__}'."
                )
            if any(extract_node.find_all(exp.Subquery, exp.Select, exp.Window)):
                raise UnsafeExpressionError(
                    "Nested subqueries or window constructs are strictly blocked"
                    " inside EXTRACT."
                )
            dtype = column_datatypes.get(id(source), "")
            if not any(t in dtype for t in temporal_types):
                raise UnsafeExpressionError(
                    f"EXTRACT operations are only permitted on temporal columns."
                    f" Resolved column '{source.name}' is of type '{dtype}'."
                )
        for interval_node in tree.find_all(exp.Interval):
            if any(interval_node.find_all(exp.Subquery, exp.Select, exp.Window)):
                raise UnsafeExpressionError(
                    "Nested subqueries or window constructs are strictly blocked"
                    " inside INTERVAL."
                )

    # ------------------------------------------------------------------
    # Literal parameterization and row-limit enforcement
    # ------------------------------------------------------------------

    def _parameterize_literals(
        self, tree: exp.Expression, literals: list[exp.Literal]
    ) -> dict[str, Any]:
        """Replaces literal values with named query parameters."""
        parameters: dict[str, Any] = {}
        param_counter = 1
        for node_inst in literals:
            param_name = f"p{param_counter}"
            if node_inst.is_string:
                parameters[param_name] = node_inst.this
            elif node_inst.is_number:
                parameters[param_name] = (
                    float(node_inst.this)
                    if "." in node_inst.this
                    else int(node_inst.this)
                )
            else:
                parameters[param_name] = node_inst.this
            node_inst.replace(exp.Parameter(this=exp.var(param_name)))
            param_counter += 1
        return parameters

    def _apply_row_limit(self, tree: exp.Expression, row_limit: int) -> bool:
        """Injects a LIMIT clause when the query has no aggregation or GROUP BY."""
        is_aggregated = any(tree.find_all(exp.AggFunc))
        has_groupby = tree.args.get("group") is not None
        if not is_aggregated and not has_groupby:
            if not tree.args.get("limit"):
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
    ) -> None:
        """
        Validates that every explicit JOIN ON condition references a declared
        relationship edge. Runs on the abstract AST before physical substitution.
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
            found_declared_edge = False
            for eq_node in on_clause.find_all(exp.EQ):
                left, right = eq_node.left, eq_node.right
                if not isinstance(left, exp.Column) or not isinstance(
                    right, exp.Column
                ):
                    continue
                left_ref = self._resolve_col_join_ref(left, sql_alias_to_table)
                right_ref = self._resolve_col_join_ref(right, sql_alias_to_table)
                if left_ref is None or right_ref is None:
                    continue
                pair: frozenset[str] = frozenset({left_ref, right_ref})
                if pair not in declared_edges:
                    raise TranslationError(
                        f"JOIN condition '{left_ref} = {right_ref}' does not"
                        f" correspond to any declared relationship in the schema."
                        f" Hallucinated JOIN blocked."
                    )
                found_declared_edge = True
            if not found_declared_edge:
                raise TranslationError(
                    "JOIN ON clause contains no column-equality condition that"
                    " matches a declared relationship. Non-equality, literal,"
                    " or unqualified JOIN predicates are not permitted."
                )

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
