from typing import Any

from sqlglot import exp

from app.api.models import TranslationRepair
from app.compiler.models import ExecutableQuery, ValidatedAST
from app.compiler.safety import UnsafeExpressionError, SafetyPolicyViolationError
from app.steward import RegistrySchema
from app.steward.models import AbstractRelationshipDef


class TranslationError(Exception):
    pass


class DeterministicTranslator:
    """
    Translates an abstract validated AST into a parameterized physics execution query
    using copy-on-write to preserve the original AST.
    """

    def translate(
        self, ast: ValidatedAST, schema: RegistrySchema,
        abstract_query_hash: str = "default_hash", safety_version: str = "v1.0.0",
        row_limit: int = 1000,
        relationships: list[AbstractRelationshipDef] | None = None,
    ) -> ExecutableQuery:
        """Translates abstract AST into parameterized physical SQL."""
        from app.api.models import TranslationRepair

        # 1. Copy-on-write
        tree = ast.tree.copy()
        repairs: list[TranslationRepair] = []

        # 2. Relationship graph validation — must run on the abstract AST before any
        #    physical substitution so column/table names still match schema aliases.
        if relationships:
            self._validate_join_graph(tree, relationships)

        # 3. Structural AST Repairs (e.g., WHERE SUM > 10 -> HAVING SUM > 10)
        self._repair_where_aggregations(tree, repairs)

        # 3. Build fast lookup map O(1)
        alias_to_physical_table: dict[str, str] = {}
        alias_to_physical_col: dict[str, str] = {}
        
        # We need a strict map tracking which tables own which columns to resolve orchestrations
        column_ownership: dict[str, set[str]] = {}
        alias_to_datatype: dict[str, str] = {}
        alias_to_safety: dict[str, "SafetyClassification"] = {}

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

        column_datatypes: dict[int, str] = {} # Map id(exp.Column) -> data_type for validation

        parameters: dict[str, Any] = {}
        param_counter = 1

        # 4. Walk and mutate the COPIED tree
        literals_to_replace = []
        
        # 4a. Extract dynamic Table Aliases (e.g. `orders AS o` -> `o: orders`)
        dynamic_table_aliases = {}
        # Also track the full physical table scope injected into this exact query
        tables_in_scope = set()
        
        # Track every runtime prefix explicitly utilized by each physical table.
        # This prevents generating "users.name" if "users" was only ever aliased as "u", which violates dialect rules.
        table_runtime_prefixes: dict[str, set[str]] = {}
        
        for table_node in tree.find_all(exp.Table):
            t_name = table_node.name.lower()
            tables_in_scope.add(t_name)
            
            if t_name not in table_runtime_prefixes:
                table_runtime_prefixes[t_name] = set()
                
            if table_node.alias:
                dynamic_table_aliases[table_node.alias.lower()] = t_name
                table_runtime_prefixes[t_name].add(table_node.alias.lower())

        # 3b. Mutate abstract components to physical execution targets
        for node in tree.walk():
            # Extract specific node instance
            node_inst = node[0] if isinstance(node, tuple) else node

            # A. Target Tables directly
            if isinstance(node_inst, exp.Table):
                t_name = node_inst.name.lower()
                if t_name in alias_to_physical_table:
                    node_inst.set("this", exp.Identifier(this=alias_to_physical_table[t_name]))
                else:
                    raise TranslationError(f"Table '{t_name}' does not exist in schema context.")

            # B. Target Columns securely by resolving dynamic prefixes against the mapping
            elif isinstance(node_inst, exp.Column):
                c_name = node_inst.name.lower()
                t_prefix = node_inst.table.lower() if node_inst.table else ""
                
                if t_prefix:
                    # Is this prefix formally declared in the query scope?
                    if t_prefix in dynamic_table_aliases or t_prefix in tables_in_scope:
                        resolved_table = dynamic_table_aliases.get(t_prefix, t_prefix)
                        
                        if resolved_table not in alias_to_physical_table:
                            raise TranslationError(f"Table '{resolved_table}' does not exist in schema context.")
                            
                        full_alias = f"{resolved_table}.{c_name}"
                        
                        if full_alias in alias_to_physical_col:
                            node_inst.set("this", exp.Identifier(this=alias_to_physical_col[full_alias]))
                            column_datatypes[id(node_inst)] = alias_to_datatype.get(full_alias, "")

                            # Dialects strictly fail if querying `original_table.column` when an alias `t` is defined in FROM.
                            assigned_aliases = table_runtime_prefixes.get(resolved_table, set())
                            if t_prefix in assigned_aliases:
                                runtime_prefix = t_prefix
                            elif not assigned_aliases:
                                runtime_prefix = alias_to_physical_table[resolved_table]
                            elif len(assigned_aliases) == 1:
                                runtime_prefix = next(iter(assigned_aliases))
                            else:
                                raise TranslationError(f"Ambiguous target prefix '{t_prefix}' for column '{c_name}' from self-joined table '{resolved_table}' with aliases {list(assigned_aliases)}.")

                            node_inst.set("table", exp.Identifier(this=runtime_prefix))
                            self._check_column_safety(c_name, resolved_table, alias_to_safety[full_alias], node_inst)
                        else:
                            raise TranslationError(f"Column '{full_alias}' does not exist in schema context.")
                            
                    else:
                        # Prefix is NOT declared in scope. We must apply strict normalization invariants.
                        owning_tables = column_ownership.get(c_name, set())
                        scoped_owning_tables = owning_tables.intersection(tables_in_scope)
                        
                        if not scoped_owning_tables:
                            raise TranslationError(f"Orphaned prefix '{t_prefix}' refers to column '{c_name}', which does not belong to any table formally declared in scope {list(tables_in_scope)}.")
                            
                        if len(scoped_owning_tables) > 1:
                            raise TranslationError(f"Ambiguous orphaned prefix '{t_prefix}' for column '{c_name}'. Exists in multiple scoped tables: {list(scoped_owning_tables)}.")
                            
                        # Exactly ONE table in scope owns this column! Provable deterministic repair.
                        unique_owning_table = scoped_owning_tables.pop()
                        real_physical_table = alias_to_physical_table[unique_owning_table]
                        
                        # Infer legal runtime prefix for repaired table
                        assigned_aliases = table_runtime_prefixes.get(unique_owning_table, set())
                        if not assigned_aliases:
                            runtime_prefix = real_physical_table
                        elif len(assigned_aliases) == 1:
                            runtime_prefix = next(iter(assigned_aliases))
                        else:
                            raise TranslationError(f"Cannot auto-heal orphaned prefix '{t_prefix}' for column '{c_name}' due to ambiguous multiple aliases for table '{unique_owning_table}' ({list(assigned_aliases)}).")
                        
                        repairs.append(TranslationRepair(
                            type="orphaned_alias",
                            original=f"{t_prefix}.{c_name}",
                            resolved_to=f"{real_physical_table}.{alias_to_physical_col[f'{unique_owning_table}.{c_name}']}",
                            reason=f"Unique column ownership logically inferred over mapped structure."
                        ))

                        owning_full_alias = f"{unique_owning_table}.{c_name}"
                        node_inst.set("this", exp.Identifier(this=alias_to_physical_col[owning_full_alias]))
                        node_inst.set("table", exp.Identifier(this=runtime_prefix))
                        column_datatypes[id(node_inst)] = alias_to_datatype.get(owning_full_alias, "")
                        self._check_column_safety(c_name, unique_owning_table, alias_to_safety[owning_full_alias], node_inst)
                        
                else:
                    # No explicitly requested prefix. Check standard ambiguity.
                    owning_tables = column_ownership.get(c_name, set())
                    scoped_owning_tables = owning_tables.intersection(tables_in_scope)
                    
                    if len(scoped_owning_tables) > 1:
                         raise TranslationError(f"Ambiguous naked column '{c_name}'. Belongs to multiple scoped tables: {list(scoped_owning_tables)}. Explicit aliasing required.")
                    elif len(scoped_owning_tables) == 1:
                         unique_owning_table = scoped_owning_tables.pop()
                         no_prefix_full_alias = f"{unique_owning_table}.{c_name}"
                         node_inst.set("this", exp.Identifier(this=alias_to_physical_col[no_prefix_full_alias]))
                         column_datatypes[id(node_inst)] = alias_to_datatype.get(no_prefix_full_alias, "")
                         self._check_column_safety(c_name, unique_owning_table, alias_to_safety[no_prefix_full_alias], node_inst)
                    else:
                        if c_name in alias_to_physical_col:
                            node_inst.set("this", exp.Identifier(this=alias_to_physical_col[c_name]))

            # C. Collect Literals
            elif isinstance(node_inst, exp.Literal):
                literals_to_replace.append(node_inst)
                
        # D. Validate EXTRACT AST rules post-resolution
        temporal_types = {"timestamp", "date", "datetime", "time", "interval"}
        for extract_node in tree.find_all(exp.Extract):
            source = extract_node.expression
            if not isinstance(source, exp.Column):
                 raise UnsafeExpressionError(f"EXTRACT numeric target must be natively bound to a column, found '{type(source).__name__}'.")
            
            if any(extract_node.find_all((exp.Subquery, exp.Select, exp.Window))):
                 raise UnsafeExpressionError("Nested subqueries or window constructs are strictly blocked inside EXTRACT.")
                 
            # Extract target datatype validation
            dtype = column_datatypes.get(id(source), "")
            if not any(t in dtype for t in temporal_types):
                 raise UnsafeExpressionError(f"EXTRACT operations are only permitted on temporal columns. Resolved column '{source.name}' is of type '{dtype}'.")

        for interval_node in tree.find_all(exp.Interval):
            if any(interval_node.find_all((exp.Subquery, exp.Select, exp.Window))):
                 raise UnsafeExpressionError("Nested subqueries or window constructs are strictly blocked inside INTERVAL.")
                 
        # E. Parameterize Literals Safely
        for node_inst in literals_to_replace:
            # We replace string/numeric literals with query parameters
            param_name = f"p{param_counter}"

            # Keep original value for the param dictionary
            if node_inst.is_string:
                parameters[param_name] = node_inst.this
            elif node_inst.is_number:
                # simplistic number parsing
                parameters[param_name] = (
                    float(node_inst.this)
                    if "." in node_inst.this
                    else int(node_inst.this)
                )
            else:
                parameters[param_name] = node_inst.this

            # Replace the literal node in the AST with a positional
            # or named parameter depending on dialect
            # For postgres, standard asyncpg / sqlalchemy parameters
            # are either $1 or :p1
            # sqlglot Parameter node
            param_node = exp.Parameter(this=exp.var(param_name))
            node_inst.replace(param_node)

            param_counter += 1

        # Conditionally apply limits if no aggregation and no group by
        is_aggregated = any(tree.find_all(exp.AggFunc))
        has_groupby = tree.args.get("group") is not None
        row_limit_applied = False

        if not is_aggregated and not has_groupby:
            existing_limit = tree.args.get("limit")
            if not existing_limit:
                tree.set("limit", exp.Limit(expression=exp.Literal.number(row_limit)))
                row_limit_applied = True

        final_sql = tree.sql(dialect="postgres")

        # sqlglot formatting quirk: parameters like :p1 might render
        # depending on dialect ($p1 in postgres). 
        # SQLAlchemy requires :p1 for text() binding universally.
        import re
        final_sql = re.sub(r'\$(p\d+)\b', r':\1', final_sql)

        return ExecutableQuery(
            sql=final_sql,
            parameters=parameters,
            registry_version=schema.version,
            safety_engine_version=safety_version,
            abstract_query_hash=abstract_query_hash,
            row_limit_applied=row_limit_applied,
            translation_repairs=repairs
        )

    def _validate_join_graph(
        self,
        tree: exp.Expression,
        relationships: list[AbstractRelationshipDef],
    ) -> None:
        """
        Validates that every explicit JOIN ON condition references a declared relationship edge.

        Two-layer defence:
        1. The ON condition column pair must match a declared edge in relationships (structural).
        2. join_participation_allowed on each column is enforced separately by _check_column_safety.

        Runs on the abstract AST (before physical substitution) so names match schema aliases.
        Raises TranslationError for undeclared JOIN edges (hallucinated JOINs).
        """
        # Build SQL-alias → abstract-table-alias map from abstract table nodes.
        sql_alias_to_table: dict[str, str] = {}
        for table_node in tree.find_all(exp.Table):
            t_name = table_node.name.lower()
            sql_alias_to_table[t_name] = t_name
            if table_node.alias:
                sql_alias_to_table[table_node.alias.lower()] = t_name

        # Build a frozenset edge index for O(1) pair lookup.
        # An edge is directional in the data model but bidirectional for JOIN ON semantics.
        declared_edges: set[frozenset] = set()
        for rel in relationships:
            if rel.source_column and rel.target_column:
                declared_edges.add(frozenset({
                    f"{rel.source_table}.{rel.source_column}",
                    f"{rel.target_table}.{rel.target_column}",
                }))

        def _resolve(col: exp.Column) -> str | None:
            c_name = col.name.lower()
            t_prefix = col.table.lower() if col.table else ""
            if not t_prefix:
                return None
            abstract_table = sql_alias_to_table.get(t_prefix, t_prefix)
            return f"{abstract_table}.{c_name}"

        for join_node in tree.find_all(exp.Join):
            on_clause = join_node.args.get("on")
            if on_clause is None:
                # No ON condition — implicit/cross join; handled by SafetyEngine.
                continue

            for eq_node in on_clause.find_all(exp.EQ):
                left, right = eq_node.left, eq_node.right
                if not isinstance(left, exp.Column) or not isinstance(right, exp.Column):
                    continue

                left_ref = _resolve(left)
                right_ref = _resolve(right)

                if left_ref is None or right_ref is None:
                    # Cannot resolve without an explicit table prefix — skip (TranslationError
                    # for ambiguous naked columns will surface later in the column walk).
                    continue

                pair = frozenset({left_ref, right_ref})
                if pair not in declared_edges:
                    raise TranslationError(
                        f"JOIN condition '{left_ref} = {right_ref}' does not correspond to any "
                        f"declared relationship in the schema. Hallucinated JOIN blocked."
                    )

    def _get_column_sql_context(self, col_node: exp.Column) -> set[str]:
        """
        Returns the set of SQL contexts a column participates in.

        Two independent checks:
        - Aggregation: is the column wrapped inside any AggFunc? (does not stop clause detection)
        - Clause: what is the nearest bounding SQL clause?

        Uses sqlglot's native find_ancestor so intermediate nodes
        (exp.Alias, exp.Cast, exp.Paren, operators, etc.) are traversed transparently.
        """
        contexts: set[str] = set()

        if col_node.find_ancestor(exp.AggFunc):
            contexts.add("aggregation")

        clause = col_node.find_ancestor(
            exp.Where, exp.Group, exp.Having, exp.Join, exp.Order, exp.Select
        )
        if isinstance(clause, exp.Where):    contexts.add("where")
        elif isinstance(clause, exp.Group):  contexts.add("group_by")
        elif isinstance(clause, exp.Having): contexts.add("having")
        elif isinstance(clause, exp.Join):   contexts.add("join")
        elif isinstance(clause, exp.Order):  contexts.add("order_by")
        elif isinstance(clause, exp.Select): contexts.add("select")

        return contexts

    def _check_column_safety(
        self,
        col_alias: str,
        table_alias: str,
        safety: "SafetyClassification",
        col_node: exp.Column,
    ) -> None:
        """
        Raises SafetyPolicyViolationError if the column is used in a SQL clause
        its SafetyClassification does not permit.

        Called after physical name resolution so the error message uses the
        abstract alias (human-readable) not the physical target name.
        """
        from app.steward.models import SafetyClassification  # local import avoids circular dep

        contexts = self._get_column_sql_context(col_node)
        label = f"'{table_alias}.{col_alias}'"

        if "aggregation" in contexts:
            # Column is inside an aggregation function — aggregation_allowed is the sole
            # gating check. The surrounding clause (WHERE, HAVING, SELECT) is irrelevant
            # because the column is not directly exposed there; only the aggregate result is.
            if not safety.aggregation_allowed:
                raise SafetyPolicyViolationError(
                    message=f"Column {label} is not permitted inside aggregation functions."
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
                # Bare column in HAVING (no aggregation wrapping) — semantically a filter predicate
                raise SafetyPolicyViolationError(
                    message=f"Column {label} is not permitted in HAVING conditions."
                )

    def _extract_conjunctions(self, node: exp.Expression) -> list[exp.Expression]:
        """Flattens an AND boolean tree into a list of its leaf expressions."""
        if isinstance(node, exp.And):
            return self._extract_conjunctions(node.left) + self._extract_conjunctions(node.right)
        return [node]

    def _repair_where_aggregations(self, tree: exp.Expression, repairs: list["TranslationRepair"]) -> None:
        """
        Safely extracts pure aggregate conditions from the WHERE clause and relocates them to the HAVING clause.
        Maintains deterministic safety by ignoring Windows, Subqueries, and ensuring AND split semantics.
        """
        where_node = tree.args.get("where")
        if not where_node:
            return
            
        condition = where_node.this
        
        # Security Policy 1: If ANY exp.Or exists anywhere in the WHERE clause, abandon rewrite entirely.
        # Changing boolean grouping order by pulling an ORed aggregate into HAVING logically corrupts the query.
        if any(condition.find_all(exp.Or)):
            return
            
        conjunctions = self._extract_conjunctions(condition)
        
        where_conditions = []
        having_conditions = []
        
        for c in conjunctions:
            has_agg = any(c.find_all(exp.AggFunc))
            has_window = any(c.find_all(exp.Window))
            has_subquery = any(c.find_all((exp.Select, exp.Subquery)))
            
            # Rule 1: Do not move complex constructs like windows or subqueries. Let native PG catch them.
            if has_window or has_subquery:
                where_conditions.append(c)
                continue
                
            # Rule 2: Move valid unmixed aggregates to HAVING
            if has_agg:
                # Security Policy 2: Mixed node check.
                # Must ensure there are NO naked columns that exist *outside* an AggFunc block in this conjunction.
                # E.g., `SUM(a) > b` -> `a` is inside SUM, `b` is naked. This is illegal grouping context.
                has_naked_column = False
                for col in c.find_all(exp.Column):
                    # Check if any parent up the AST tree is an AggFunc
                    current = col.parent
                    is_inside_agg = False
                    while current:
                        if isinstance(current, exp.AggFunc):
                            is_inside_agg = True
                            break
                        current = current.parent
                        
                    if not is_inside_agg:
                        has_naked_column = True
                        break
                        
                if has_naked_column:
                    # Mixed scalar+aggregate leaf. Do not move. Postgres will throw standard error natively.
                    where_conditions.append(c)
                else:
                    having_conditions.append(c)
            else:
                where_conditions.append(c)
                
        if not having_conditions:
            return
            
        # Rebuild WHERE clause
        if where_conditions:
            tree.set("where", exp.Where(this=exp.and_(*where_conditions)))
        else:
            tree.set("where", None)
            
        # Rebuild HAVING clause and append any pre-existing expressions
        existing_having = tree.args.get("having")
        new_having_expr = exp.and_(*having_conditions)
        
        if existing_having:
            new_having_expr = exp.and_(existing_having.this, new_having_expr)
            
        tree.set("having", exp.Having(this=new_having_expr))
        
        repairs.append(TranslationRepair(
            type="where_aggregation_relocation",
            original=condition.sql(dialect="postgres"),
            resolved_to="Split into WHERE and HAVING",
            reason="Pure aggregate condition found in WHERE clause. Securely relocated to HAVING."
        ))
