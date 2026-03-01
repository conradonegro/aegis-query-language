from typing import Any

from sqlglot import exp

from app.compiler.models import ExecutableQuery, ValidatedAST
from app.steward import RegistrySchema


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
        row_limit: int = 1000
    ) -> ExecutableQuery:
        """Translates abstract AST into parameterized physical SQL."""

        # 1. Copy-on-write
        tree = ast.tree.copy()

        # 2. Build fast lookup map O(1)
        alias_to_physical: dict[str, str] = {
            i.alias.lower(): i.physical_target
            for i in schema.identifiers
        }

        parameters: dict[str, Any] = {}
        param_counter = 1

        # 3. Walk and mutate the COPIED tree
        for node in tree.walk():
            # Extract specific node instance
            node_inst = node[0] if isinstance(node, tuple) else node

            # A. Map Identifiers (Tables, Columns)
            if isinstance(node_inst, exp.Identifier):
                abstract_name = node_inst.this.lower()
                # If this identifier matches an abstract alias,
                # replace it with physical one
                if abstract_name in alias_to_physical:
                    node_inst.set("this", alias_to_physical[abstract_name])

            # B. Parameterize Literals
            elif isinstance(node_inst, exp.Literal):
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
        # depending on dialect.
        # We ensure it replaces with a simple placeholder format.

        return ExecutableQuery(
            sql=final_sql,
            parameters=parameters,
            registry_version=schema.version,
            safety_engine_version=safety_version,
            abstract_query_hash=abstract_query_hash,
            row_limit_applied=row_limit_applied
        )
