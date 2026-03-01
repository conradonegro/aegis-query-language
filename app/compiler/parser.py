import sqlglot

from app.compiler.models import AbstractQuery, SQLAst


class SQLParser:
    """Parses abstract SQL from the LLM into a validated sqlglot expression."""

    def parse(self, query: AbstractQuery) -> SQLAst:
        """
        Parses the raw SQL string into an AST.
        Raises a sqlglot.errors.ParseError on invalid syntax.
        """
        # We enforce postgres dialect for standard syntax
        tree = sqlglot.parse_one(query.sql, read="postgres")
        return SQLAst(tree=tree)
