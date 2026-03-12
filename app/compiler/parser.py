import sqlglot

from app.compiler.models import AbstractQuery, SQLAst


class SQLParser:
    """Parses abstract SQL from the LLM into a validated sqlglot expression."""

    def parse(self, query: AbstractQuery) -> SQLAst:
        """
        Parses the raw SQL string into an AST.
        Raises a sqlglot.errors.ParseError on invalid syntax.
        """
        # We enforce postgres dialect for standard syntax.
        # parse() returns a list — assert exactly one statement so multi-statement
        # payloads (e.g. "SELECT 1; DROP TABLE users") are rejected rather than
        # silently discarding everything after the first semicolon.
        trees = sqlglot.parse(query.sql, read="postgres")
        if len(trees) != 1:
            raise sqlglot.errors.ParseError(
                f"Expected exactly 1 SQL statement, got {len(trees)}."
            )
        return SQLAst(tree=trees[0])
