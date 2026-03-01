import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from app.compiler import ExecutableQuery
from app.execution.models import ExecutionContext, QueryResult

logger = logging.getLogger(__name__)

class ExecutionEngine:
    """
    Safely executes an ExecutableQuery against a PostgreSQL database.
    - One engine per service.
    - Raw SQL execution over safe parameters.
    - Mandatory statement_timeout block.
    """

    def __init__(
        self, connection_string: str, pool_size: int = 20, max_overflow: int = 10
    ):
        # SQLAlchemy engine hygiene: explicitly bounded size, disable autocommit
        from typing import Any
        engine_kwargs: dict[str, Any] = {"echo": False}
        if "sqlite" not in connection_string:
            engine_kwargs["pool_size"] = pool_size
            engine_kwargs["max_overflow"] = max_overflow

        self.engine: AsyncEngine = create_async_engine(
            connection_string,
            **engine_kwargs
        )

    async def execute(
        self, query: ExecutableQuery, *, context: ExecutionContext
    ) -> QueryResult:
        """
        Executes raw parameterized SQL safely wrapped in a local statement_timeout.
        """
        # We prefer async with engine.begin() for transactional block execution
        async with self.engine.begin() as conn:
            # Enforce mandatory statement timeout specifically for this
            # transaction block (LOCAL). This protects against LLM-generated
            # long-running queries DOSing the database.
            # Local checks skip this only for mocked sqlite unit tests.
            if self.engine.name == "postgresql":
                timeout_sql = text("SET LOCAL statement_timeout = :timeout")
                await conn.execute(
                    timeout_sql,
                    {"timeout": context.statement_timeout_ms}
                )

            # Execute actual query using raw sql string and bound dictionary parameters
            query_sql = text(query.sql)
            result = await conn.execute(query_sql, query.parameters)

            # Extract standard dict response
            columns = list(result.keys())
            rows = [dict(row._mapping) for row in result.all()]

            metadata = {
                "row_limit_applied": query.row_limit_applied,
                "registry_version": query.registry_version,
                "safety_engine_version": query.safety_engine_version,
                "abstract_query_hash": query.abstract_query_hash,
            }

            return QueryResult(
                columns=columns,
                rows=rows,
                metadata=metadata
            )

    async def close(self) -> None:
        """Gracefully release pool."""
        await self.engine.dispose()
