from typing import Protocol

from app.compiler import ExecutableQuery
from app.execution.models import ExecutionContext, QueryResult


class ExecutionLayer(Protocol):
    """Defines the boundary for the database execution layer."""
    async def execute(
        self, query: ExecutableQuery, *, context: ExecutionContext
    ) -> QueryResult:
        ...
