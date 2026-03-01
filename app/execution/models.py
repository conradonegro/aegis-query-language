from typing import Any

from pydantic import BaseModel


class ExecutionContext(BaseModel):
    """Contextual logic for query execution timeouts and roles."""
    tenant_id: str
    user_id: str | None = None
    statement_timeout_ms: int = 5000

class QueryResult(BaseModel):
    """Uniform output from the execution layer."""
    columns: list[str]
    rows: list[dict[str, Any]]
    metadata: dict[str, Any]
