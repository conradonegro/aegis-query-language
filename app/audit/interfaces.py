from typing import Protocol

from app.audit.models import QueryAuditEvent


class QueryAuditSink(Protocol):
    """Sink interface for cross-cutting query audit events."""
    async def record(self, event: QueryAuditEvent) -> None:
        ...
