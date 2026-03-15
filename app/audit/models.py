from pydantic import BaseModel


class QueryAuditEvent(BaseModel):
    """Cross-cutting representation of query performance and outcomes."""
    query_id: str
    tenant_id: str | None = None
    user_id: str | None = None
    natural_language_query: str
    abstract_query: str
    physical_query: str
    registry_version: str
    safety_engine_version: str
    abstract_query_hash: str
    latency_ms: float
    prompt_tokens: int
    completion_tokens: int
    status: str
    error_message: str | None = None
    row_limit_applied: bool = False

    credential_id: str | None = None

    rag_outcome: str | None = None
    rag_matched_value: str | None = None
    rag_abstract_column: str | None = None
    rag_similarity_score: float | None = None
