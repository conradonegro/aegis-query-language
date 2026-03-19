from pydantic import BaseModel


class QueryAuditEvent(BaseModel):
    """
    Cross-cutting representation of query performance and outcomes.

    SQL and version fields are optional so that a partial event can be emitted
    for failure paths where compilation did not complete (safety rejections, LLM
    refusals, session errors, etc.).  A missing value means "not reached", not
    "unknown".
    """
    query_id: str
    tenant_id: str | None = None
    user_id: str | None = None
    credential_id: str | None = None
    natural_language_query: str
    operation: str = "execute"          # "generate" | "execute"
    status: str                          # "SUCCESS" | "FAILURE"

    # Populated only when compilation succeeded
    abstract_query: str | None = None
    physical_query: str | None = None
    registry_version: str | None = None
    safety_engine_version: str | None = None
    abstract_query_hash: str | None = None
    latency_ms: float = 0.0
    row_limit_applied: bool = False

    # LLM metadata — available whenever the LLM call completed
    prompt_tokens: int = 0
    completion_tokens: int = 0
    provider_id: str | None = None

    # Failure details
    error_type: str | None = None       # exception class name, e.g. "TranslationError"
    error_message: str | None = None

    # RAG provenance
    rag_outcome: str | None = None
    rag_matched_value: str | None = None
    rag_abstract_column: str | None = None
    rag_similarity_score: float | None = None
