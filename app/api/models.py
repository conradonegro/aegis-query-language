from typing import Any
from pydantic import BaseModel, ConfigDict, Field


class QueryRequest(BaseModel):
    """
    Incoming REST payload from an external client asserting an intent.
    """
    model_config = ConfigDict(frozen=True)

    intent: str = Field(..., description="The Natural Language question or request from the user.")
    schema_hints: list[str] = Field(default_factory=list, description="Optional business logic hints for the LLM context.")
    explain: bool = Field(default=False, description="Debug flag to securely expose internal pipeline compilation context.")


class ExplainabilityContext(BaseModel):
    """
    Secure export of the internal proxy compilation states.
    Only surfaced if specifically requested via `explain=true` flag.
    """
    model_config = ConfigDict(frozen=True)

    rag: dict[str, Any] = Field(..., description="RAG Outcome, scored candidates, and reason.")
    schema_filter: dict[str, Any] = Field(..., description="Included and excluded conceptual schema aliases.")
    prompt: dict[str, Any] = Field(..., description="Redacted system prompt and explicit user envelope.")
    llm: dict[str, Any] = Field(..., description="Provider, model, tokens, and latency.")
    translation: dict[str, Any] = Field(..., description="Abstract query mapping traces, parameterized ASTs, and binding derivations.")


class QueryGenerateResponse(BaseModel):
    """
    Response schema for the `/generate` endpoint exposing only the compiled intent.
    """
    model_config = ConfigDict(frozen=True)

    query_id: str = Field(..., description="Unique provenance ID of the Abstract AST compilation.")
    sql: str = Field(..., description="The fully parameterized SQL string.")
    parameters: dict[str, str | int | float | bool] = Field(..., description="Bind parameters for the query.")
    latency_ms: float = Field(..., description="Compilation pipeline latency including LLM overhead.")
    explainability: ExplainabilityContext | None = Field(default=None, description="Diagnostic pipeline traces.")


class QueryExecuteResponse(BaseModel):
    """
    Response schema for the `/execute` endpoint returning physical results.
    """
    model_config = ConfigDict(frozen=True)

    query_id: str = Field(..., description="Unique provenance ID of the Abstract AST execution.")
    results: list[dict[str, str | int | float | bool | None]]
    row_count: int
    execution_latency_ms: float = Field(..., description="Physical database execution latency.")
    explainability: ExplainabilityContext | None = Field(default=None, description="Diagnostic pipeline traces.")


class ErrorResponse(BaseModel):
    """
    Standard stable HTTP Error payload interface.
    """
    code: int = Field(..., description="HTTP Status code (400, 403, 500)")
    message: str = Field(..., description="Human-readable domain boundary exception description.")
    request_id: str | None = Field(default=None, description="Request trace ID if applicable.")
    explainability: dict[str, Any] | None = Field(default=None, description="Partial pipeline traces if execution halted.")
