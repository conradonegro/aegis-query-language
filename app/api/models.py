from pydantic import BaseModel, ConfigDict, Field


class QueryRequest(BaseModel):
    """
    Incoming REST payload from an external client asserting an intent.
    """
    model_config = ConfigDict(frozen=True)

    intent: str = Field(..., description="The Natural Language question or request from the user.")
    schema_hints: list[str] = Field(default_factory=list, description="Optional business logic hints for the LLM context.")


class QueryGenerateResponse(BaseModel):
    """
    Response schema for the `/generate` endpoint exposing only the compiled intent.
    """
    model_config = ConfigDict(frozen=True)

    query_id: str = Field(..., description="Unique provenance ID of the Abstract AST compilation.")
    sql: str = Field(..., description="The fully parameterized SQL string.")
    parameters: dict[str, str | int | float | bool] = Field(..., description="Bind parameters for the query.")
    latency_ms: float = Field(..., description="Compilation pipeline latency including LLM overhead.")


class QueryExecuteResponse(BaseModel):
    """
    Response schema for the `/execute` endpoint returning physical results.
    """
    model_config = ConfigDict(frozen=True)

    query_id: str = Field(..., description="Unique provenance ID of the Abstract AST execution.")
    results: list[dict[str, str | int | float | bool | None]]
    row_count: int
    execution_latency_ms: float = Field(..., description="Physical database execution latency.")


class ErrorResponse(BaseModel):
    """
    Standard stable HTTP Error payload interface.
    """
    code: int = Field(..., description="HTTP Status code (400, 403, 500)")
    message: str = Field(..., description="Human-readable domain boundary exception description.")
    request_id: str | None = Field(default=None, description="Request trace ID if applicable.")
