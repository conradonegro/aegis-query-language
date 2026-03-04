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
    
    session_id: str | None = Field(default=None, description="Optional ongoing Chat Session UUID to append to.")
    provider_id: str | None = Field(default=None, description="Optional explicit LLM Provider ID to use for this execution.")


class TranslationRepair(BaseModel):
    """
    Formal explainability trace of an AST Alias Normalization resolving orphaned contexts.
    """
    type: str = Field(..., description="The classification of the repair (e.g., 'orphaned_alias')")
    original: str = Field(..., description="The hallucinated or orphaned AST node")
    resolved_to: str = Field(..., description="The deterministically mapped physical structure")
    reason: str = Field(..., description="The logical invariant satisfying the mutation")

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
    translation_repairs: list[TranslationRepair] = Field(default_factory=list, description="Explicit traces of AST token normalizations against strictly isolated structures.")


class QueryGenerateResponse(BaseModel):
    """
    Response schema for the `/generate` endpoint exposing only the compiled intent.
    """
    model_config = ConfigDict(frozen=True)

    query_id: str = Field(..., description="Unique provenance ID of the Abstract AST compilation.")
    session_id: str | None = Field(default=None, description="The Chat Session UUID holding this interaction.")
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
    session_id: str | None = Field(default=None, description="The Chat Session UUID holding this interaction.")
    sql: str = Field(..., description="The fully parameterized SQL string.")
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


class ProtocolMetadataVersion(BaseModel):
    version_id: str
    status: str
    created_at: str

class MetadataCompileResponse(BaseModel):
    artifact_id: str
    version_id: str
    artifact_hash: str
    compiled_at: str

# Steward UI Schemas
class ProtocolColumn(BaseModel):
    column_id: str
    real_name: str
    alias: str
    description: str | None
    data_type: str
    is_primary_key: bool
    allowed_in_select: bool
    allowed_in_filter: bool
    allowed_in_join: bool
    safety_classification: dict[str, Any] | None

class ProtocolTable(BaseModel):
    table_id: str
    real_name: str
    alias: str
    description: str | None
    active: bool
    columns: list[ProtocolColumn]

class ProtocolRelationship(BaseModel):
    relationship_id: str
    source_table_id: str
    source_column_id: str
    target_table_id: str
    target_column_id: str
    relationship_type: str
    cardinality: str

class ProtocolSchemaResponse(BaseModel):
    version_id: str
    tables: list[ProtocolTable]
    relationships: list[ProtocolRelationship]

class TableUpdateRequest(BaseModel):
    alias: str | None = None
    description: str | None = None
    active: bool | None = None

class ColumnUpdateRequest(BaseModel):
    alias: str | None = None
    description: str | None = None
    allowed_in_select: bool | None = None
    allowed_in_filter: bool | None = None
    allowed_in_join: bool | None = None

class VersionCreateRequest(BaseModel):
    baseline_version_id: str | None = None
