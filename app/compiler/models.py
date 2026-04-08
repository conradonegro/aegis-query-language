from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.api.models import TranslationRepair
from app.steward import AbstractRelationshipDef, AbstractTableDef


class UserIntent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    natural_language_query: str
    source_database: str | None = None

class ChatHistoryItem(BaseModel):
    role: Literal["user", "assistant", "system"]
    content: str

class ValueMatchResult(BaseModel):
    status: Literal["success", "ambiguous", "no_match"]
    matches: list[str] = []

class RAGIncludedColumns(BaseModel):
    """
    STRICT INVARIANT PAYLOAD:
    This type explicitly wraps columns that were successfully extracted by the RAG
    Vector Engine. It enforces the compiler pipeline invariant that the SchemaFilter
    will NEVER bypass rules for arbitrary user strings, only for fully validated
    RAG outcomes.
    """
    columns: list[str] = []

class FilteredSchema(BaseModel):
    version: str
    tables: list[AbstractTableDef]
    relationships: list[AbstractRelationshipDef]
    omitted_columns: dict[str, str]
    source_database_used: str | None = None
    source_database_mode: str = "none"  # "explicit" | "auto_detected" | "none"
    db_detection_scores: dict[str, int] = {}

class SessionQueryContext(BaseModel):
    last_filtered_schema: FilteredSchema
    last_successful_sql: str
    timestamp: float
    registry_version: str

class PromptHints(BaseModel):
    column_hints: list[str]
    rag_provenance: dict[str, Any] | None = None

class PromptEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    system_instruction: str
    user_prompt: str
    chat_history: list[ChatHistoryItem] = Field(default_factory=list)

class LLMResult(BaseModel):
    raw_text: str
    model_id: str
    latency_ms: float
    prompt_tokens: int
    completion_tokens: int

class LLMQueryResponse(BaseModel):
    """Validated structure of the JSON object returned by any LLM gateway."""
    sql: str | None = None
    refused: bool = False
    reason: str | None = None

    @model_validator(mode="after")
    def validate_refusal_contract(self) -> "LLMQueryResponse":
        if self.refused and self.sql is not None:
            raise ValueError(
                "Ambiguous LLM response: 'refused' is true but 'sql' is present. "
                "A refusal must not contain SQL."
            )
        if not self.refused and not self.sql:
            raise ValueError(
                "Invalid LLM response: 'refused' is false but 'sql' is absent or empty."
            )
        return self

class AbstractQuery(BaseModel):
    sql: str

@dataclass
class SQLAst:
    """Wrapper around SQL AST for abstract query (e.g. sqlglot expression)."""
    tree: Any

@dataclass
class ValidatedAST:
    """Successfully validated SQL AST."""
    tree: Any

class ExecutableQuery(BaseModel):
    """Final, parameterized query ready for execution."""
    model_config = ConfigDict(extra="forbid")
    sql: str
    parameters: dict[str, Any]
    registry_version: str
    safety_engine_version: str
    abstract_query_hash: str
    abstract_sql: str | None = None
    row_limit_applied: bool = False
    query_id: str | None = None
    compilation_latency_ms: float | None = None
    explainability: dict[str, Any] | None = None
    translation_repairs: list[TranslationRepair] = Field(default_factory=list)
    source_database_used: str | None = None
    # LLM usage metrics — always populated regardless of the explain flag.
    llm_prompt_tokens: int = 0
    llm_completion_tokens: int = 0
