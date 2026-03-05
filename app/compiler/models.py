from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel, Field

from app.api.models import TranslationRepair
from app.steward import AbstractColumnDef, AbstractRelationshipDef, AbstractTableDef


class UserIntent(BaseModel):
    natural_language_query: str

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
    will NEVER bypass rules for arbitrary user strings, only for fully validated RAG outcomes.
    """
    columns: list[str] = []
    
class FilteredSchema(BaseModel):
    version: str
    tables: list[AbstractTableDef]
    relationships: list[AbstractRelationshipDef]
    omitted_columns: dict[str, str]

class SessionQueryContext(BaseModel):
    last_filtered_schema: FilteredSchema
    last_successful_sql: str
    timestamp: float

class PromptHints(BaseModel):
    column_hints: list[str]
    rag_provenance: dict[str, Any] | None = None

class PromptEnvelope(BaseModel):
    system_instruction: str
    schema_context: str
    user_prompt: str
    hints: str
    chat_history: list[ChatHistoryItem] = Field(default_factory=list)

class LLMResult(BaseModel):
    raw_text: str
    model_id: str
    latency_ms: float
    prompt_tokens: int
    completion_tokens: int

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
