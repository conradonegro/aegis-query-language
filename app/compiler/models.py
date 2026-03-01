from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel

from app.steward import AbstractIdentifierDef


class UserIntent(BaseModel):
    natural_language_query: str

class ValueMatchResult(BaseModel):
    status: Literal["success", "ambiguous", "no_match"]
    matches: list[str] = []
class FilteredSchema(BaseModel):
    version: str
    active_identifiers: list[AbstractIdentifierDef]
    omitted_identifiers: dict[str, str]

class PromptHints(BaseModel):
    column_hints: list[str]
    rag_provenance: dict[str, Any] | None = None

class PromptEnvelope(BaseModel):
    system_instruction: str
    schema_context: str
    user_prompt: str
    hints: str

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
    row_limit_applied: bool = False
    query_id: str | None = None
    compilation_latency_ms: float | None = None
