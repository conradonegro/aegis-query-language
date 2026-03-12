from typing import Protocol

from app.compiler.models import (
    AbstractQuery,
    ExecutableQuery,
    FilteredSchema,
    LLMResult,
    PromptEnvelope,
    PromptHints,
    SQLAst,
    UserIntent,
    ValidatedAST,
    ValueMatchResult,
    RAGIncludedColumns,
    ChatHistoryItem,
)
from app.steward import AbstractRelationshipDef, RegistrySchema


class ValueVectorStoreProtocol(Protocol):
    def match_value(self, value: str, *, min_confidence: float) -> ValueMatchResult:
        ...

class SchemaFilterProtocol(Protocol):
    def is_follow_up(
        self, intent: UserIntent, last_schema: FilteredSchema | None, full_schema: RegistrySchema | None = None
    ) -> bool:
        """Returns True if the intent is a follow-up on the previous query context."""
        ...

    def filter_schema(
        self, intent: UserIntent, schema: RegistrySchema, included_columns: RAGIncludedColumns | None = None
    ) -> FilteredSchema:
        """
        Filters the schema based on intent mapping.

        INVARIANT: `included_columns` MUST ONLY be populated by trusted internal
        RAG match resolutions. User hints or external API payloads must NEVER be
        allowed to arbitrarily bypass token overlap filtering rules.
        """
        ...

class PromptBuilderProtocol(Protocol):
    def build_prompt(
        self, intent: UserIntent, schema: FilteredSchema, hints: PromptHints, chat_history: list[ChatHistoryItem] | None = None
    ) -> PromptEnvelope:
        ...

class LLMGatewayProtocol(Protocol):
    async def generate(self, prompt: PromptEnvelope) -> LLMResult:
        ...

class SQLParserProtocol(Protocol):
    def parse(self, query: AbstractQuery) -> SQLAst:
        ...

class TranslatorProtocol(Protocol):
    def translate(
        self,
        ast: ValidatedAST,
        schema: RegistrySchema,
        abstract_query_hash: str = "default_hash",
        safety_version: str = "v1.0.0",
        row_limit: int = 1000,
        relationships: list[AbstractRelationshipDef] | None = None,
    ) -> ExecutableQuery:
        ...

class SafetyEngineProtocol(Protocol):
    def validate(self, ast: SQLAst) -> ValidatedAST:
        ...
