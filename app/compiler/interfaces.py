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
from app.steward import RegistrySchema


class ValueVectorStoreProtocol(Protocol):
    def match_value(self, value: str, *, min_confidence: float) -> ValueMatchResult:
        ...

class SchemaFilterProtocol(Protocol):
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
        self, ast: ValidatedAST, schema: RegistrySchema
    ) -> ExecutableQuery:
        ...

class SafetyEngineProtocol(Protocol):
    def validate(self, ast: SQLAst) -> ValidatedAST:
        ...
