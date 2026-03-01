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
)
from app.steward import RegistrySchema


class ValueVectorStoreProtocol(Protocol):
    def match_value(self, value: str, *, min_confidence: float) -> ValueMatchResult:
        ...

class SchemaFilterProtocol(Protocol):
    def filter_schema(
        self, intent: UserIntent, schema: RegistrySchema
    ) -> FilteredSchema:
        ...

class PromptBuilderProtocol(Protocol):
    def build_prompt(
        self, intent: UserIntent, schema: FilteredSchema, hints: PromptHints
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
