from typing import Protocol

from app.compiler.models import (
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


class ValueVectorStore(Protocol):
    def match_value(self, value: str, *, min_confidence: float) -> ValueMatchResult:
        ...

class SchemaFilter(Protocol):
    def filter_schema(
        self, intent: UserIntent, schema: RegistrySchema
    ) -> FilteredSchema:
        ...

class PromptBuilder(Protocol):
    def build_prompt(
        self, intent: UserIntent, schema: FilteredSchema, hints: PromptHints
    ) -> PromptEnvelope:
        ...

class LLMGateway(Protocol):
    async def generate(self, prompt: PromptEnvelope) -> LLMResult:
        ...

class SQLTranslator(Protocol):
    def translate(
        self, ast: ValidatedAST, schema: RegistrySchema
    ) -> ExecutableQuery:
        ...

class SQLSafetyEngine(Protocol):
    def validate(self, ast: SQLAst) -> ValidatedAST:
        ...
