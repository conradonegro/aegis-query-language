import pytest

from app.compiler.engine import CompilerEngine
from app.compiler.filter import DeterministicSchemaFilter
from app.compiler.gateway import MockLLMGateway
from app.compiler.interfaces import (
    LLMGatewayProtocol,
    PromptBuilderProtocol,
    SafetyEngineProtocol,
    SchemaFilterProtocol,
    SQLParserProtocol,
    TranslatorProtocol,
)
from app.compiler.models import PromptHints, UserIntent
from app.compiler.parser import SQLParser
from app.compiler.prompting import PromptBuilder
from app.compiler.safety import SafetyEngine
from app.compiler.translator import DeterministicTranslator
from app.steward.models import (
    AbstractIdentifierDef,
    RegistrySchema,
    SafetyClassification,
)


@pytest.fixture
def mock_registry() -> RegistrySchema:
    return RegistrySchema(
        version="1.0.0",
        identifiers=[
            AbstractIdentifierDef(
                alias="users",
                description="The users table",
                safety=SafetyClassification(allowed_in_select=True),
                physical_target="auth.users"
            )
        ]
    )

@pytest.fixture
def compiler_engine() -> CompilerEngine:
    gateway = MockLLMGateway(mock_response_sql="SELECT * FROM users")
    return CompilerEngine(
        schema_filter=DeterministicSchemaFilter(),
        prompt_builder=PromptBuilder(),
        llm_gateway=gateway,
        parser=SQLParser(),
        safety_engine=SafetyEngine(),
        translator=DeterministicTranslator()
    )


@pytest.mark.asyncio
async def test_compiler_engine_success(compiler_engine: CompilerEngine, mock_registry: RegistrySchema) -> None:
    intent = UserIntent(natural_language_query="Show me all users")
    hints = PromptHints(column_hints=[])
    
    # Run the full engine pipeline
    executable = await compiler_engine.compile(
        intent=intent,
        schema=mock_registry,
        hints=hints
    )
    
    # Assert successful orchestration
    assert executable.query_id is not None
    assert executable.compilation_latency_ms is not None
    assert "users" in executable.sql.lower()
    assert executable.registry_version == "1.0.0"
    assert executable.row_limit_applied is True
