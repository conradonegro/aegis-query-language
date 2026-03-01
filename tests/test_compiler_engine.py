import pytest

from app.compiler.engine import CompilerEngine, RAGUncertaintyError
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
from app.rag.models import CategoricalValue
from app.rag.store import InMemoryVectorStore
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

@pytest.fixture
def rag_compiler_engine(compiler_engine: CompilerEngine) -> CompilerEngine:
    store = InMemoryVectorStore()
    store.index_value(CategoricalValue(value="Alice", abstract_column="name", tenant_id="default_tenant"))
    compiler_engine.set_vector_store(store)
    return compiler_engine

@pytest.mark.asyncio
async def test_compiler_engine_rag_success(rag_compiler_engine: CompilerEngine, mock_registry: RegistrySchema) -> None:
    intent = UserIntent(natural_language_query="Show me Alice")
    hints = PromptHints(column_hints=[])
    
    executable = await rag_compiler_engine.compile(intent, mock_registry, hints)
    
    assert executable is not None
    assert len(hints.column_hints) == 1
    assert "Alice" in hints.column_hints[0]
    assert hints.rag_provenance is not None
    assert hints.rag_provenance["rag_outcome"] == "SINGLE_HIGH_CONFIDENCE_MATCH"
    assert hints.rag_provenance["rag_matched_value"] == "Alice"

@pytest.mark.asyncio
async def test_compiler_engine_rag_no_match(rag_compiler_engine: CompilerEngine, mock_registry: RegistrySchema) -> None:
    intent = UserIntent(natural_language_query="Show me Bob")
    hints = PromptHints(column_hints=[])
    
    with pytest.raises(RAGUncertaintyError) as exc:
        await rag_compiler_engine.compile(intent, mock_registry, hints)
        
    assert "strict policy outcome: NO_MATCH" in str(exc.value)
    assert "Tenant vector store is empty" in str(exc.value) or "No candidates met the threshold (0.85)" in str(exc.value)

@pytest.mark.asyncio
async def test_compiler_engine_rag_ambiguous_match(rag_compiler_engine: CompilerEngine, mock_registry: RegistrySchema) -> None:
    # Add an ambiguous item
    rag_compiler_engine.vector_store.index_value(CategoricalValue(value="Alice Cooper", abstract_column="name", tenant_id="default_tenant"))
    
    intent = UserIntent(natural_language_query="Show me Alice or Alice Cooper")
    hints = PromptHints(column_hints=[])
    
    with pytest.raises(RAGUncertaintyError) as exc:
        await rag_compiler_engine.compile(intent, mock_registry, hints)
        
    assert "strict policy outcome: AMBIGUOUS_MATCH" in str(exc.value)
    assert "Ambiguous: 2 competing matches breached the threshold." in str(exc.value)
