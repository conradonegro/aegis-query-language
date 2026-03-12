import json

import pytest

from app.compiler.engine import CompilerEngine, RAGUncertaintyError
from app.compiler.filter import DeterministicSchemaFilter
from app.compiler.gateway import MockLLMGateway
from app.compiler.ollama import LLMGenerationError
from app.compiler.translator import TranslationError
from app.compiler.interfaces import (
    LLMGatewayProtocol,
    PromptBuilderProtocol,
    SafetyEngineProtocol,
    SchemaFilterProtocol,
    SQLParserProtocol,
    TranslatorProtocol,
)
from app.compiler.models import PromptHints, UserIntent, LLMResult
from app.compiler.parser import SQLParser
from app.compiler.prompting import PromptBuilder
from app.compiler.safety import SafetyEngine
from app.compiler.translator import DeterministicTranslator
from app.rag.models import CategoricalValue
from app.rag.store import InMemoryVectorStore
from app.steward.models import (
    AbstractTableDef,
    AbstractColumnDef,
    RegistrySchema,
    SafetyClassification,
)


@pytest.fixture
def mock_registry() -> RegistrySchema:
    return RegistrySchema(
        version="1.0.0",
        tables=[
            AbstractTableDef(
                alias="users",
                description="The users table",
                physical_target="auth.users",
                columns=[
                    AbstractColumnDef(alias="id", description="ID", safety=SafetyClassification(allowed_in_select=True), physical_target="auth.users.id")
                ]
            )
        ],
        relationships=[]
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
    store.index_value(CategoricalValue(value="Alice", abstract_column="users.name", tenant_id="default_tenant"))
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
    
    executable = await rag_compiler_engine.compile(intent, mock_registry, hints)
        
    assert executable is not None
    assert hints.rag_provenance is not None
    assert hints.rag_provenance["rag_outcome"] == "NO_MATCH"
    assert "No candidates met the threshold" in hints.rag_provenance["rag_reason"]

@pytest.mark.asyncio
async def test_compiler_engine_rag_ambiguous_match(rag_compiler_engine: CompilerEngine, mock_registry: RegistrySchema) -> None:
    # Add an ambiguous item
    rag_compiler_engine.vector_store.index_value(CategoricalValue(value="Alice Cooper", abstract_column="users.name", tenant_id="default_tenant"))
    
    intent = UserIntent(natural_language_query="Show me Alice or Alice Cooper")
    hints = PromptHints(column_hints=[])
    
    executable = await rag_compiler_engine.compile(intent, mock_registry, hints)
        
    assert executable is not None
    assert hints.rag_provenance is not None
    assert hints.rag_provenance["rag_outcome"] == "AMBIGUOUS_MATCH"
    assert "Ambiguous: 2 competing matches breached the threshold." in hints.rag_provenance["rag_reason"]

@pytest.mark.asyncio
async def test_compiler_engine_follow_up_reuse(compiler_engine: CompilerEngine, mock_registry: RegistrySchema) -> None:
    session_id = "test_session_1"
    
    # 1. Fresh query
    intent1 = UserIntent(natural_language_query="Show me all users")
    hints1 = PromptHints(column_hints=[])
    exec1 = await compiler_engine.compile(intent=intent1, schema=mock_registry, hints=hints1, session_id=session_id, explain=True)
    
    # Verify state is stored
    assert session_id in compiler_engine.session_store._local

    stored_schema = compiler_engine.session_store._local[session_id].last_filtered_schema
    assert len(stored_schema.tables) > 0
    assert stored_schema.tables[0].alias == "users"

    # 2. Strict Follow-up (No structural tokens, short)
    intent2 = UserIntent(natural_language_query="and in 2016")
    hints2 = PromptHints(column_hints=[])
    exec2 = await compiler_engine.compile(intent=intent2, schema=mock_registry, hints=hints2, session_id=session_id, explain=True)
    
    assert exec2.explainability["schema_filter"]["reasons"] == ["Reused precisely from prior SessionQueryContext (Follow-up)"]
    assert exec2.explainability["schema_filter"]["included_aliases"] == exec1.explainability["schema_filter"]["included_aliases"]


@pytest.mark.asyncio
async def test_compiler_engine_follow_up_topic_shift(compiler_engine: CompilerEngine, mock_registry: RegistrySchema) -> None:
    session_id = "test_session_2"
    
    # 1. Fresh query
    intent1 = UserIntent(natural_language_query="Show me all users")
    hints1 = PromptHints(column_hints=[])
    await compiler_engine.compile(intent=intent1, schema=mock_registry, hints=hints1, session_id=session_id, explain=True)
    
    # 2. Topic Shift (contains structural token "users" which forces a fresh pull)
    intent2 = UserIntent(natural_language_query="what about authors")
    
    # Let's add authors to registry so we can test the shift
    mock_registry.tables.append(AbstractTableDef(
        alias="authors",
        description="The authors table",
        physical_target="auth.authors",
        columns=[]
    ))
    
    hints2 = PromptHints(column_hints=[])
    exec2 = await compiler_engine.compile(intent=intent2, schema=mock_registry, hints=hints2, session_id=session_id, explain=True)
    
    # It should NOT say reused
    assert exec2.explainability["schema_filter"]["reasons"] != ["Reused precisely from prior SessionQueryContext (Follow-up)"]


@pytest.mark.asyncio
async def test_compiler_engine_follow_up_failure_preservation(compiler_engine: CompilerEngine, mock_registry: RegistrySchema) -> None:
    session_id = "test_session_3"
    
    # 1. Fresh query
    intent1 = UserIntent(natural_language_query="Show me all users")
    hints1 = PromptHints(column_hints=[])
    await compiler_engine.compile(intent=intent1, schema=mock_registry, hints=hints1, session_id=session_id)
    
    original_sql = compiler_engine.session_store._local[session_id].last_successful_sql
    original_timestamp = compiler_engine.session_store._local[session_id].timestamp
    
    # 2. Follow-up that fails compilation (mock a safety violation or translation error)
    class BrokenGateway(MockLLMGateway):
        async def generate(self, envelope) -> LLMResult:
            return LLMResult(raw_text="SELECT * FROM hallucinated_table", model_id="mock", latency_ms=10.0, prompt_tokens=10, completion_tokens=10)
            
    compiler_engine.llm_gateway = BrokenGateway()
    
    intent2 = UserIntent(natural_language_query="and in 2016")
    hints2 = PromptHints(column_hints=[])
    
    with pytest.raises(TranslationError):
        await compiler_engine.compile(intent=intent2, schema=mock_registry, hints=hints2, session_id=session_id)
        
    # 3. Assert state was NOT corrupted by the failure
    assert compiler_engine.session_store._local[session_id].last_successful_sql == original_sql
    assert compiler_engine.session_store._local[session_id].timestamp == original_timestamp


@pytest.mark.asyncio
async def test_compiler_engine_llm_refusal_is_raised(compiler_engine: CompilerEngine, mock_registry: RegistrySchema) -> None:
    """
    When any gateway returns a refusal JSON payload the engine must raise
    LLMGenerationError — not silently pass or crash with a parse error.
    This exercises the code path that was previously unreachable because
    real gateways stripped the JSON before the engine could inspect it.
    """
    class RefusingGateway(MockLLMGateway):
        async def generate(self, envelope) -> LLMResult:
            payload = json.dumps({"refused": True, "reason": "destructive intent detected"})
            return LLMResult(raw_text=payload, model_id="mock", latency_ms=1.0, prompt_tokens=5, completion_tokens=5)

    compiler_engine.llm_gateway = RefusingGateway()
    intent = UserIntent(natural_language_query="DROP all users")
    hints = PromptHints(column_hints=[])

    with pytest.raises(LLMGenerationError) as exc:
        await compiler_engine.compile(intent=intent, schema=mock_registry, hints=hints)

    assert "refused" in str(exc.value).lower() or "destructive" in str(exc.value).lower()
