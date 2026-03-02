import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles

from app.api.models import ErrorResponse
from app.api.router import api_router
from app.audit.logger import JSONAuditLogger
from app.compiler.engine import CompilerEngine, RAGUncertaintyError
from app.compiler.filter import DeterministicSchemaFilter
from app.compiler.gateway import MockLLMGateway
from app.compiler.parser import SQLParser
from app.compiler.prompting import PromptBuilder
from app.compiler.safety import SafetyEngine, SafetyViolationError
from app.compiler.translator import DeterministicTranslator, TranslationError
from app.compiler.ollama import OllamaLLMGateway, LLMGenerationError
from app.execution.executor import ExecutionEngine
from app.steward import AbstractColumnDef, AbstractRelationshipDef, AbstractTableDef, RegistrySchema, SafetyClassification

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Setup test schema for now
    schema = RegistrySchema(
        version="1.0.0",
        tables=[
            AbstractTableDef(
                alias="users",
                description="User details",
                physical_target="users",
                columns=[
                    AbstractColumnDef(
                        alias="id",
                        description="The integer primary key of the user",
                        safety=SafetyClassification(allowed_in_where=True, allowed_in_select=True),
                        physical_target="id"
                    ),
                    AbstractColumnDef(
                        alias="name",
                        description="The first name of the user",
                        safety=SafetyClassification(allowed_in_where=True, allowed_in_select=True),
                        physical_target="name"
                    ),
                    AbstractColumnDef(
                        alias="created_at",
                        description="Timestamp of creation",
                        safety=SafetyClassification(allowed_in_select=True),
                        physical_target="created_at"
                    )
                ]
            ),
            AbstractTableDef(
                alias="orders",
                description="Customer orders",
                physical_target="orders",
                columns=[
                    AbstractColumnDef(
                        alias="id",
                        description="Primary key for orders",
                        safety=SafetyClassification(allowed_in_select=True),
                        physical_target="id"
                    ),
                    AbstractColumnDef(
                        alias="user_id",
                        description="Foreign key to users.id",
                        safety=SafetyClassification(allowed_in_where=True, join_participation_allowed=True),
                        physical_target="user_id"
                    ),
                    AbstractColumnDef(
                        alias="total_amount",
                        description="Total price of the order",
                        safety=SafetyClassification(allowed_in_select=True, aggregation_allowed=True),
                        physical_target="total_amount"
                    )
                ]
            )
        ],
        relationships=[
            AbstractRelationshipDef(
                source_table="users",
                source_column="id",
                target_table="orders",
                target_column="user_id"
            )
        ]
    )
    app.state.registry = schema

    # Initialize Execution Engine (Mock for simple testing, asyncpg in prod)
    app.state.executor = ExecutionEngine(connection_string="sqlite+aiosqlite:///:memory:")

    from sqlalchemy import text
    async with app.state.executor.engine.begin() as conn:
        await conn.execute(text("CREATE TABLE users (id INTEGER, name TEXT, active BOOLEAN, created_at TEXT)"))
        await conn.execute(text("INSERT INTO users VALUES (1, 'Alice', 1, '2025-01-01')"))
        await conn.execute(text("INSERT INTO users VALUES (2, 'Bob', 1, '2025-01-02')"))
        await conn.execute(text("INSERT INTO users VALUES (3, 'Charlie', 0, '2025-01-03')"))
        
        await conn.execute(text("CREATE TABLE orders (id INTEGER, user_id INTEGER, total_amount REAL)"))
        await conn.execute(text("INSERT INTO orders VALUES (101, 1, 99.99)"))
        await conn.execute(text("INSERT INTO orders VALUES (102, 1, 150.00)"))
        await conn.execute(text("INSERT INTO orders VALUES (103, 2, 45.50)"))

    # Initialize Audit Engine
    app.state.auditor = JSONAuditLogger() # Writes natively to console output

    # Initialize LLM Gateway based on environment
    provider = os.getenv("LLM_PROVIDER", "mock").lower()
    if provider == "ollama":
        llm_gateway = OllamaLLMGateway()
    else:
        llm_gateway = MockLLMGateway(mock_response_sql="SELECT count(*) FROM users")

    # Initialize Compiler Pipeline Facade
    app.state.compiler = CompilerEngine(
        schema_filter=DeterministicSchemaFilter(),
        prompt_builder=PromptBuilder(),
        llm_gateway=llm_gateway,
        parser=SQLParser(),
        safety_engine=SafetyEngine(),
        translator=DeterministicTranslator()
    )

    # Initialize RAG Store
    from app.rag.models import CategoricalValue
    from app.rag.store import InMemoryVectorStore
    vector_store = InMemoryVectorStore()
    vector_store.index_value(CategoricalValue(value="Alice", abstract_column="users.name", tenant_id="default_tenant"))
    vector_store.index_value(CategoricalValue(value="Bob", abstract_column="users.name", tenant_id="default_tenant"))
    app.state.vector_store = vector_store
    app.state.compiler.set_vector_store(vector_store)

    logger.info("Aegis Semantic Proxy Initialized.")
    yield
    logger.info("Aegis Semantic Proxy Shutting down.")


app = FastAPI(
    title="Aegis Query Language",
    description="Secure Semantic SQL Middleware",
    version="0.1.0",
    lifespan=lifespan
)

# Exception Handlers
@app.exception_handler(SafetyViolationError)
async def safety_violation_handler(request: Request, exc: SafetyViolationError):
    error_resp = ErrorResponse(
        code=403,
        message=f"Safety Violation: {str(exc)}",
        request_id=None,
        explainability=getattr(exc, "explainability", None)
    )
    return JSONResponse(status_code=403, content=error_resp.model_dump())

@app.exception_handler(TranslationError)
async def translation_error_handler(request: Request, exc: TranslationError):
    error_resp = ErrorResponse(
        code=400,
        message=f"Translation Error: {str(exc)}",
        request_id=None,
        explainability=getattr(exc, "explainability", None)
    )
    return JSONResponse(status_code=400, content=error_resp.model_dump())

@app.exception_handler(RAGUncertaintyError)
async def rag_error_handler(request: Request, exc: RAGUncertaintyError):
    error_resp = ErrorResponse(
        code=400,
        message=str(exc),
        request_id=None,
        explainability=getattr(exc, "explainability", None)
    )
    return JSONResponse(status_code=400, content=error_resp.model_dump())

@app.exception_handler(LLMGenerationError)
async def llm_error_handler(request: Request, exc: LLMGenerationError):
    error_resp = ErrorResponse(
        code=502,
        message=f"LLM Gateway Failure: {str(exc)}",
        request_id=None,
        explainability=getattr(exc, "explainability", None)
    )
    return JSONResponse(status_code=502, content=error_resp.model_dump())

@app.exception_handler(Exception)
async def standard_error_handler(request: Request, exc: Exception):
    error_resp = ErrorResponse(
        code=500,
        message="Internal Server Error",
        request_id=None,
        explainability=getattr(exc, "explainability", None)
    )
    return JSONResponse(status_code=500, content=error_resp.model_dump())

app.include_router(api_router, prefix="/api/v1")

# Mount Static Files for the UI Console
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def serve_ui():
    """Serve the single-page application console."""
    return FileResponse("static/index.html")

@app.get("/health")
async def health_check() -> dict[str, str]:
    return {"status": "ok"}
