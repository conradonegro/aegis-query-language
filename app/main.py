import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.api.models import ErrorResponse
from app.api.router import api_router
from app.audit.logger import JSONAuditLogger
from app.compiler.engine import CompilerEngine
from app.compiler.filter import DeterministicSchemaFilter
from app.compiler.gateway import MockLLMGateway
from app.compiler.parser import SQLParser
from app.compiler.prompting import PromptBuilder
from app.compiler.safety import SafetyEngine, SafetyViolationError
from app.compiler.translator import DeterministicTranslator, TranslationError
from app.execution.executor import ExecutionEngine
from app.steward import AbstractIdentifierDef, RegistrySchema, SafetyClassification

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Setup test schema for now
    schema = RegistrySchema(
        version="1.0.0",
        identifiers=[
            AbstractIdentifierDef(
                alias="users",
                description="User details",
                safety=SafetyClassification(allowed_in_select=True),
                physical_target="auth.users"
            )
        ]
    )
    app.state.registry = schema

    # Initialize Execution Engine (Mock for simple testing, asyncpg in prod)
    app.state.executor = ExecutionEngine(connection_string="sqlite+aiosqlite:///:memory:")

    # Initialize Audit Engine
    app.state.auditor = JSONAuditLogger() # Writes natively to console output

    # Initialize Compiler Pipeline Facade
    app.state.compiler = CompilerEngine(
        schema_filter=DeterministicSchemaFilter(),
        prompt_builder=PromptBuilder(),
        llm_gateway=MockLLMGateway(mock_response_sql="SELECT count(*) FROM users"),
        parser=SQLParser(),
        safety_engine=SafetyEngine(),
        translator=DeterministicTranslator()
    )

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
        request_id=None
    )
    return JSONResponse(status_code=403, content=error_resp.model_dump())

@app.exception_handler(TranslationError)
async def translation_error_handler(request: Request, exc: TranslationError):
    error_resp = ErrorResponse(
        code=400,
        message=f"Translation Error: {str(exc)}",
        request_id=None
    )
    return JSONResponse(status_code=400, content=error_resp.model_dump())

@app.exception_handler(Exception)
async def standard_error_handler(request: Request, exc: Exception):
    error_resp = ErrorResponse(
        code=500,
        message="Internal Server Error",
        request_id=None
    )
    return JSONResponse(status_code=500, content=error_resp.model_dump())

app.include_router(api_router, prefix="/api/v1")

@app.get("/health")
async def health_check() -> dict[str, str]:
    return {"status": "ok"}
