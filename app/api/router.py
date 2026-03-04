import uuid

from fastapi import APIRouter, BackgroundTasks, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.requests import Request

from app.api.compiler import MetadataCompiler
from app.api.meta_models import MetadataVersion
from app.api.models import (
    MetadataCompileResponse,
    ProtocolMetadataVersion,
    QueryExecuteResponse,
    QueryGenerateResponse,
    QueryRequest,
)
from app.audit import QueryAuditEvent
from app.compiler.engine import CompilerEngine
from app.compiler.models import PromptHints, UserIntent
from app.execution import ExecutionContext
from app.execution.interfaces import ExecutionLayer
from app.steward import RegistrySchema

api_router = APIRouter()


def get_compiler(request: Request) -> CompilerEngine:
    return request.app.state.compiler

def get_executor(request: Request) -> ExecutionLayer:
    return request.app.state.executor

def get_auditor(request: Request):
    return request.app.state.auditor

def get_registry(request: Request) -> RegistrySchema:
    return request.app.state.registry

async def get_registry_runtime_db_session(request: Request) -> AsyncSession:
    async with request.app.state.registry_runtime_session_factory() as session:
        yield session

async def get_registry_admin_db_session(request: Request) -> AsyncSession:
    async with request.app.state.registry_admin_session_factory() as session:
        yield session

async def get_steward_db_session(request: Request) -> AsyncSession:
    async with request.app.state.steward_session_factory() as session:
        yield session


@api_router.post("/query/generate", response_model=QueryGenerateResponse)
async def generate_query(
    payload: QueryRequest,
    compiler: CompilerEngine = Depends(get_compiler),
    registry: RegistrySchema = Depends(get_registry)
) -> QueryGenerateResponse:
    """
    Compiles natural language into an ExecutableQuery, strictly omitting physical DB execution.
    """
    intent = UserIntent(natural_language_query=payload.intent)
    hints = PromptHints(column_hints=payload.schema_hints)
    
    executable = await compiler.compile(
        schema=registry,
        intent=intent,
        hints=hints,
        explain=payload.explain
    )
    
    return QueryGenerateResponse(
        query_id=executable.query_id or "",
        sql=executable.sql,
        parameters=executable.parameters,
        latency_ms=executable.compilation_latency_ms or 0.0,
        explainability=executable.explainability
    )


@api_router.post("/query/execute", response_model=QueryExecuteResponse)
async def execute_query(
    payload: QueryRequest,
    background_tasks: BackgroundTasks,
    compiler: CompilerEngine = Depends(get_compiler),
    executor: ExecutionLayer = Depends(get_executor),
    auditor=Depends(get_auditor),
    registry: RegistrySchema = Depends(get_registry)
) -> QueryExecuteResponse:
    """
    Compiles and executes the query against the physical database.
    Dispatches asynchronous audit sink logging.
    """
    intent = UserIntent(natural_language_query=payload.intent)
    hints = PromptHints(column_hints=payload.schema_hints)
    
    # Compile
    executable = await compiler.compile(
        schema=registry,
        intent=intent,
        hints=hints,
        explain=payload.explain
    )
    
    # Execute
    context = ExecutionContext(
            tenant_id="default_tenant",
            user_id="api_user",
            metadata={}
        )
    result = await executor.execute(executable, context=context)
    
    # Background audit log mapping
    event = QueryAuditEvent(
        query_id=executable.query_id or str(uuid.uuid4()),
        tenant_id=context.tenant_id,
        user_id=context.user_id,
        natural_language_query=payload.intent,
        abstract_query=executable.sql, # Tracking compiled sql 
        physical_query=executable.sql,
        registry_version=executable.registry_version,
        safety_engine_version=executable.safety_engine_version,
        abstract_query_hash=executable.abstract_query_hash,
        latency_ms=executable.compilation_latency_ms or 0.0,
        prompt_tokens=0,
        completion_tokens=0,
        status="SUCCESS",
        error_message=None,
        row_limit_applied=executable.row_limit_applied
    )
    
    # Add audit dispatch to non-blocking tasks list
    background_tasks.add_task(auditor.record, event)
    
    return QueryExecuteResponse(
        query_id=executable.query_id or "",
        results=result.rows,
        row_count=len(result.rows),
        execution_latency_ms=0.0,
        explainability=executable.explainability
    )


@api_router.get("/metadata/versions", response_model=list[ProtocolMetadataVersion])
async def list_metadata_versions(
    session: AsyncSession = Depends(get_registry_runtime_db_session)
) -> list[ProtocolMetadataVersion]:
    """Retrieve all metadata schema versions."""
    res = await session.execute(select(MetadataVersion).order_by(MetadataVersion.created_at.desc()))
    versions = res.scalars().all()
    
    return [
        ProtocolMetadataVersion(
            version_id=str(v.version_id),
            status=v.status,
            created_at=v.created_at.isoformat()
        ) for v in versions
    ]


@api_router.post("/metadata/compile/{version_id}", response_model=MetadataCompileResponse)
async def compile_metadata_version(
    version_id: uuid.UUID,
    session: AsyncSession = Depends(get_registry_admin_db_session)
) -> MetadataCompileResponse:
    """Compile an active metadata version into a runtime Aegis Registry artifact."""
    artifact = await MetadataCompiler.compile_version(
        session=session,
        version_id=version_id,
        actor="admin_api"
    )
    
    return MetadataCompileResponse(
        artifact_id=str(artifact.artifact_id),
        version_id=str(artifact.version_id),
        artifact_hash=artifact.artifact_hash,
        compiled_at=artifact.compiled_at.isoformat()
    )
