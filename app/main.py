import asyncio
import logging
import os
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from urllib.parse import urlparse, urlunparse

import redis.asyncio as aioredis
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import select as sa_select
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.api.meta_models import (
    CompiledRegistryArtifact,
    MetadataColumnValue,
    MetadataVersion,
)
from app.api.models import ErrorResponse
from app.api.router import api_router
from app.audit.logger import JSONAuditLogger
from app.compiler.engine import CompilerEngine, RAGUncertaintyError
from app.compiler.filter import DeterministicSchemaFilter
from app.compiler.gateway import MockLLMGateway
from app.compiler.interfaces import LLMGatewayProtocol
from app.compiler.ollama import LLMGenerationError, OllamaLLMGateway
from app.compiler.parser import SQLParser
from app.compiler.prompting import PromptBuilder
from app.compiler.safety import SafetyEngine, SafetyViolationError
from app.compiler.session_store import SessionStore
from app.compiler.translator import DeterministicTranslator, TranslationError
from app.execution.executor import ExecutionEngine
from app.rag.builder import RagDivergenceError, build_from_artifact, build_test_store
from app.rag.store import InMemoryVectorStore
from app.steward import (
    AbstractColumnDef,
    AbstractRelationshipDef,
    AbstractTableDef,
    RegistrySchema,
    SafetyClassification,
)
from app.vault import get_secrets_manager

load_dotenv()


def _mask_redis_url(redis_url: str) -> str:
    """Return a log-safe version of a Redis URL with the password redacted."""
    parsed = urlparse(redis_url)
    if parsed.password:
        masked = parsed.netloc.replace(f":{parsed.password}@", ":***@")
        return urlunparse(parsed._replace(netloc=masked))
    return redis_url


async def _boot_rag_index(app: FastAPI) -> None:
    """Background task: build the RAG index from the active compiled artifact."""
    try:
        async with app.state.registry_runtime_session_factory() as val_session:
            stmt = (
                sa_select(CompiledRegistryArtifact)
                .join(
                    MetadataVersion,
                    CompiledRegistryArtifact.version_id
                    == MetadataVersion.version_id,
                )
                .where(MetadataVersion.status == "active")
                .order_by(CompiledRegistryArtifact.compiled_at.desc())
                .limit(1)
            )
            result = await val_session.execute(stmt)
            artifact = result.scalar_one_or_none()
            if artifact is None:
                logger.info("RAG: no active artifact — index stays empty")
                return

            col_values = await _fetch_rag_column_values_for_version(
                artifact.version_id, val_session
            )

        new_store = await build_from_artifact(
            artifact_blob=artifact.artifact_blob,
            version_id=str(artifact.version_id),
            tenant_id="default_tenant",
            artifact_version=artifact.artifact_hash,
            column_values=col_values,
        )
        app.state.vector_store = new_store
        app.state.compiler.set_vector_store(new_store)
        logger.info("RAG: index ready — %s", artifact.artifact_hash[:12])
    except RagDivergenceError:
        logger.warning("RAG: divergence at boot — index stays empty")
    except Exception:
        logger.exception("RAG: boot build failed — index stays empty")


async def _fetch_rag_column_values_for_version(
    version_id: uuid.UUID,
    session: AsyncSession,
) -> dict[str, list[str]]:
    """Fetch all active RAG values for a version, grouped by column_id string."""
    stmt = (
        sa_select(MetadataColumnValue.column_id, MetadataColumnValue.value)
        .where(
            MetadataColumnValue.active.is_(True),
            MetadataColumnValue.version_id == version_id,
        )
        .order_by(MetadataColumnValue.column_id, MetadataColumnValue.value)
    )
    result = await session.execute(stmt)
    groups: dict[str, list[str]] = {}
    for col_id, value in result.all():
        key = str(col_id)
        groups.setdefault(key, []).append(value)
    return groups

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _build_test_registry_schema() -> RegistrySchema:
    """Returns the deterministic static schema used in TESTING mode."""
    return RegistrySchema(
        version="1.0.0",
        tables=[
            AbstractTableDef(
                alias="users",
                description="User details",
                physical_target="users",
                columns=[
                    AbstractColumnDef(
                        alias="id",
                        description="PK",
                        safety=SafetyClassification(
                            allowed_in_where=True, allowed_in_select=True
                        ),
                        physical_target="id",
                    ),
                    AbstractColumnDef(
                        alias="name",
                        description="Name",
                        safety=SafetyClassification(
                            allowed_in_where=True, allowed_in_select=True
                        ),
                        physical_target="name",
                    ),
                    AbstractColumnDef(
                        alias="active",
                        description="Active",
                        safety=SafetyClassification(
                            allowed_in_where=True, allowed_in_select=True
                        ),
                        physical_target="active",
                    ),
                    AbstractColumnDef(
                        alias="created_at",
                        description="Creation",
                        safety=SafetyClassification(allowed_in_select=True),
                        physical_target="created_at",
                    ),
                ],
            ),
            AbstractTableDef(
                alias="orders",
                description="Customer orders",
                physical_target="orders",
                columns=[
                    AbstractColumnDef(
                        alias="id",
                        description="PK",
                        safety=SafetyClassification(allowed_in_select=True),
                        physical_target="id",
                    ),
                    AbstractColumnDef(
                        alias="user_id",
                        description="FK",
                        safety=SafetyClassification(
                            allowed_in_where=True,
                            join_participation_allowed=True,
                        ),
                        physical_target="user_id",
                    ),
                    AbstractColumnDef(
                        alias="total_amount",
                        description="Total",
                        safety=SafetyClassification(
                            allowed_in_select=True, aggregation_allowed=True
                        ),
                        physical_target="total_amount",
                    ),
                ],
            ),
        ],
        relationships=[
            AbstractRelationshipDef(
                source_table="users",
                source_column="id",
                target_table="orders",
                target_column="user_id",
            )
        ],
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    from app.steward.loader import RegistryLoader

    runtime_db_url = os.getenv("DB_URL_RUNTIME", os.getenv("DATABASE_URL"))
    registry_runtime_db_url = os.getenv(
        "DB_URL_REGISTRY_RUNTIME", os.getenv("DATABASE_URL")
    )
    steward_db_url = os.getenv("DB_URL_STEWARD", os.getenv("DATABASE_URL"))
    registry_admin_db_url = os.getenv(
        "DB_URL_REGISTRY_ADMIN", os.getenv("DATABASE_URL")
    )

    if not (
        runtime_db_url
        and registry_runtime_db_url
        and steward_db_url
        and registry_admin_db_url
    ):
        raise RuntimeError(
            "Least Privilege PostgreSQL connection URLs are not fully configured."
        )

    secrets_mgr = get_secrets_manager()

    def _secure_url(raw_url: str, role_name: str) -> str:
        url_obj = make_url(raw_url)
        if url_obj.get_dialect().name in ["sqlite", "sqlite+aiosqlite"]:
            return raw_url  # Local test memory URI remains unmodified
        password = secrets_mgr.get_database_password(role_name)
        url_obj = url_obj.set(password=password)
        if os.getenv("ENVIRONMENT") == "production":
            new_query = dict(url_obj.query)
            new_query["ssl"] = "require"
            url_obj = url_obj.set(query=new_query)
        return url_obj.render_as_string(hide_password=False)

    secure_registry_runtime_db_url = _secure_url(
        registry_runtime_db_url, "user_aegis_registry_runtime"
    )
    secure_steward_db_url = _secure_url(steward_db_url, "user_aegis_steward")
    secure_registry_admin_db_url = _secure_url(
        registry_admin_db_url, "user_aegis_registry_admin"
    )
    secure_runtime_db_url = _secure_url(runtime_db_url, "user_aegis_runtime")

    app.state.registry_runtime_session_factory = async_sessionmaker(
        create_async_engine(secure_registry_runtime_db_url),
        expire_on_commit=False,
    )
    app.state.steward_session_factory = async_sessionmaker(
        create_async_engine(secure_steward_db_url), expire_on_commit=False
    )
    app.state.registry_admin_session_factory = async_sessionmaker(
        create_async_engine(secure_registry_admin_db_url), expire_on_commit=False
    )
    app.state.runtime_session_factory = async_sessionmaker(
        create_async_engine(secure_runtime_db_url), expire_on_commit=False
    )

    if os.getenv("TESTING") == "true":
        logger.info(
            "[*] Testing mode detected: Seeding deterministic static RegistrySchema"
        )
        schema = _build_test_registry_schema()
    else:
        async with app.state.registry_runtime_session_factory() as session:
            _loaded = await RegistryLoader.load_active_schema(session)
        if _loaded is None:
            logger.warning(
                "[!] No Active Metadata version found. Serving empty schema fallback."
            )
            schema = RegistrySchema(version="0.0.0", tables=[], relationships=[])
        else:
            schema = _loaded

    app.state.registry = schema
    app.state.executor = ExecutionEngine(connection_string=secure_runtime_db_url)

    redis_url = os.getenv("REDIS_URL")
    redis_client = (
        aioredis.from_url(redis_url, decode_responses=True) if redis_url else None
    )
    session_store = SessionStore(redis_client=redis_client)
    if redis_url:
        logger.info("Session store: Redis (%s)", _mask_redis_url(redis_url))
    else:
        logger.info(
            "Session store: in-memory (set REDIS_URL to enable Redis)"
        )

    app.state.auditor = JSONAuditLogger()

    provider = os.getenv("LLM_PROVIDER", "mock").lower()
    llm_gateway: LLMGatewayProtocol
    if provider == "ollama":
        llm_gateway = OllamaLLMGateway()
    else:
        llm_gateway = MockLLMGateway(
            mock_response_sql="SELECT count(*) FROM users"
        )

    app.state.compiler = CompilerEngine(
        schema_filter=DeterministicSchemaFilter(),
        prompt_builder=PromptBuilder(),
        llm_gateway=llm_gateway,
        parser=SQLParser(),
        safety_engine=SafetyEngine(),
        translator=DeterministicTranslator(),
    )
    app.state.compiler.session_store = session_store

    # Initialize RAG Store
    if os.getenv("TESTING") == "true":
        vector_store = build_test_store()
        app.state.vector_store = vector_store
        app.state.compiler.set_vector_store(vector_store)
    else:
        # Start with empty store; background task will swap when ready
        empty_store = InMemoryVectorStore()
        app.state.vector_store = empty_store
        app.state.compiler.set_vector_store(empty_store)
        _ = asyncio.create_task(_boot_rag_index(app))

    logger.info("Aegis Semantic Proxy Initialized.")
    yield
    await session_store.close()
    await app.state.executor.close()
    logger.info("Aegis Semantic Proxy Shutting down.")


app = FastAPI(
    title="Aegis Query Language",
    description="Secure Semantic SQL Middleware",
    version="0.1.0",
    lifespan=lifespan,
)


@app.exception_handler(SafetyViolationError)
async def safety_violation_handler(
    request: Request, exc: SafetyViolationError
) -> JSONResponse:
    error_resp = ErrorResponse(
        code=403,
        message=f"Safety Violation: {str(exc)}",
        request_id=None,
        explainability=getattr(exc, "explainability", None),
    )
    return JSONResponse(status_code=403, content=error_resp.model_dump())


@app.exception_handler(TranslationError)
async def translation_error_handler(
    request: Request, exc: TranslationError
) -> JSONResponse:
    error_resp = ErrorResponse(
        code=400,
        message=f"Translation Error: {str(exc)}",
        request_id=None,
        explainability=getattr(exc, "explainability", None),
    )
    return JSONResponse(status_code=400, content=error_resp.model_dump())


@app.exception_handler(RAGUncertaintyError)
async def rag_error_handler(
    request: Request, exc: RAGUncertaintyError
) -> JSONResponse:
    error_resp = ErrorResponse(
        code=400,
        message=str(exc),
        request_id=None,
        explainability=getattr(exc, "explainability", None),
    )
    return JSONResponse(status_code=400, content=error_resp.model_dump())


@app.exception_handler(LLMGenerationError)
async def llm_error_handler(
    request: Request, exc: LLMGenerationError
) -> JSONResponse:
    error_resp = ErrorResponse(
        code=502,
        message=f"LLM Gateway Failure: {str(exc)}",
        request_id=None,
        explainability=getattr(exc, "explainability", None),
    )
    return JSONResponse(status_code=502, content=error_resp.model_dump())


@app.exception_handler(Exception)
async def standard_error_handler(
    request: Request, exc: Exception
) -> JSONResponse:
    error_resp = ErrorResponse(
        code=500,
        message="Internal Server Error",
        request_id=None,
        explainability=getattr(exc, "explainability", None),
    )
    return JSONResponse(status_code=500, content=error_resp.model_dump())


app.include_router(api_router, prefix="/api/v1")

app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def serve_ui() -> FileResponse:
    """Serve the single-page application console."""
    return FileResponse("static/index.html")


@app.get("/health")
async def health_check(request: Request) -> dict[str, str | bool]:
    store = getattr(request.app.state, "vector_store", None)
    index_ready: bool = store.index_ready if store is not None else False
    return {"status": "ok", "index_ready": index_ready}
