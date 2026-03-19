import asyncio
import logging
import os
import uuid
from datetime import UTC, datetime
from typing import Annotated, Any, Literal, cast

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from starlette.requests import Request

from app.api.auth import (
    ResolvedCredential,
    _hash_api_key,
    generate_api_key,
    require_admin_credential,
    require_query_credential,
)
from app.api.compiler import MetadataCompiler
from app.api.dependencies import (
    get_registry_admin_db_session,
    get_registry_runtime_db_session,
    get_runtime_db_session,
    get_steward_db_session,
)
from app.api.meta_models import (
    ChatMessage,
    ChatSession,
    CompiledRegistryArtifact,
    MetadataAudit,
    MetadataColumn,
    MetadataColumnValue,
    MetadataRelationship,
    MetadataTable,
    MetadataVersion,
    TenantCredential,
)
from app.api.models import (
    ColumnUpdateRequest,
    ColumnValueBulkImportRequest,
    ColumnValueBulkImportResponse,
    ColumnValueCreateRequest,
    CredentialCreateRequest,
    CredentialCreateResponse,
    CredentialListItem,
    ExplainabilityContext,
    MetadataCompileResponse,
    ProtocolColumn,
    ProtocolColumnValue,
    ProtocolMetadataVersion,
    ProtocolRelationship,
    ProtocolSchemaResponse,
    ProtocolTable,
    QueryExecuteResponse,
    QueryGenerateResponse,
    QueryRequestWithHints,
    TableUpdateRequest,
    VersionCreateRequest,
    VersionStatusUpdateRequest,
)
from app.audit import QueryAuditEvent
from app.audit.chaining import compute_audit_row_hash, get_canonical_json
from app.compiler.backend_hints import BackendHintContext, build_backend_hints
from app.compiler.engine import CompilerEngine
from app.compiler.models import (
    ChatHistoryItem,
    ExecutableQuery,
    PromptHints,
    UserIntent,
)
from app.compiler.provider_config import (
    MalformedProviderIdError,
    ProviderNotAllowedError,
    assert_provider_allowed,
    parse_provider_id,
)
from app.execution import ExecutionContext
from app.execution.interfaces import ExecutionLayer
from app.rag.normalizer import normalize as normalize_rag_value
from app.steward import RegistrySchema
from app.vault import VaultMissingSecretError, get_secrets_manager

logger = logging.getLogger(__name__)

async def _persist_chat_turn(
    session: AsyncSession,
    session_id: uuid.UUID,
    user_msg: ChatMessage,
    assistant_msg: ChatMessage,
) -> None:
    """
    Sequences and persists a user+assistant message pair inside a single DB
    transaction, using SELECT … FOR UPDATE on the parent ChatSession row to
    serialise concurrent writers across all workers.

    The FOR UPDATE lock is held until the transaction commits, so no two workers
    can read the same MAX(sequence_number) for the same session simultaneously.
    SQLite (used in tests) silently ignores FOR UPDATE, which is fine because
    tests are single-threaded and the unique constraint still guards correctness.
    """
    locked = await session.scalar(
        select(ChatSession)
        .where(ChatSession.session_id == session_id)
        .with_for_update()
    )
    if locked is None:
        raise HTTPException(
            status_code=500,
            detail="Chat session row missing unexpectedly; this is a server bug.",
        )

    last_seq: int = (
        await session.scalar(
            select(func.max(ChatMessage.sequence_number)).where(
                ChatMessage.session_id == session_id
            )
        )
    ) or 0

    user_msg.sequence_number = last_seq + 1
    assistant_msg.sequence_number = last_seq + 2
    session.add_all([user_msg, assistant_msg])
    await session.commit()


def _validate_provider_id(raw: str | None, credential_id: str) -> str | None:
    """
    Validates and allowlist-checks a client-supplied provider_id.

    Returns the normalised provider_id string, or None if no override was
    requested.  Raises HTTPException(400) for malformed or blocked providers.
    Logs a security warning (with credential ID) before raising for blocked
    providers, without surfacing allowlist details to the client.
    """
    if not raw:
        return None
    try:
        normalised = parse_provider_id(raw)
    except MalformedProviderIdError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    try:
        assert_provider_allowed(normalised)
    except ProviderNotAllowedError:
        logger.warning(
            "security: provider_id '%s' blocked — not in server allowlist "
            "(credential_id=%s)",
            raw,
            credential_id,
        )
        raise HTTPException(
            status_code=400,
            detail="Requested provider not permitted by server configuration.",
        ) from None
    return normalised

api_router = APIRouter()


def get_utc_now() -> datetime:
    """Injectable clock dependency — override in tests for deterministic timestamps."""
    return datetime.now(UTC)


async def _resolve_session(
    payload_session_id: str | None,
    session: AsyncSession,
    tenant_id: str,
) -> tuple[uuid.UUID, list[ChatHistoryItem]]:
    """
    Resolve or create a chat session, scoped to the requesting tenant.

    If payload_session_id is a valid UUID referencing an existing ChatSession
    owned by tenant_id, its message history is loaded and returned.  A session
    UUID belonging to a different tenant is treated as not-found (IDOR guard).
    Otherwise a new session row is created and committed so the PK exists
    before any message writes.
    """
    session_id: uuid.UUID | None = None
    chat_history: list[ChatHistoryItem] = []

    if payload_session_id:
        try:
            session_uuid = uuid.UUID(payload_session_id)
            res = await session.execute(
                select(ChatSession).where(
                    ChatSession.session_id == session_uuid,
                    ChatSession.tenant_id == tenant_id,
                )
            )
            chat_session = res.scalar_one_or_none()
            if chat_session:
                session_id = session_uuid
                msgs_res = await session.execute(
                    select(ChatMessage)
                    .where(ChatMessage.session_id == session_uuid)
                    .order_by(ChatMessage.sequence_number)
                )
                for msg in msgs_res.scalars().all():
                    chat_history.append(ChatHistoryItem(
                        role=cast(
                            Literal["user", "assistant", "system"], msg.role
                        ),
                        content=msg.content,
                    ))
        except ValueError:
            pass  # Ignore invalid UUID format and fall back to none

    if not session_id:
        session_id = uuid.uuid4()
        new_session = ChatSession(session_id=session_id, tenant_id=tenant_id)
        session.add(new_session)
        # Commit to ensure the PK exists before messages are flushed.
        await session.commit()

    return session_id, chat_history


def get_compiler(request: Request) -> CompilerEngine:
    return cast(CompilerEngine, request.app.state.compiler)

def get_executor(request: Request) -> ExecutionLayer:
    return cast(ExecutionLayer, request.app.state.executor)

def get_auditor(request: Request) -> Any:
    return request.app.state.auditor

def get_registry(
    request: Request,
    cred: Annotated[ResolvedCredential, Depends(require_query_credential)],
) -> RegistrySchema:
    schema = cast(
        RegistrySchema | None, request.app.state.registries.get(cred.tenant_id)
    )
    if schema is None:
        raise HTTPException(
            status_code=503,
            detail=(
                "No compiled schema available for this tenant. "
                "Compile a registry artifact first."
            ),
        )
    return schema


@api_router.post("/query/generate", response_model=QueryGenerateResponse)
async def generate_query(
    payload: QueryRequestWithHints,
    compiler: Annotated[CompilerEngine, Depends(get_compiler)],
    auditor: Annotated[Any, Depends(get_auditor)],
    registry: Annotated[RegistrySchema, Depends(get_registry)],
    session: Annotated[AsyncSession, Depends(get_runtime_db_session)],
    cred: Annotated[ResolvedCredential, Depends(require_query_credential)],
    now: Annotated[datetime, Depends(get_utc_now)],
) -> QueryGenerateResponse:
    """
    Compiles natural language into an ExecutableQuery, strictly omitting
    physical DB execution.
    """
    provider_id = _validate_provider_id(payload.provider_id, cred.credential_id)

    if payload.explain and cred.scope != "admin":
        raise HTTPException(
            status_code=403,
            detail="explain=true requires admin scope.",
        )

    intent = UserIntent(
        natural_language_query=payload.intent,
        source_database=payload.source_database,
    )
    ctx = BackendHintContext(tenant_id=cred.tenant_id, now=now)
    hints = PromptHints(column_hints=build_backend_hints(ctx))
    if os.getenv("SCHEMA_HINTS", "").lower() == "on":
        hints.column_hints.extend(payload.schema_hints)

    # Audit locals — populated as execution proceeds; survive all exception paths.
    _exec: ExecutableQuery | None = None
    _llm_expl: dict[str, Any] = {}
    _audit_status = "SUCCESS"
    _audit_error_type: str | None = None
    _audit_error_msg: str | None = None

    try:
        session_id, chat_history = await _resolve_session(
            payload.session_id, session, tenant_id=cred.tenant_id
        )

        _exec = await compiler.compile(
            schema=registry,
            intent=intent,
            hints=hints,
            explain=payload.explain,
            chat_history=chat_history,
            provider_id=provider_id,
            session_id=str(session_id),
            tenant_id=cred.tenant_id,
        )
        _llm_expl = (
            _exec.explainability.get("llm", {})
            if _exec.explainability
            else {}
        )

        await _persist_chat_turn(
            session=session,
            session_id=session_id,
            user_msg=ChatMessage(
                message_id=uuid.uuid4(),
                session_id=session_id,
                sequence_number=0,  # overwritten inside _persist_chat_turn
                role="user",
                content=intent.natural_language_query,
                provider_id=provider_id,
            ),
            assistant_msg=ChatMessage(
                message_id=uuid.uuid4(),
                session_id=session_id,
                sequence_number=0,  # overwritten inside _persist_chat_turn
                role="assistant",
                content=(
                    _exec.abstract_sql
                    if _exec.abstract_sql is not None
                    else _exec.sql
                ),
                provider_id=(
                    _llm_expl.get("provider")
                    if _exec.explainability
                    else provider_id
                ),
                prompt_tokens=_exec.llm_prompt_tokens,
                completion_tokens=_exec.llm_completion_tokens,
            ),
        )

        explain_ctx = (
            ExplainabilityContext.model_validate(_exec.explainability)
            if _exec.explainability is not None
            else None
        )
        return QueryGenerateResponse(
            query_id=_exec.query_id or "",
            session_id=str(session_id),
            sql=_exec.sql,
            parameters=_exec.parameters,
            latency_ms=_exec.compilation_latency_ms or 0.0,
            source_database_used=_exec.source_database_used,
            explainability=explain_ctx,
        )

    except Exception as exc:
        _audit_status = "FAILURE"
        _audit_error_type = type(exc).__name__
        _audit_error_msg = str(exc)
        raise

    finally:
        asyncio.create_task(
            auditor.record(
                QueryAuditEvent(
                    query_id=(_exec.query_id or str(uuid.uuid4()))
                    if _exec else str(uuid.uuid4()),
                    tenant_id=cred.tenant_id,
                    user_id=cred.user_id,
                    credential_id=cred.credential_id,
                    natural_language_query=payload.intent,
                    operation="generate",
                    status=_audit_status,
                    abstract_query=_exec.abstract_sql if _exec else None,
                    physical_query=_exec.sql if _exec else None,
                    registry_version=_exec.registry_version if _exec else None,
                    safety_engine_version=_exec.safety_engine_version
                    if _exec else None,
                    abstract_query_hash=_exec.abstract_query_hash if _exec else None,
                    latency_ms=_exec.compilation_latency_ms or 0.0 if _exec else 0.0,
                    row_limit_applied=_exec.row_limit_applied if _exec else False,
                    prompt_tokens=_exec.llm_prompt_tokens if _exec else 0,
                    completion_tokens=_exec.llm_completion_tokens if _exec else 0,
                    provider_id=_llm_expl.get("provider") or provider_id,
                    error_type=_audit_error_type,
                    error_message=_audit_error_msg,
                )
            )
        )


def _coerce_row(
    row: dict[str, object],
) -> dict[str, str | int | float | bool | None]:
    out: dict[str, str | int | float | bool | None] = {}
    for k, v in row.items():
        if v is None or isinstance(v, (str, int, float, bool)):
            out[k] = v
        else:
            out[k] = str(v)
    return out


@api_router.post("/query/execute", response_model=QueryExecuteResponse)
async def execute_query(
    payload: QueryRequestWithHints,
    compiler: Annotated[CompilerEngine, Depends(get_compiler)],
    executor: Annotated[ExecutionLayer, Depends(get_executor)],
    auditor: Annotated[Any, Depends(get_auditor)],
    registry: Annotated[RegistrySchema, Depends(get_registry)],
    session: Annotated[AsyncSession, Depends(get_runtime_db_session)],
    cred: Annotated[ResolvedCredential, Depends(require_query_credential)],
    now: Annotated[datetime, Depends(get_utc_now)],
) -> QueryExecuteResponse:
    """
    Compiles and executes the query against the physical database.
    Dispatches an asynchronous audit event for every outcome, including failures.
    """
    provider_id = _validate_provider_id(payload.provider_id, cred.credential_id)

    if payload.explain and cred.scope != "admin":
        raise HTTPException(
            status_code=403,
            detail="explain=true requires admin scope.",
        )

    intent = UserIntent(
        natural_language_query=payload.intent,
        source_database=payload.source_database,
    )
    ctx = BackendHintContext(tenant_id=cred.tenant_id, now=now)
    hints = PromptHints(column_hints=build_backend_hints(ctx))
    if os.getenv("SCHEMA_HINTS", "").lower() == "on":
        hints.column_hints.extend(payload.schema_hints)

    # Audit locals — populated as execution proceeds; survive all exception paths.
    _exec: ExecutableQuery | None = None
    _llm_expl: dict[str, Any] = {}
    _audit_status = "SUCCESS"
    _audit_error_type: str | None = None
    _audit_error_msg: str | None = None

    try:
        session_id, chat_history = await _resolve_session(
            payload.session_id, session, tenant_id=cred.tenant_id
        )

        # Compile
        _exec = await compiler.compile(
            schema=registry,
            intent=intent,
            hints=hints,
            explain=payload.explain,
            chat_history=chat_history,
            provider_id=provider_id,
            session_id=str(session_id),
            tenant_id=cred.tenant_id,
        )
        _llm_expl = (
            _exec.explainability.get("llm", {})
            if _exec.explainability
            else {}
        )

        await _persist_chat_turn(
            session=session,
            session_id=session_id,
            user_msg=ChatMessage(
                message_id=uuid.uuid4(),
                session_id=session_id,
                sequence_number=0,  # overwritten inside _persist_chat_turn
                role="user",
                content=intent.natural_language_query,
                provider_id=provider_id,
            ),
            assistant_msg=ChatMessage(
                message_id=uuid.uuid4(),
                session_id=session_id,
                sequence_number=0,  # overwritten inside _persist_chat_turn
                role="assistant",
                # Store abstract_sql (obfuscated aliases) rather than the physical SQL
                # so the LLM cannot learn physical column/table names from its own
                # prior responses. abstract_sql is always populated by the compiler
                # engine regardless of explain flag.
                content=(
                    _exec.abstract_sql
                    if _exec.abstract_sql is not None
                    else _exec.sql
                ),
                provider_id=(
                    _llm_expl.get("provider")
                    if _exec.explainability
                    else provider_id
                ),
                prompt_tokens=_exec.llm_prompt_tokens,
                completion_tokens=_exec.llm_completion_tokens,
            ),
        )

        # Execute
        context = ExecutionContext(
            tenant_id=cred.tenant_id,
            user_id=cred.user_id,
        )
        result = await executor.execute(_exec, context=context)

        exec_explain_ctx = (
            ExplainabilityContext.model_validate(_exec.explainability)
            if _exec.explainability is not None
            else None
        )
        return QueryExecuteResponse(
            query_id=_exec.query_id or "",
            session_id=str(session_id),
            sql=_exec.sql,
            results=[_coerce_row(r) for r in result.rows],
            row_count=len(result.rows),
            execution_latency_ms=0.0,
            source_database_used=_exec.source_database_used,
            explainability=exec_explain_ctx,
        )

    except Exception as exc:
        _audit_status = "FAILURE"
        _audit_error_type = type(exc).__name__
        _audit_error_msg = str(exc)
        raise

    finally:
        asyncio.create_task(
            auditor.record(
                QueryAuditEvent(
                    query_id=(_exec.query_id or str(uuid.uuid4()))
                    if _exec else str(uuid.uuid4()),
                    tenant_id=cred.tenant_id,
                    user_id=cred.user_id,
                    credential_id=cred.credential_id,
                    natural_language_query=payload.intent,
                    operation="execute",
                    status=_audit_status,
                    abstract_query=_exec.abstract_sql if _exec else None,
                    physical_query=_exec.sql if _exec else None,
                    registry_version=_exec.registry_version if _exec else None,
                    safety_engine_version=_exec.safety_engine_version
                    if _exec else None,
                    abstract_query_hash=_exec.abstract_query_hash if _exec else None,
                    latency_ms=_exec.compilation_latency_ms or 0.0 if _exec else 0.0,
                    row_limit_applied=_exec.row_limit_applied if _exec else False,
                    prompt_tokens=_exec.llm_prompt_tokens if _exec else 0,
                    completion_tokens=_exec.llm_completion_tokens if _exec else 0,
                    provider_id=_llm_expl.get("provider") or provider_id,
                    error_type=_audit_error_type,
                    error_message=_audit_error_msg,
                )
            )
        )


@api_router.get("/metadata/versions", response_model=list[ProtocolMetadataVersion])
async def list_metadata_versions(
    session: Annotated[AsyncSession, Depends(get_registry_runtime_db_session)],
    cred: Annotated[ResolvedCredential, Depends(require_admin_credential)],
) -> list[ProtocolMetadataVersion]:
    """Retrieve all metadata schema versions."""
    res = await session.execute(
        select(MetadataVersion)
        .where(MetadataVersion.tenant_id == cred.tenant_id)
        .order_by(MetadataVersion.created_at.desc())
    )
    versions = res.scalars().all()

    return [
        ProtocolMetadataVersion(
            version_id=str(v.version_id),
            status=v.status,
            created_at=v.created_at.isoformat()
        ) for v in versions
    ]


# Valid forward/backward transitions in the review lifecycle.
_ALLOWED_TRANSITIONS: dict[str, set[str]] = {
    "draft":          {"pending_review"},
    "pending_review": {"active", "draft"},
    "active":         {"archived"},
    "archived":       set(),  # terminal — clone to create a new draft
}

# Semantic audit action for each transition (matches MetadataAudit.action enum).
_TRANSITION_AUDIT_ACTION: dict[tuple[str, str], str] = {
    ("draft",          "pending_review"): "update",   # submitted for review
    ("pending_review", "active"):         "approve",  # reviewer approved
    ("pending_review", "draft"):          "update",   # reviewer rejected / returned
    ("active",         "archived"):       "revoke",   # retired from production
}


@api_router.patch(
    "/metadata/versions/{version_id}/status",
    response_model=ProtocolMetadataVersion,
)
async def update_version_status(
    version_id: uuid.UUID,
    payload: VersionStatusUpdateRequest,
    session: Annotated[AsyncSession, Depends(get_registry_admin_db_session)],
    cred: Annotated[ResolvedCredential, Depends(require_admin_credential)],
) -> ProtocolMetadataVersion:
    """
    Advance or retract a MetadataVersion through its review lifecycle.

    Allowed transitions:
      draft → pending_review
      pending_review → active | draft
      active → archived

    archived is a terminal state — clone the version to start a new draft.
    Every transition is written to the WORM audit chain atomically with the
    status change. If the requested status equals the current status the
    request is treated as a no-op and returns 200 without touching the DB.
    """
    res = await session.execute(
        select(MetadataVersion).where(MetadataVersion.version_id == version_id)
    )
    version = res.scalar_one_or_none()
    if not version:
        raise HTTPException(status_code=404, detail="Version not found")
    if version.tenant_id != cred.tenant_id:
        raise HTTPException(
            status_code=403, detail="Version does not belong to your tenant."
        )

    # Idempotency: already at the target status — return without side-effects
    if version.status == payload.status:
        return ProtocolMetadataVersion(
            version_id=str(version.version_id),
            status=version.status,
            created_at=version.created_at.isoformat(),
        )

    allowed = _ALLOWED_TRANSITIONS.get(version.status, set())
    if payload.status not in allowed:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Transition from '{version.status}' to '{payload.status}' is not "
                f"permitted. Allowed targets: "
                f"{sorted(allowed) if allowed else 'none — this is a terminal state'}."
            ),
        )

    previous_status = version.status

    # ------------------------------------------------------------------
    # Pre-activation checks (active transition only)
    # ------------------------------------------------------------------
    # These must run before any mutations so that failures are clean 422s
    # with no partial state written to the DB.
    existing_active: MetadataVersion | None = None
    if payload.status == "active":
        # A compiled artifact must exist before a version can be activated.
        # compile_version() accepts pending_review, so the artifact should be
        # compiled while the old active version is still serving traffic —
        # no downtime window.
        artifact_check = await session.execute(
            select(CompiledRegistryArtifact).where(
                CompiledRegistryArtifact.version_id == version_id
            )
        )
        if artifact_check.scalar_one_or_none() is None:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"Version {version_id} has no compiled artifact. "
                    "Compile it first via POST /metadata/compile/{version_id}."
                ),
            )

        # Locate any currently active version for this tenant so it can be
        # atomically archived in the same transaction below.
        existing_active_res = await session.execute(
            select(MetadataVersion).where(
                MetadataVersion.tenant_id == cred.tenant_id,
                MetadataVersion.status == "active",
                MetadataVersion.version_id != version_id,
            )
        )
        existing_active = existing_active_res.scalars().first()

        # Approval timestamps — only meaningful for the active transition;
        # placed here to keep both active-only mutations in one branch.
        version.approved_by = cred.user_id
        version.approved_at = datetime.now(UTC)

    # Apply status change
    version.status = payload.status
    if payload.reason:
        version.change_reason = payload.reason

    # ------------------------------------------------------------------
    # WORM audit chain
    # ------------------------------------------------------------------
    # Fetch the chain tip once; it will be threaded through the optional
    # implicit archival record and then the main transition record so that
    # both entries form a valid contiguous chain within this transaction.
    last_audit_res = await session.execute(
        select(MetadataAudit)
        .order_by(MetadataAudit.timestamp.desc(), MetadataAudit.audit_id.desc())
        .limit(1)
    )
    last_row = last_audit_res.scalar_one_or_none()
    chain_tip = last_row.row_hash if last_row else ""

    secrets_mgr = get_secrets_manager()

    # If an old active version is being superseded, archive it first and add
    # its own WORM audit entry before the main activation entry.
    if existing_active is not None:
        existing_active.status = "archived"

        archive_ts = datetime.now(UTC)
        archive_payload = {
            "event": "status_transition",
            "version_id": str(existing_active.version_id),
            "from_status": "active",
            "to_status": "archived",
            "reason": "Superseded by activation of a newer version",
            "status": "SUCCESS",
        }
        archive_canonical = get_canonical_json(archive_payload)
        archive_row_hash = compute_audit_row_hash(
            chain_tip, archive_canonical, archive_ts.isoformat()
        )
        session.add(
            MetadataAudit(
                version_id=existing_active.version_id,
                actor=cred.user_id,
                action="revoke",
                payload=archive_payload,
                timestamp=archive_ts,
                previous_hash=chain_tip,
                row_hash=archive_row_hash,
                key_id=secrets_mgr.get_current_signing_key_id(),
                credential_id=cred.credential_id,
            )
        )
        # Thread the hash forward: the activation record chains from here.
        chain_tip = archive_row_hash

    audit_timestamp = datetime.now(UTC)
    audit_action = _TRANSITION_AUDIT_ACTION[(previous_status, payload.status)]
    audit_payload_data = {
        "event": "status_transition",
        "version_id": str(version_id),
        "from_status": previous_status,
        "to_status": payload.status,
        "reason": payload.reason,
        "status": "SUCCESS",
    }

    audit_canonical = get_canonical_json(audit_payload_data)
    new_row_hash = compute_audit_row_hash(
        chain_tip, audit_canonical, audit_timestamp.isoformat()
    )

    session.add(
        MetadataAudit(
            version_id=version_id,
            actor=cred.user_id,
            action=audit_action,
            payload=audit_payload_data,
            timestamp=audit_timestamp,
            previous_hash=chain_tip,
            row_hash=new_row_hash,
            key_id=secrets_mgr.get_current_signing_key_id(),
            credential_id=cred.credential_id,
        )
    )

    # Atomic commit: status change(s) + audit record(s) together.
    # On PostgreSQL, the partial unique index uq_one_active_version_per_tenant
    # acts as a final backstop against concurrent activations — an IntegrityError
    # here means two callers raced; return 409 so the client can retry.
    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        raise HTTPException(
            status_code=409,
            detail=(
                "Activation conflict: another version became active concurrently. "
                "Retry the request after verifying the current active version."
            ),
        ) from exc

    return ProtocolMetadataVersion(
        version_id=str(version.version_id),
        status=version.status,
        created_at=version.created_at.isoformat(),
    )


@api_router.get("/metadata/active")
async def get_active_metadata(
    registry: Annotated[RegistrySchema, Depends(get_registry)],
    cred: Annotated[ResolvedCredential, Depends(require_admin_credential)],
) -> dict[str, str | None]:
    """Retrieve the ID of the actively loaded registry schema."""
    return {
        "version_id": str(registry.version) if hasattr(registry, "version") else None
    }


async def _fetch_rag_column_values(
    version_id: uuid.UUID,
    session: AsyncSession,
) -> dict[str, list[str]]:
    """Fetch all active RAG values for a version, grouped by column_id string."""
    stmt = (
        select(MetadataColumnValue.column_id, MetadataColumnValue.value)
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


@api_router.post(
    "/metadata/compile/{version_id}", response_model=MetadataCompileResponse
)
async def compile_metadata_version(
    version_id: uuid.UUID,
    request: Request,
    session: Annotated[AsyncSession, Depends(get_registry_admin_db_session)],
    cred: Annotated[ResolvedCredential, Depends(require_admin_credential)],
    wait_for_index: bool = False,
) -> MetadataCompileResponse:
    """Compile an active metadata version into a runtime Aegis Registry artifact."""
    from app.api.compiler import MixedTenantArtifactError
    from app.rag.builder import RagDivergenceError, build_from_artifact
    from app.steward.loader import RegistryLoader

    # Ownership check: verify this version belongs to the requesting tenant
    version_res = await session.execute(
        select(MetadataVersion).where(MetadataVersion.version_id == version_id)
    )
    version_obj = version_res.scalar_one_or_none()
    if not version_obj:
        raise HTTPException(status_code=404, detail="Version not found.")
    if version_obj.tenant_id != cred.tenant_id:
        raise HTTPException(
            status_code=403, detail="Version does not belong to your tenant."
        )

    # Refresh strategy-driven values before compiling
    async with request.app.state.steward_session_factory() as steward_session:
        async with request.app.state.runtime_session_factory() as runtime_session:
            refreshed = await _run_strategy_refresh(
                steward_session, runtime_session, version_id, cred.tenant_id
            )
    if refreshed:
        logger.info(
            "compile: refreshed strategy values for %d column(s) in version %s",
            refreshed,
            version_id,
        )

    try:
        artifact = await MetadataCompiler.compile_version(
            session=session,
            version_id=version_id,
            actor=cred.user_id,
        )
    except MixedTenantArtifactError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    # Hot-reload this tenant's schema slot only
    async with request.app.state.registry_runtime_session_factory() as rt_session:
        schema = await RegistryLoader.load_active_schema(rt_session, cred.tenant_id)
    request.app.state.registries[cred.tenant_id] = schema

    # Fetch column values for RAG builder
    async with request.app.state.registry_admin_session_factory() as val_session:
        column_values = await _fetch_rag_column_values(version_id, val_session)

    async def _rebuild_index() -> None:
        try:
            new_store = await build_from_artifact(
                artifact_blob=artifact.artifact_blob,
                version_id=str(version_id),
                tenant_id=cred.tenant_id,
                artifact_version=artifact.artifact_hash,
                column_values=column_values,
            )
            request.app.state.vector_stores[cred.tenant_id] = new_store
            request.app.state.compiler.set_vector_store(new_store, cred.tenant_id)
        except RagDivergenceError:
            logger.warning(
                "RAG divergence detected for version %s — "
                "index not updated; re-compile after fixing values.",
                version_id,
            )
        except Exception:
            logger.exception(
                "RAG index rebuild failed for version %s", version_id
            )

    if wait_for_index:
        await _rebuild_index()
    else:
        _ = asyncio.create_task(_rebuild_index())

    return MetadataCompileResponse(
        artifact_id=str(artifact.artifact_id),
        version_id=str(artifact.version_id),
        artifact_hash=artifact.artifact_hash,
        compiled_at=artifact.compiled_at.isoformat(),
    )


@api_router.get(
    "/metadata/columns/{column_id}/values",
    response_model=list[ProtocolColumnValue],
)
async def list_column_values(
    column_id: uuid.UUID,
    session: Annotated[AsyncSession, Depends(get_steward_db_session)],
    cred: Annotated[ResolvedCredential, Depends(require_admin_credential)],
) -> list[ProtocolColumnValue]:
    """List curated RAG values for a column."""
    stmt = (
        select(MetadataColumnValue)
        .join(
            MetadataColumn,
            MetadataColumnValue.column_id == MetadataColumn.column_id,
        )
        .join(MetadataVersion, MetadataColumn.version_id == MetadataVersion.version_id)
        .where(
            MetadataColumnValue.column_id == column_id,
            MetadataVersion.tenant_id == cred.tenant_id,
        )
        .order_by(MetadataColumnValue.value)
    )
    result = await session.execute(stmt)
    rows = result.scalars().all()
    return [
        ProtocolColumnValue(
            value_id=str(r.value_id),
            value=r.value,
            active=r.active,
            created_at=r.created_at.isoformat(),
        )
        for r in rows
    ]


@api_router.post(
    "/metadata/columns/{column_id}/values",
    response_model=ProtocolColumnValue,
    status_code=201,
)
async def create_column_value(
    column_id: uuid.UUID,
    payload: ColumnValueCreateRequest,
    session: Annotated[AsyncSession, Depends(get_steward_db_session)],
    cred: Annotated[ResolvedCredential, Depends(require_admin_credential)],
) -> ProtocolColumnValue:
    """Add a curated RAG value to a column."""
    col_res = await session.execute(
        select(MetadataColumn)
        .join(MetadataVersion, MetadataColumn.version_id == MetadataVersion.version_id)
        .where(
            MetadataColumn.column_id == column_id,
            MetadataVersion.tenant_id == cred.tenant_id,
        )
    )
    col = col_res.scalar_one_or_none()
    if col is None:
        raise HTTPException(status_code=404, detail="Column not found.")

    try:
        norm = normalize_rag_value(payload.value)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    if norm is None:
        raise HTTPException(
            status_code=422, detail="Value is empty after normalization."
        )

    val = MetadataColumnValue(
        column_id=column_id,
        version_id=col.version_id,
        value=payload.value.strip(),
    )
    session.add(val)
    try:
        await session.commit()
        await session.refresh(val)
    except Exception as exc:
        await session.rollback()
        raise HTTPException(
            status_code=409, detail="Value already exists for this column."
        ) from exc
    return ProtocolColumnValue(
        value_id=str(val.value_id),
        value=val.value,
        active=val.active,
        created_at=val.created_at.isoformat(),
    )


@api_router.post(
    "/metadata/columns/{column_id}/values/bulk",
    response_model=ColumnValueBulkImportResponse,
    status_code=201,
)
async def bulk_import_column_values(
    column_id: uuid.UUID,
    payload: ColumnValueBulkImportRequest,
    session: Annotated[AsyncSession, Depends(get_steward_db_session)],
    cred: Annotated[ResolvedCredential, Depends(require_admin_credential)],
) -> ColumnValueBulkImportResponse:
    """Bulk-import curated RAG values (e.g. from a CSV upload via client)."""
    col_res = await session.execute(
        select(MetadataColumn)
        .join(MetadataVersion, MetadataColumn.version_id == MetadataVersion.version_id)
        .where(
            MetadataColumn.column_id == column_id,
            MetadataVersion.tenant_id == cred.tenant_id,
        )
    )
    col = col_res.scalar_one_or_none()
    if col is None:
        raise HTTPException(status_code=404, detail="Column not found.")

    existing_res = await session.execute(
        select(MetadataColumnValue.value).where(
            MetadataColumnValue.column_id == column_id
        )
    )
    existing_raw: set[str] = set(existing_res.scalars().all())
    existing_normalized: set[str] = set()
    for ev in existing_raw:
        try:
            en = normalize_rag_value(ev)
            if en:
                existing_normalized.add(en.lower())
        except ValueError:
            pass

    imported = 0
    skipped_duplicate = 0
    skipped_invalid = 0
    seen_in_batch: set[str] = set()

    for raw_val in payload.values:
        try:
            norm = normalize_rag_value(raw_val)
        except ValueError:
            skipped_invalid += 1
            continue
        if norm is None:
            skipped_invalid += 1
            continue
        norm_lower = norm.lower()
        if norm_lower in existing_normalized or norm_lower in seen_in_batch:
            skipped_duplicate += 1
            continue
        seen_in_batch.add(norm_lower)
        session.add(
            MetadataColumnValue(
                column_id=column_id,
                version_id=col.version_id,
                value=raw_val.strip(),
            )
        )
        imported += 1

    await session.commit()
    return ColumnValueBulkImportResponse(
        imported=imported,
        skipped_duplicate=skipped_duplicate,
        skipped_invalid=skipped_invalid,
    )


@api_router.delete(
    "/metadata/columns/{column_id}/values/{value_id}",
    status_code=204,
)
async def deactivate_column_value(
    column_id: uuid.UUID,
    value_id: uuid.UUID,
    session: Annotated[AsyncSession, Depends(get_steward_db_session)],
    cred: Annotated[ResolvedCredential, Depends(require_admin_credential)],
) -> None:
    """Soft-delete (deactivate) a curated RAG value."""
    res = await session.execute(
        select(MetadataColumnValue)
        .join(
            MetadataColumn,
            MetadataColumnValue.column_id == MetadataColumn.column_id,
        )
        .join(MetadataVersion, MetadataColumn.version_id == MetadataVersion.version_id)
        .where(
            MetadataColumnValue.value_id == value_id,
            MetadataColumnValue.column_id == column_id,
            MetadataVersion.tenant_id == cred.tenant_id,
        )
    )
    val = res.scalar_one_or_none()
    if val is None:
        raise HTTPException(status_code=404, detail="Value not found.")
    val.active = False
    await session.commit()


def _pg_quote(name: str) -> str:
    """Double-quote a PostgreSQL identifier, escaping any embedded quotes."""
    return '"' + name.replace('"', '""') + '"'


def _build_sample_sql(
    table_real_name: str,
    col_real_name: str,
    strategy: str,
    order_by_col: str | None,
    order_direction: str,
    limit: int,
) -> str:
    """Build a safe sample SQL string. Identifiers come from the trusted registry."""
    t = _pg_quote(table_real_name)
    c = _pg_quote(col_real_name)
    where = f"WHERE {c} IS NOT NULL AND {c}::text <> ''"
    if strategy == "top_n_by" and order_by_col:
        o = _pg_quote(order_by_col)
        direction = "DESC" if order_direction == "desc" else "ASC"
        return f"SELECT {c} FROM {t} {where} ORDER BY {o} {direction} LIMIT {limit}"
    if strategy == "most_frequent":
        return (
            f"SELECT {c} FROM {t} {where} "
            f"GROUP BY {c} ORDER BY COUNT(*) DESC LIMIT {limit}"
        )
    # default: distinct
    return f"SELECT DISTINCT {c} FROM {t} {where} LIMIT {limit}"


async def _run_strategy_refresh(
    steward_session: AsyncSession,
    runtime_session: AsyncSession,
    version_id: uuid.UUID,
    tenant_id: str,
) -> int:
    """For every refresh_on_compile column in version, re-run its strategy and
    replace existing values. Returns number of columns refreshed."""
    from sqlalchemy import delete as sa_delete

    stmt = (
        select(MetadataColumn, MetadataTable.real_name.label("table_real_name"))
        .join(MetadataTable, MetadataColumn.table_id == MetadataTable.table_id)
        .join(MetadataVersion, MetadataColumn.version_id == MetadataVersion.version_id)
        .where(
            MetadataColumn.version_id == version_id,
            MetadataColumn.refresh_on_compile.is_(True),
            MetadataColumn.rag_enabled.is_(True),
            MetadataVersion.tenant_id == tenant_id,
        )
    )
    result = await steward_session.execute(stmt)
    rows = result.all()
    refreshed = 0
    for col, table_real_name in rows:
        strategy = col.rag_sample_strategy or "distinct"
        limit = col.rag_limit or 100
        order_by = col.rag_order_by_column
        order_dir = col.rag_order_direction or "desc"

        sql = _build_sample_sql(
            table_real_name, col.real_name, strategy, order_by, order_dir, limit
        )
        try:
            sample_res = await runtime_session.execute(text(sql))
            raw_values: list[str] = [str(r[0]) for r in sample_res.fetchall()]
        except Exception as exc:
            logger.warning(
                "refresh_on_compile: sample query failed for column %s — %s",
                col.column_id,
                exc,
            )
            continue

        # Replace all existing active values for this column
        await steward_session.execute(
            sa_delete(MetadataColumnValue).where(
                MetadataColumnValue.column_id == col.column_id
            )
        )
        seen: set[str] = set()
        for raw_val in raw_values:
            norm_key = raw_val.strip().lower()
            if not norm_key or norm_key in seen:
                continue
            seen.add(norm_key)
            steward_session.add(
                MetadataColumnValue(
                    column_id=col.column_id,
                    version_id=col.version_id,
                    value=raw_val.strip(),
                )
            )
        await steward_session.flush()
        refreshed += 1

    if refreshed:
        await steward_session.commit()
    return refreshed


@api_router.delete("/metadata/columns/{column_id}/values", status_code=204)
async def clear_column_values(
    column_id: uuid.UUID,
    session: Annotated[AsyncSession, Depends(get_steward_db_session)],
    cred: Annotated[ResolvedCredential, Depends(require_admin_credential)],
) -> None:
    """Hard-delete all curated RAG values for a column."""
    from sqlalchemy import delete as sa_delete

    col_res = await session.execute(
        select(MetadataColumn)
        .join(MetadataVersion, MetadataColumn.version_id == MetadataVersion.version_id)
        .where(
            MetadataColumn.column_id == column_id,
            MetadataVersion.tenant_id == cred.tenant_id,
        )
    )
    if col_res.scalar_one_or_none() is None:
        raise HTTPException(status_code=404, detail="Column not found.")
    await session.execute(
        sa_delete(MetadataColumnValue).where(
            MetadataColumnValue.column_id == column_id
        )
    )
    await session.commit()


@api_router.get(
    "/metadata/columns/{column_id}/sample",
    response_model=list[str],
)
async def sample_column_values(
    column_id: uuid.UUID,
    request: Request,
    session: Annotated[AsyncSession, Depends(get_steward_db_session)],
    cred: Annotated[ResolvedCredential, Depends(require_admin_credential)],
) -> list[str]:
    """Run the column's sampling strategy against the runtime DB and return results.

    Does NOT persist anything — use the bulk import endpoint to save values.
    """
    stmt = (
        select(MetadataColumn, MetadataTable.real_name.label("table_real_name"))
        .join(MetadataTable, MetadataColumn.table_id == MetadataTable.table_id)
        .join(MetadataVersion, MetadataColumn.version_id == MetadataVersion.version_id)
        .where(
            MetadataColumn.column_id == column_id,
            MetadataVersion.tenant_id == cred.tenant_id,
        )
    )
    res = await session.execute(stmt)
    row = res.one_or_none()
    if row is None:
        raise HTTPException(status_code=404, detail="Column not found.")
    col, table_real_name = row

    strategy = col.rag_sample_strategy or "distinct"
    limit = col.rag_limit or 100
    order_by = col.rag_order_by_column
    order_dir = col.rag_order_direction or "desc"

    if strategy == "top_n_by" and not order_by:
        raise HTTPException(
            status_code=422,
            detail="top_n_by strategy requires rag_order_by_column to be set.",
        )

    sql = _build_sample_sql(
        table_real_name, col.real_name, strategy, order_by, order_dir, limit
    )

    try:
        async with request.app.state.runtime_session_factory() as rt_session:
            sample_res = await rt_session.execute(text(sql))
            values = [str(r[0]) for r in sample_res.fetchall() if r[0] is not None]
    except Exception as exc:
        raise HTTPException(
            status_code=422,
            detail=f"Sample query failed: {exc}",
        ) from exc

    return values


def _map_col(c: MetadataColumn) -> ProtocolColumn:
    return ProtocolColumn(
        column_id=str(c.column_id),
        real_name=c.real_name,
        alias=c.alias,
        description=c.description,
        data_type=c.data_type,
        is_primary_key=c.is_primary_key,
        allowed_in_select=c.allowed_in_select,
        allowed_in_filter=c.allowed_in_filter,
        allowed_in_join=c.allowed_in_join,
        safety_classification=c.safety_classification,
        rag_enabled=c.rag_enabled,
        rag_cardinality_hint=cast(
            Literal["low", "medium", "high"] | None,
            c.rag_cardinality_hint,
        ),
        rag_limit=c.rag_limit,
        rag_sample_strategy=cast(
            Literal["distinct", "top_n_by", "most_frequent"] | None,
            c.rag_sample_strategy,
        ),
        rag_order_by_column=c.rag_order_by_column,
        rag_order_direction=cast(
            Literal["asc", "desc"] | None,
            c.rag_order_direction,
        ),
        refresh_on_compile=c.refresh_on_compile,
    )

def _map_table(t: MetadataTable) -> ProtocolTable:
    return ProtocolTable(
        table_id=str(t.table_id),
        real_name=t.real_name,
        alias=t.alias,
        description=t.description,
        active=t.active,
        columns=[_map_col(c) for c in t.columns]
    )


@api_router.get(
    "/metadata/versions/{version_id}/schema",
    response_model=ProtocolSchemaResponse,
)
async def get_metadata_schema(
    version_id: uuid.UUID,
    session: Annotated[AsyncSession, Depends(get_steward_db_session)],
    cred: Annotated[ResolvedCredential, Depends(require_admin_credential)],
) -> ProtocolSchemaResponse:
    stmt = (
        select(MetadataVersion)
        .where(MetadataVersion.version_id == version_id)
        .options(
            selectinload(MetadataVersion.tables).selectinload(MetadataTable.columns),
            selectinload(MetadataVersion.edges)
        )
    )
    res = await session.execute(stmt)
    version = res.scalar_one_or_none()
    if not version:
        raise HTTPException(status_code=404, detail="Version not found")
    if version.tenant_id != cred.tenant_id:
        raise HTTPException(
            status_code=403, detail="Version does not belong to your tenant."
        )

    tables_out = [_map_table(t) for t in version.tables]

    edges_out = []
    for e in version.edges:
        edges_out.append(ProtocolRelationship(
            relationship_id=str(e.relationship_id),
            source_table_id=str(e.source_table_id),
            source_column_id=str(e.source_column_id),
            target_table_id=str(e.target_table_id),
            target_column_id=str(e.target_column_id),
            relationship_type=e.relationship_type,
            cardinality=e.cardinality
        ))

    return ProtocolSchemaResponse(
        version_id=str(version.version_id),
        tables=tables_out,
        relationships=edges_out
    )


@api_router.put("/metadata/tables/{table_id}", response_model=ProtocolTable)
async def update_metadata_table(
    table_id: uuid.UUID,
    payload: TableUpdateRequest,
    session: Annotated[AsyncSession, Depends(get_steward_db_session)],
    cred: Annotated[ResolvedCredential, Depends(require_admin_credential)],
) -> ProtocolTable:
    stmt = (
        select(MetadataTable)
        .join(MetadataVersion, MetadataTable.version_id == MetadataVersion.version_id)
        .where(
            MetadataTable.table_id == table_id,
            MetadataVersion.tenant_id == cred.tenant_id,
        )
        .options(selectinload(MetadataTable.columns))
    )
    res = await session.execute(stmt)
    table = res.scalar_one_or_none()
    if not table:
         raise HTTPException(status_code=404, detail="Table not found")

    if payload.alias is not None:
        table.alias = payload.alias
    if payload.description is not None:
        table.description = payload.description
    if payload.active is not None:
        table.active = payload.active

    await session.commit()
    return _map_table(table)


@api_router.put("/metadata/columns/{column_id}", response_model=ProtocolColumn)
async def update_metadata_column(
    column_id: uuid.UUID,
    payload: ColumnUpdateRequest,
    session: Annotated[AsyncSession, Depends(get_steward_db_session)],
    cred: Annotated[ResolvedCredential, Depends(require_admin_credential)],
) -> ProtocolColumn:
    stmt = (
        select(MetadataColumn)
        .join(MetadataVersion, MetadataColumn.version_id == MetadataVersion.version_id)
        .where(
            MetadataColumn.column_id == column_id,
            MetadataVersion.tenant_id == cred.tenant_id,
        )
    )
    res = await session.execute(stmt)
    col = res.scalar_one_or_none()
    if not col:
        raise HTTPException(status_code=404, detail="Column not found")

    _UPDATABLE = (
        "alias", "description", "allowed_in_select", "allowed_in_filter",
        "allowed_in_join", "rag_enabled", "rag_cardinality_hint", "rag_limit",
        "rag_sample_strategy", "rag_order_by_column", "rag_order_direction",
        "refresh_on_compile",
    )
    for field in _UPDATABLE:
        val = getattr(payload, field)
        if val is not None:
            setattr(col, field, val)

    await session.commit()
    return _map_col(col)


@api_router.post("/metadata/versions", response_model=ProtocolMetadataVersion)
async def create_metadata_version(
    payload: VersionCreateRequest,
    session: Annotated[AsyncSession, Depends(get_steward_db_session)],
    cred: Annotated[ResolvedCredential, Depends(require_admin_credential)],
) -> ProtocolMetadataVersion:
    new_version_id = uuid.uuid4()
    new_version = MetadataVersion(
        version_id=new_version_id,
        tenant_id=cred.tenant_id,
        status="draft",
        created_by=cred.user_id,
        change_reason="Steward UI clone",
    )
    session.add(new_version)

    if payload.baseline_version_id:
        baseline_id = uuid.UUID(payload.baseline_version_id)
        stmt = (
            select(MetadataVersion)
            .where(MetadataVersion.version_id == baseline_id)
            .options(
                selectinload(MetadataVersion.tables).selectinload(
                    MetadataTable.columns
                ),
                selectinload(MetadataVersion.edges)
            )
        )
        res = await session.execute(stmt)
        baseline = res.scalar_one_or_none()
        if not baseline:
             raise HTTPException(status_code=404, detail="Baseline not found")
        if baseline.tenant_id != cred.tenant_id:
            raise HTTPException(
                status_code=403,
                detail="Baseline version does not belong to your tenant.",
            )

        table_id_map = {}
        col_id_map = {}

        # Deep clone
        for old_t in baseline.tables:
            new_t_id = uuid.uuid4()
            new_t = MetadataTable(
                table_id=new_t_id,
                version_id=new_version.version_id,
                real_name=old_t.real_name,
                alias=old_t.alias,
                description=old_t.description,
                tenant_id=old_t.tenant_id,
                active=old_t.active
            )
            session.add(new_t)
            table_id_map[old_t.table_id] = new_t_id

            for old_c in old_t.columns:
                new_c_id = uuid.uuid4()
                new_c = MetadataColumn(
                    column_id=new_c_id,
                    version_id=new_version.version_id,
                    table_id=new_t_id,
                    real_name=old_c.real_name,
                    alias=old_c.alias,
                    description=old_c.description,
                    data_type=old_c.data_type,
                    is_nullable=old_c.is_nullable,
                    is_primary_key=old_c.is_primary_key,
                    is_unique=old_c.is_unique,
                    is_sensitive=old_c.is_sensitive,
                    allowed_in_select=old_c.allowed_in_select,
                    allowed_in_filter=old_c.allowed_in_filter,
                    allowed_in_join=old_c.allowed_in_join,
                    safety_classification=old_c.safety_classification,
                    rag_enabled=old_c.rag_enabled,
                    rag_cardinality_hint=old_c.rag_cardinality_hint,
                    rag_limit=old_c.rag_limit,
                    rag_sample_strategy=old_c.rag_sample_strategy,
                    rag_order_by_column=old_c.rag_order_by_column,
                    rag_order_direction=old_c.rag_order_direction,
                    refresh_on_compile=old_c.refresh_on_compile,
                )
                session.add(new_c)
                col_id_map[old_c.column_id] = new_c_id

        # Flush tables and columns to the DB before inserting relationships so
        # the FK constraint (version_id, source/target_column_id) → metadata_columns
        # is satisfied when the relationship rows are written.
        await session.flush()

        for old_e in baseline.edges:
            src_col_id = col_id_map.get(old_e.source_column_id)
            tgt_col_id = col_id_map.get(old_e.target_column_id)
            if not src_col_id or not tgt_col_id:
                # Edge references a column that wasn't cloned — skip rather than
                # inserting an old column UUID under the new version_id (FK violation).
                continue
            new_e = MetadataRelationship(
                relationship_id=uuid.uuid4(),
                version_id=new_version.version_id,
                source_table_id=table_id_map[old_e.source_table_id],
                source_column_id=src_col_id,
                target_table_id=table_id_map[old_e.target_table_id],
                target_column_id=tgt_col_id,
                relationship_type=old_e.relationship_type,
                cardinality=old_e.cardinality,
                bidirectional=old_e.bidirectional,
                active=old_e.active
            )
            session.add(new_e)

    await session.commit()
    await session.refresh(new_version)
    return ProtocolMetadataVersion(
        version_id=str(new_version.version_id),
        status=new_version.status,
        created_at=new_version.created_at.isoformat()
    )


@api_router.post("/metadata/versions/{version_id}/obfuscate")
async def obfuscate_schema(
    version_id: uuid.UUID,
    session: Annotated[AsyncSession, Depends(get_steward_db_session)],
    cred: Annotated[ResolvedCredential, Depends(require_admin_credential)],
) -> dict[str, int | str]:
    stmt = (
        select(MetadataVersion)
        .where(MetadataVersion.version_id == version_id)
        .options(
            selectinload(MetadataVersion.tables).selectinload(MetadataTable.columns)
        )
    )
    res = await session.execute(stmt)
    version = res.scalar_one_or_none()
    if not version:
        raise HTTPException(status_code=404, detail="Version not found")
    if version.tenant_id != cred.tenant_id:
        raise HTTPException(
            status_code=403, detail="Version does not belong to your tenant."
        )

    t_counter = 1
    c_counter = 1

    for table in version.tables:
        table.alias = f"table{t_counter:04d}"
        t_counter += 1
        for col in table.columns:
            col.alias = f"col{c_counter:04d}"
            c_counter += 1

    await session.commit()
    return {
        "status": "success",
        "tables_obfuscated": t_counter - 1,
        "columns_obfuscated": c_counter - 1,
    }


# ---------------------------------------------------------------------------
# Credential management endpoints (admin scope required)
# ---------------------------------------------------------------------------


@api_router.post(
    "/auth/credentials",
    response_model=CredentialCreateResponse,
    status_code=201,
)
async def create_credential(
    payload: CredentialCreateRequest,
    session: Annotated[AsyncSession, Depends(get_registry_admin_db_session)],
    cred: Annotated[ResolvedCredential, Depends(require_admin_credential)],
) -> CredentialCreateResponse:
    """
    Create a new tenant API key.  The raw key is returned exactly once in the
    response body and is never stored — only its HMAC-SHA256 digest is persisted.
    """
    try:
        secret = get_secrets_manager().get_credential_hmac_secret()
    except VaultMissingSecretError as exc:
        logger.error("HMAC secret unavailable while creating credential: %s", exc)
        raise HTTPException(
            status_code=500, detail="Auth service unavailable."
        ) from exc

    if payload.tenant_id != cred.tenant_id:
        raise HTTPException(
            status_code=403,
            detail="Cannot create credentials for a different tenant.",
        )

    raw_key = generate_api_key()
    key_hash = _hash_api_key(raw_key, secret)

    new_cred = TenantCredential(
        credential_id=uuid.uuid4(),
        tenant_id=payload.tenant_id,
        user_id=payload.user_id,
        key_hash=key_hash,
        scope=payload.scope,
        description=payload.description,
        is_active=True,
    )
    session.add(new_cred)
    try:
        await session.commit()
        await session.refresh(new_cred)
    except Exception as exc:
        await session.rollback()
        raise HTTPException(
            status_code=409, detail="A credential with this key hash already exists."
        ) from exc

    return CredentialCreateResponse(
        credential_id=str(new_cred.credential_id),
        tenant_id=new_cred.tenant_id,
        user_id=new_cred.user_id,
        scope=new_cred.scope,
        description=new_cred.description,
        is_active=new_cred.is_active,
        created_at=new_cred.created_at.isoformat(),
        raw_key=raw_key,
    )


@api_router.get("/auth/credentials", response_model=list[CredentialListItem])
async def list_credentials(
    session: Annotated[AsyncSession, Depends(get_registry_admin_db_session)],
    cred: Annotated[ResolvedCredential, Depends(require_admin_credential)],
) -> list[CredentialListItem]:
    """List all tenant credentials (active and inactive)."""
    res = await session.execute(
        select(TenantCredential)
        .where(TenantCredential.tenant_id == cred.tenant_id)
        .order_by(TenantCredential.created_at.desc())
    )
    rows = res.scalars().all()
    return [
        CredentialListItem(
            credential_id=str(r.credential_id),
            tenant_id=r.tenant_id,
            user_id=r.user_id,
            scope=r.scope,
            description=r.description,
            is_active=r.is_active,
            created_at=r.created_at.isoformat(),
        )
        for r in rows
    ]


@api_router.delete("/auth/credentials/{credential_id}", status_code=204)
async def revoke_credential(
    credential_id: uuid.UUID,
    session: Annotated[AsyncSession, Depends(get_registry_admin_db_session)],
    cred: Annotated[ResolvedCredential, Depends(require_admin_credential)],
) -> None:
    """Soft-revoke a tenant API key by setting is_active=False."""
    res = await session.execute(
        select(TenantCredential).where(
            TenantCredential.credential_id == credential_id
        )
    )
    target = res.scalar_one_or_none()
    if target is None:
        raise HTTPException(status_code=404, detail="Credential not found.")
    if target.tenant_id != cred.tenant_id:
        raise HTTPException(
            status_code=403, detail="Credential does not belong to your tenant."
        )
    target.is_active = False
    await session.commit()
