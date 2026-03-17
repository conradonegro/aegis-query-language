import asyncio
import logging
import uuid
from collections import defaultdict
from datetime import UTC, datetime
from typing import Annotated, Any, Literal, cast

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from sqlalchemy import select
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
    QueryRequest,
    TableUpdateRequest,
    VersionCreateRequest,
    VersionStatusUpdateRequest,
)
from app.audit import QueryAuditEvent
from app.audit.chaining import compute_audit_row_hash, get_canonical_json
from app.compiler.engine import CompilerEngine
from app.compiler.models import ChatHistoryItem, PromptHints, UserIntent
from app.execution import ExecutionContext
from app.execution.interfaces import ExecutionLayer
from app.rag.normalizer import normalize as normalize_rag_value
from app.steward import RegistrySchema
from app.vault import VaultMissingSecretError, get_secrets_manager

logger = logging.getLogger(__name__)

# Per-session asyncio locks prevent concurrent requests on the same session from
# reading the same last_seq value and colliding on uq_session_sequence.
# A plain defaultdict is safe here: asyncio is single-threaded so dict access is
# non-preemptive, and asyncio.Lock() requires no running event loop since Python 3.10.
_session_locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

api_router = APIRouter()


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

def get_registry(request: Request) -> RegistrySchema:
    return cast(RegistrySchema, request.app.state.registry)


@api_router.post("/query/generate", response_model=QueryGenerateResponse)
async def generate_query(
    payload: QueryRequest,
    compiler: Annotated[CompilerEngine, Depends(get_compiler)],
    registry: Annotated[RegistrySchema, Depends(get_registry)],
    session: Annotated[AsyncSession, Depends(get_runtime_db_session)],
    cred: Annotated[ResolvedCredential, Depends(require_query_credential)],
) -> QueryGenerateResponse:
    """
    Compiles natural language into an ExecutableQuery, strictly omitting
    physical DB execution.
    """
    intent = UserIntent(
        natural_language_query=payload.intent,
        source_database=payload.source_database,
    )
    hints = PromptHints(column_hints=payload.schema_hints)

    session_id, chat_history = await _resolve_session(
        payload.session_id, session, tenant_id=cred.tenant_id
    )

    executable = await compiler.compile(
        schema=registry,
        intent=intent,
        hints=hints,
        explain=payload.explain,
        chat_history=chat_history,
        provider_id=payload.provider_id,
        session_id=str(session_id),
        tenant_id=cred.tenant_id,
    )

    async with _session_locks[str(session_id)]:
        seq_res = await session.execute(
            select(ChatMessage.sequence_number)
            .where(ChatMessage.session_id == session_id)
            .order_by(ChatMessage.sequence_number.desc())
            .limit(1)
        )
        last_seq = seq_res.scalar_one_or_none() or 0

        user_msg = ChatMessage(
            message_id=uuid.uuid4(),
            session_id=session_id,
            sequence_number=last_seq + 1,
            role="user",
            content=intent.natural_language_query,
            provider_id=payload.provider_id
        )
        _llm_expl = (
            executable.explainability.get("llm", {})
            if executable.explainability
            else {}
        )
        assistant_msg = ChatMessage(
            message_id=uuid.uuid4(),
            session_id=session_id,
            sequence_number=last_seq + 2,
            role="assistant",
            content=(
                executable.abstract_sql
                if executable.abstract_sql is not None
                else executable.sql
            ),
            provider_id=(
                _llm_expl.get("provider")
                if executable.explainability
                else payload.provider_id
            ),
            prompt_tokens=_llm_expl.get("prompt_tokens"),
            completion_tokens=_llm_expl.get("completion_tokens"),
        )
        session.add_all([user_msg, assistant_msg])
        await session.commit()

    explain_ctx = (
        ExplainabilityContext.model_validate(executable.explainability)
        if executable.explainability is not None
        else None
    )
    return QueryGenerateResponse(
        query_id=executable.query_id or "",
        session_id=str(session_id),
        sql=executable.sql,
        parameters=executable.parameters,
        latency_ms=executable.compilation_latency_ms or 0.0,
        source_database_used=executable.source_database_used,
        explainability=explain_ctx,
    )


@api_router.post("/query/execute", response_model=QueryExecuteResponse)
async def execute_query(
    payload: QueryRequest,
    background_tasks: BackgroundTasks,
    compiler: Annotated[CompilerEngine, Depends(get_compiler)],
    executor: Annotated[ExecutionLayer, Depends(get_executor)],
    auditor: Annotated[Any, Depends(get_auditor)],
    registry: Annotated[RegistrySchema, Depends(get_registry)],
    session: Annotated[AsyncSession, Depends(get_runtime_db_session)],
    cred: Annotated[ResolvedCredential, Depends(require_query_credential)],
) -> QueryExecuteResponse:
    """
    Compiles and executes the query against the physical database.
    Dispatches asynchronous audit sink logging.
    """
    intent = UserIntent(
        natural_language_query=payload.intent,
        source_database=payload.source_database,
    )
    hints = PromptHints(column_hints=payload.schema_hints)

    session_id, chat_history = await _resolve_session(
        payload.session_id, session, tenant_id=cred.tenant_id
    )

    # Compile
    executable = await compiler.compile(
        schema=registry,
        intent=intent,
        hints=hints,
        explain=payload.explain,
        chat_history=chat_history,
        provider_id=payload.provider_id,
        session_id=str(session_id),
        tenant_id=cred.tenant_id,
    )

    async with _session_locks[str(session_id)]:
        seq_res = await session.execute(
            select(ChatMessage.sequence_number)
            .where(ChatMessage.session_id == session_id)
            .order_by(ChatMessage.sequence_number.desc())
            .limit(1)
        )
        last_seq = seq_res.scalar_one_or_none() or 0

        user_msg = ChatMessage(
            message_id=uuid.uuid4(),
            session_id=session_id,
            sequence_number=last_seq + 1,
            role="user",
            content=intent.natural_language_query,
            provider_id=payload.provider_id
        )
        _exec_llm_expl = (
            executable.explainability.get("llm", {})
            if executable.explainability
            else {}
        )
        assistant_msg = ChatMessage(
            message_id=uuid.uuid4(),
            session_id=session_id,
            sequence_number=last_seq + 2,
            role="assistant",
            # Store abstract_sql (obfuscated aliases) rather than the physical SQL
            # so the LLM cannot learn physical column/table names from its own
            # prior responses. abstract_sql is always populated by the compiler
            # engine regardless of explain flag.
            content=(
                executable.abstract_sql
                if executable.abstract_sql is not None
                else executable.sql
            ),
            provider_id=(
                _exec_llm_expl.get("provider")
                if executable.explainability
                else payload.provider_id
            ),
            prompt_tokens=_exec_llm_expl.get("prompt_tokens"),
            completion_tokens=_exec_llm_expl.get("completion_tokens"),
        )
        session.add_all([user_msg, assistant_msg])
        await session.commit()

    # Execute
    context = ExecutionContext(
        tenant_id=cred.tenant_id,
        user_id=cred.user_id,
    )
    result = await executor.execute(executable, context=context)

    # Background audit log mapping
    event = QueryAuditEvent(
        query_id=executable.query_id or str(uuid.uuid4()),
        tenant_id=context.tenant_id,
        user_id=context.user_id,
        credential_id=cred.credential_id,
        natural_language_query=payload.intent,
        abstract_query=executable.sql,
        physical_query=executable.sql,
        registry_version=executable.registry_version,
        safety_engine_version=executable.safety_engine_version,
        abstract_query_hash=executable.abstract_query_hash,
        latency_ms=executable.compilation_latency_ms or 0.0,
        prompt_tokens=0,
        completion_tokens=0,
        status="SUCCESS",
        error_message=None,
        row_limit_applied=executable.row_limit_applied,
    )

    # Add audit dispatch to non-blocking tasks list
    background_tasks.add_task(auditor.record, event)

    exec_explain_ctx = (
        ExplainabilityContext.model_validate(executable.explainability)
        if executable.explainability is not None
        else None
    )
    return QueryExecuteResponse(
        query_id=executable.query_id or "",
        session_id=str(session_id),
        sql=executable.sql,
        results=result.rows,
        row_count=len(result.rows),
        execution_latency_ms=0.0,
        source_database_used=executable.source_database_used,
        explainability=exec_explain_ctx,
    )


@api_router.get("/metadata/versions", response_model=list[ProtocolMetadataVersion])
async def list_metadata_versions(
    session: Annotated[AsyncSession, Depends(get_registry_runtime_db_session)],
    cred: Annotated[ResolvedCredential, Depends(require_admin_credential)],
) -> list[ProtocolMetadataVersion]:
    """Retrieve all metadata schema versions."""
    res = await session.execute(
        select(MetadataVersion).order_by(MetadataVersion.created_at.desc())
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

    # Apply status change
    version.status = payload.status
    if payload.reason:
        version.change_reason = payload.reason

    # Approval timestamps are populated when a version becomes active
    if payload.status == "active":
        version.approved_by = cred.user_id
        version.approved_at = datetime.now(UTC)

    # Build the WORM audit chain record — must happen in the same transaction
    last_audit_res = await session.execute(
        select(MetadataAudit)
        .order_by(MetadataAudit.timestamp.desc(), MetadataAudit.audit_id.desc())
        .limit(1)
    )
    last_row = last_audit_res.scalar_one_or_none()
    previous_hash = last_row.row_hash if last_row else ""

    audit_timestamp = datetime.now(UTC)
    audit_action = _TRANSITION_AUDIT_ACTION[(previous_status, payload.status)]
    audit_payload = {
        "event": "status_transition",
        "version_id": str(version_id),
        "from_status": previous_status,
        "to_status": payload.status,
        "reason": payload.reason,
        "status": "SUCCESS",
    }

    audit_canonical = get_canonical_json(audit_payload)
    new_row_hash = compute_audit_row_hash(
        previous_hash, audit_canonical, audit_timestamp.isoformat()
    )

    secrets_mgr = get_secrets_manager()
    audit_event = MetadataAudit(
        version_id=version_id,
        actor=cred.user_id,
        action=audit_action,
        payload=audit_payload,
        timestamp=audit_timestamp,
        previous_hash=previous_hash,
        row_hash=new_row_hash,
        key_id=secrets_mgr.get_current_signing_key_id(),
        credential_id=cred.credential_id,
    )
    session.add(audit_event)

    # Single atomic commit — status change and audit record together.
    # If audit chain construction raises, the status change is also rolled back.
    await session.commit()

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
    from app.rag.builder import RagDivergenceError, build_from_artifact
    from app.steward.loader import RegistryLoader

    artifact = await MetadataCompiler.compile_version(
        session=session,
        version_id=version_id,
        actor=cred.user_id,
    )

    # Hot-reload schema
    async with request.app.state.registry_runtime_session_factory() as rt_session:
        schema = await RegistryLoader.load_active_schema(rt_session)
    request.app.state.registry = schema

    # Fetch column values for RAG builder
    async with request.app.state.registry_admin_session_factory() as val_session:
        column_values = await _fetch_rag_column_values(version_id, val_session)

    async def _rebuild_index() -> None:
        try:
            new_store = await build_from_artifact(
                artifact_blob=artifact.artifact_blob,
                version_id=str(version_id),
                tenant_id="default_tenant",
                artifact_version=artifact.artifact_hash,
                column_values=column_values,
            )
            request.app.state.vector_store = new_store
            request.app.state.compiler.set_vector_store(new_store)
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
        .where(MetadataColumnValue.column_id == column_id)
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
        select(MetadataColumn).where(MetadataColumn.column_id == column_id)
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
        select(MetadataColumn).where(MetadataColumn.column_id == column_id)
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
        select(MetadataColumnValue).where(
            MetadataColumnValue.value_id == value_id,
            MetadataColumnValue.column_id == column_id,
        )
    )
    val = res.scalar_one_or_none()
    if val is None:
        raise HTTPException(status_code=404, detail="Value not found.")
    val.active = False
    await session.commit()


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
        .where(MetadataTable.table_id == table_id)
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
    stmt = select(MetadataColumn).where(MetadataColumn.column_id == column_id)
    res = await session.execute(stmt)
    col = res.scalar_one_or_none()
    if not col:
         raise HTTPException(status_code=404, detail="Column not found")

    if payload.alias is not None:
        col.alias = payload.alias
    if payload.description is not None:
        col.description = payload.description
    if payload.allowed_in_select is not None:
        col.allowed_in_select = payload.allowed_in_select
    if payload.allowed_in_filter is not None:
        col.allowed_in_filter = payload.allowed_in_filter
    if payload.allowed_in_join is not None:
        col.allowed_in_join = payload.allowed_in_join
    if payload.rag_enabled is not None:
        col.rag_enabled = payload.rag_enabled
    if payload.rag_cardinality_hint is not None:
        col.rag_cardinality_hint = payload.rag_cardinality_hint
    if payload.rag_limit is not None:
        col.rag_limit = payload.rag_limit

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
                    safety_classification=old_c.safety_classification
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
        select(TenantCredential).order_by(TenantCredential.created_at.desc())
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
    target.is_active = False
    await session.commit()
