import asyncio
import uuid
from collections import defaultdict
from collections.abc import AsyncGenerator
from datetime import datetime, timezone
from typing import Any, Literal, cast

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.requests import Request

# Per-session asyncio locks prevent concurrent requests on the same session from
# reading the same last_seq value and colliding on uq_session_sequence.
# A plain defaultdict is safe here: asyncio is single-threaded so dict access is
# non-preemptive, and asyncio.Lock() requires no running event loop since Python 3.10.
_session_locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

from app.audit.chaining import compute_audit_row_hash, get_canonical_json
from app.vault import get_secrets_manager

from app.api.compiler import MetadataCompiler
from app.api.meta_models import (
    MetadataVersion,
    MetadataTable,
    MetadataColumn,
    MetadataRelationship,
    MetadataAudit,
    ChatSession,
    ChatMessage,
)
from app.api.models import (
    ExplainabilityContext,
    MetadataCompileResponse,
    ProtocolMetadataVersion,
    ProtocolSchemaResponse,
    ProtocolTable,
    ProtocolColumn,
    ProtocolRelationship,
    TableUpdateRequest,
    ColumnUpdateRequest,
    VersionCreateRequest,
    VersionStatusUpdateRequest,
    QueryExecuteResponse,
    QueryGenerateResponse,
    QueryRequest,
)
from app.audit import QueryAuditEvent
from app.compiler.engine import CompilerEngine
from app.compiler.models import ChatHistoryItem, PromptHints, UserIntent
from app.execution import ExecutionContext
from app.execution.interfaces import ExecutionLayer
from app.steward import RegistrySchema

api_router = APIRouter()


async def _resolve_session(
    payload_session_id: str | None,
    session: AsyncSession,
) -> tuple[uuid.UUID, list[ChatHistoryItem]]:
    """
    Resolve or create a chat session.

    If payload_session_id is a valid UUID referencing an existing ChatSession,
    its message history is loaded and returned. Otherwise a new session row is
    created and committed so the PK exists before any message writes.
    """
    session_id: uuid.UUID | None = None
    chat_history: list[ChatHistoryItem] = []

    if payload_session_id:
        try:
            session_uuid = uuid.UUID(payload_session_id)
            res = await session.execute(
                select(ChatSession).where(ChatSession.session_id == session_uuid)
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
                        role=cast(Literal["user", "assistant", "system"], msg.role),
                        content=msg.content,
                    ))
        except ValueError:
            pass  # Ignore invalid UUID format and fall back to none

    if not session_id:
        session_id = uuid.uuid4()
        new_session = ChatSession(session_id=session_id)
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

async def get_registry_runtime_db_session(request: Request) -> AsyncGenerator[AsyncSession, None]:
    async with request.app.state.registry_runtime_session_factory() as session:
        yield session

async def get_registry_admin_db_session(request: Request) -> AsyncGenerator[AsyncSession, None]:
    async with request.app.state.registry_admin_session_factory() as session:
        yield session

async def get_runtime_db_session(request: Request) -> AsyncGenerator[AsyncSession, None]:
    async with request.app.state.runtime_session_factory() as session:
        yield session

async def get_steward_db_session(request: Request) -> AsyncGenerator[AsyncSession, None]:
    async with request.app.state.steward_session_factory() as session:
        yield session


@api_router.post("/query/generate", response_model=QueryGenerateResponse)
async def generate_query(
    payload: QueryRequest,
    compiler: CompilerEngine = Depends(get_compiler),
    registry: RegistrySchema = Depends(get_registry),
    session: AsyncSession = Depends(get_runtime_db_session)
) -> QueryGenerateResponse:
    """
    Compiles natural language into an ExecutableQuery, strictly omitting physical DB execution.
    """
    intent = UserIntent(natural_language_query=payload.intent)
    hints = PromptHints(column_hints=payload.schema_hints)

    session_id, chat_history = await _resolve_session(payload.session_id, session)

    executable = await compiler.compile(
        schema=registry,
        intent=intent,
        hints=hints,
        explain=payload.explain,
        chat_history=chat_history,
        provider_id=payload.provider_id,
        session_id=str(session_id),
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
        assistant_msg = ChatMessage(
            message_id=uuid.uuid4(),
            session_id=session_id,
            sequence_number=last_seq + 2,
            role="assistant",
            content=executable.abstract_sql if executable.abstract_sql is not None else executable.sql,
            provider_id=executable.explainability.get("llm", {}).get("provider") if executable.explainability else payload.provider_id,
            prompt_tokens=executable.explainability.get("llm", {}).get("prompt_tokens") if executable.explainability else None,
            completion_tokens=executable.explainability.get("llm", {}).get("completion_tokens") if executable.explainability else None
        )
        session.add_all([user_msg, assistant_msg])
        await session.commit()

    return QueryGenerateResponse(
        query_id=executable.query_id or "",
        session_id=str(session_id),
        sql=executable.sql,
        parameters=executable.parameters,
        latency_ms=executable.compilation_latency_ms or 0.0,
        explainability=cast(ExplainabilityContext | None, executable.explainability),
    )


@api_router.post("/query/execute", response_model=QueryExecuteResponse)
async def execute_query(
    payload: QueryRequest,
    background_tasks: BackgroundTasks,
    compiler: CompilerEngine = Depends(get_compiler),
    executor: ExecutionLayer = Depends(get_executor),
    auditor: Any = Depends(get_auditor),
    registry: RegistrySchema = Depends(get_registry),
    session: AsyncSession = Depends(get_runtime_db_session)
) -> QueryExecuteResponse:
    """
    Compiles and executes the query against the physical database.
    Dispatches asynchronous audit sink logging.
    """
    intent = UserIntent(natural_language_query=payload.intent)
    hints = PromptHints(column_hints=payload.schema_hints)

    session_id, chat_history = await _resolve_session(payload.session_id, session)

    # Compile
    executable = await compiler.compile(
        schema=registry,
        intent=intent,
        hints=hints,
        explain=payload.explain,
        chat_history=chat_history,
        provider_id=payload.provider_id,
        session_id=str(session_id)
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
        assistant_msg = ChatMessage(
            message_id=uuid.uuid4(),
            session_id=session_id,
            sequence_number=last_seq + 2,
            role="assistant",
            # Store abstract_sql (obfuscated aliases) rather than the physical SQL so the LLM
            # cannot learn physical column/table names from its own prior responses.
            # abstract_sql is always populated by the compiler engine regardless of explain flag.
            content=executable.abstract_sql if executable.abstract_sql is not None else executable.sql,
            provider_id=executable.explainability.get("llm", {}).get("provider") if executable.explainability else payload.provider_id,
            prompt_tokens=executable.explainability.get("llm", {}).get("prompt_tokens") if executable.explainability else None,
            completion_tokens=executable.explainability.get("llm", {}).get("completion_tokens") if executable.explainability else None
        )
        session.add_all([user_msg, assistant_msg])
        await session.commit()

    # Execute
    context = ExecutionContext(
            tenant_id="default_tenant",
            user_id="api_user",
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
        session_id=str(session_id),
        sql=executable.sql,
        results=result.rows,
        row_count=len(result.rows),
        execution_latency_ms=0.0,
        explainability=cast(ExplainabilityContext | None, executable.explainability),
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


@api_router.patch("/metadata/versions/{version_id}/status", response_model=ProtocolMetadataVersion)
async def update_version_status(
    version_id: uuid.UUID,
    payload: VersionStatusUpdateRequest,
    session: AsyncSession = Depends(get_registry_admin_db_session),
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
                f"Transition from '{version.status}' to '{payload.status}' is not permitted. "
                f"Allowed targets: {sorted(allowed) if allowed else 'none — this is a terminal state'}."
            ),
        )

    previous_status = version.status

    # Apply status change
    version.status = payload.status
    if payload.reason:
        version.change_reason = payload.reason

    # Approval timestamps are populated when a version becomes active
    if payload.status == "active":
        version.approved_by = "admin_api"
        version.approved_at = datetime.now(timezone.utc)

    # Build the WORM audit chain record — must happen in the same transaction
    last_audit_res = await session.execute(
        select(MetadataAudit)
        .order_by(MetadataAudit.timestamp.desc(), MetadataAudit.audit_id.desc())
        .limit(1)
    )
    last_row = last_audit_res.scalar_one_or_none()
    previous_hash = last_row.row_hash if last_row else ""

    audit_timestamp = datetime.now(timezone.utc)
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
    new_row_hash = compute_audit_row_hash(previous_hash, audit_canonical, audit_timestamp.isoformat())

    secrets_mgr = get_secrets_manager()
    audit_event = MetadataAudit(
        version_id=version_id,
        actor="admin_api",
        action=audit_action,
        payload=audit_payload,
        timestamp=audit_timestamp,
        previous_hash=previous_hash,
        row_hash=new_row_hash,
        key_id=secrets_mgr.get_current_signing_key_id(),
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
    registry: RegistrySchema = Depends(get_registry)
) -> dict[str, str | None]:
    """Retrieve the ID of the actively loaded registry schema."""
    return {"version_id": str(registry.version) if hasattr(registry, "version") else None}


@api_router.post("/metadata/compile/{version_id}", response_model=MetadataCompileResponse)
async def compile_metadata_version(
    version_id: uuid.UUID,
    request: Request,
    session: AsyncSession = Depends(get_registry_admin_db_session)
) -> MetadataCompileResponse:
    """Compile an active metadata version into a runtime Aegis Registry artifact."""
    artifact = await MetadataCompiler.compile_version(
        session=session,
        version_id=version_id,
        actor="admin_api"
    )
    
    # Dynamically hot-reload the RegistrySchema into the active FastAPI middleware!
    from app.steward.loader import RegistryLoader
    from app.rag.models import CategoricalValue
    from app.rag.store import InMemoryVectorStore

    async with request.app.state.registry_runtime_session_factory() as rt_session:
        schema = await RegistryLoader.load_active_schema(rt_session)
        request.app.state.registry = schema

        # Dynamically re-warm the RAG Vector Store with the new Obfuscated Schema Baseline
        vector_store = InMemoryVectorStore()
        for table in (schema.tables if schema is not None else []):
            if table.description:
                vector_store.index_value(CategoricalValue(value=table.description, abstract_column=f"{table.alias}.{table.alias}", tenant_id="default_tenant"))
            for col in table.columns:
                if col.description:
                    vector_store.index_value(CategoricalValue(value=col.description, abstract_column=f"{table.alias}.{col.alias}", tenant_id="default_tenant"))
                    
        request.app.state.vector_store = vector_store
        request.app.state.compiler.set_vector_store(vector_store)
    
    return MetadataCompileResponse(
        artifact_id=str(artifact.artifact_id),
        version_id=str(artifact.version_id),
        artifact_hash=artifact.artifact_hash,
        compiled_at=artifact.compiled_at.isoformat()
    )


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
        safety_classification=c.safety_classification
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


@api_router.get("/metadata/versions/{version_id}/schema", response_model=ProtocolSchemaResponse)
async def get_metadata_schema(
    version_id: uuid.UUID,
    session: AsyncSession = Depends(get_steward_db_session)
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
    session: AsyncSession = Depends(get_steward_db_session)
) -> ProtocolTable:
    stmt = select(MetadataTable).where(MetadataTable.table_id == table_id).options(selectinload(MetadataTable.columns))
    res = await session.execute(stmt)
    table = res.scalar_one_or_none()
    if not table:
         raise HTTPException(status_code=404, detail="Table not found")
         
    if payload.alias is not None: table.alias = payload.alias
    if payload.description is not None: table.description = payload.description
    if payload.active is not None: table.active = payload.active
    
    await session.commit()
    return _map_table(table)


@api_router.put("/metadata/columns/{column_id}", response_model=ProtocolColumn)
async def update_metadata_column(
    column_id: uuid.UUID,
    payload: ColumnUpdateRequest,
    session: AsyncSession = Depends(get_steward_db_session)
) -> ProtocolColumn:
    stmt = select(MetadataColumn).where(MetadataColumn.column_id == column_id)
    res = await session.execute(stmt)
    col = res.scalar_one_or_none()
    if not col:
         raise HTTPException(status_code=404, detail="Column not found")
         
    if payload.alias is not None: col.alias = payload.alias
    if payload.description is not None: col.description = payload.description
    if payload.allowed_in_select is not None: col.allowed_in_select = payload.allowed_in_select
    if payload.allowed_in_filter is not None: col.allowed_in_filter = payload.allowed_in_filter
    if payload.allowed_in_join is not None: col.allowed_in_join = payload.allowed_in_join
    
    await session.commit()
    return _map_col(col)


@api_router.post("/metadata/versions", response_model=ProtocolMetadataVersion)
async def create_metadata_version(
    payload: VersionCreateRequest,
    session: AsyncSession = Depends(get_steward_db_session)
) -> ProtocolMetadataVersion:
    new_version_id = uuid.uuid4()
    new_version = MetadataVersion(version_id=new_version_id, status="draft", change_reason="Steward UI clone")
    session.add(new_version)
    
    if payload.baseline_version_id:
        baseline_id = uuid.UUID(payload.baseline_version_id)
        stmt = select(MetadataVersion).where(MetadataVersion.version_id == baseline_id).options(
            selectinload(MetadataVersion.tables).selectinload(MetadataTable.columns),
            selectinload(MetadataVersion.edges)
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
    session: AsyncSession = Depends(get_steward_db_session)
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
    return {"status": "success", "tables_obfuscated": t_counter - 1, "columns_obfuscated": c_counter - 1}
