"""Integration test for the WORM audit-chain retry loops.

Code-review finding #4 + reviewer's follow-up #1 (2026-04-07): the
original test built an inline retry helper and tested the helper, not
production code — the test could stay green while compiler.py and
router.py were still broken.

This replacement drives the REAL retry path by patching session.commit()
to raise an audit-chain IntegrityError on the first call and succeed
on the second call, then invoking MetadataCompiler.compile_version
through a fully-mocked session graph. We assert that:

  1. commit() was called twice (proves the retry happened)
  2. rollback() was called once (proves the retry cleaned up correctly)
  3. The function returned a non-None artifact (proves the second
     attempt succeeded)

This test cannot prove "the retry only catches audit-chain collisions
and not other IntegrityError causes" — that's what
tests/test_audit_append.py::test_audit_chain_collision_* does, and
together they constitute the regression suite for finding #4.
"""
import uuid
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.exc import IntegrityError

from app.api.compiler import MetadataCompiler
from app.audit.append import AUDIT_CHAIN_INDEX_NAME


def _make_audit_chain_integrity_error() -> IntegrityError:
    """Build an IntegrityError that is_audit_chain_collision classifies
    as an audit-chain collision."""
    orig = MagicMock()
    orig.constraint_name = AUDIT_CHAIN_INDEX_NAME
    return IntegrityError("INSERT ...", None, orig)


@pytest.mark.asyncio
async def test_compile_version_retries_on_audit_chain_collision() -> None:
    """MetadataCompiler.compile_version must retry when session.commit()
    raises an IntegrityError whose constraint is the audit chain's
    partial unique index. A second commit attempt that succeeds should
    produce a returned artifact."""
    # Build a minimal mock version graph. The real code uses selectinload
    # for the version's tables/columns/edges; a MagicMock with empty
    # collections satisfies it.
    version_id = uuid.uuid4()

    mock_version = MagicMock()
    mock_version.version_id = version_id
    mock_version.tenant_id = "test_tenant"
    mock_version.status = "active"  # triggers approved_by/at assignment
    mock_version.tables = []  # no tables → empty artifact payload
    mock_version.edges = []
    mock_version.registry_hash = None

    # session.execute is called for: (1) version SELECT, (2) chain tip
    # SELECT on each attempt, (3) the delete-artifact statement on each
    # attempt. Discriminate by the rendered table name in the statement
    # — SQLAlchemy emits the actual table name (e.g. metadata_versions),
    # NOT the ORM class name. Matching on "MetadataVersion" in str(stmt)
    # would never fire and the test would silently take the default
    # branch for every call.
    version_result = MagicMock()
    version_result.scalar_one_or_none.return_value = mock_version

    tip_result = MagicMock()
    tip_result.scalar_one_or_none.return_value = None  # genesis

    delete_result = MagicMock()

    async def fake_execute(stmt: Any) -> MagicMock:
        sql = str(stmt).lower()
        # Order matters: the audit chain tip read targets metadata_audit
        # only, while the version select targets metadata_versions only.
        # The artifact delete targets compiled_registry_artifacts.
        if "metadata_audit" in sql:
            return tip_result
        if "metadata_versions" in sql:
            return version_result
        return delete_result

    # session.commit raises on the first call, succeeds on the second.
    commit_calls = [0]

    async def fake_commit() -> None:
        commit_calls[0] += 1
        if commit_calls[0] == 1:
            raise _make_audit_chain_integrity_error()
        return None

    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(side_effect=fake_execute)
    mock_session.commit = AsyncMock(side_effect=fake_commit)
    mock_session.rollback = AsyncMock()
    mock_session.add = MagicMock()

    # Call the real production function.
    artifact = await MetadataCompiler.compile_version(
        session=mock_session,
        version_id=version_id,
        actor="test_actor",
    )

    # Assertions on real production behavior.
    assert commit_calls[0] == 2, (
        f"Expected compile_version to retry and call commit twice;"
        f" got {commit_calls[0]} calls. The retry loop is not wired up"
        f" to the audit-chain classifier."
    )
    assert mock_session.rollback.await_count >= 1, (
        "Expected rollback to be called after the first IntegrityError;"
        " the retry loop is not cleaning up the failed attempt."
    )
    assert artifact is not None


@pytest.mark.asyncio
async def test_compile_version_does_not_retry_on_unrelated_integrity_error() -> None:
    """MetadataCompiler.compile_version must NOT retry when the
    IntegrityError is for a constraint OTHER than the audit chain.
    Unrelated errors must propagate immediately so the caller sees the
    real cause."""
    version_id = uuid.uuid4()

    mock_version = MagicMock()
    mock_version.version_id = version_id
    mock_version.tenant_id = "test_tenant"
    mock_version.status = "active"
    mock_version.tables = []
    mock_version.edges = []

    version_result = MagicMock()
    version_result.scalar_one_or_none.return_value = mock_version
    tip_result = MagicMock()
    tip_result.scalar_one_or_none.return_value = None

    async def fake_execute(stmt: Any) -> MagicMock:
        # Match on the rendered table name (SQLAlchemy emits real table
        # names, not ORM class names — see comment in the previous test).
        sql = str(stmt).lower()
        if "metadata_audit" in sql:
            return tip_result
        if "metadata_versions" in sql:
            return version_result
        return MagicMock()

    # Raise a DIFFERENT IntegrityError — e.g. a fake FK violation.
    fk_orig = MagicMock()
    fk_orig.constraint_name = "metadata_columns_version_id_fkey"
    fk_orig.args = ("unrelated fk violation",)
    fk_exc = IntegrityError("INSERT ...", None, fk_orig)

    commit_calls = [0]

    async def fake_commit() -> None:
        commit_calls[0] += 1
        raise fk_exc  # every call raises

    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(side_effect=fake_execute)
    mock_session.commit = AsyncMock(side_effect=fake_commit)
    mock_session.rollback = AsyncMock()
    mock_session.add = MagicMock()

    # The unrelated IntegrityError must propagate — NOT be retried.
    with pytest.raises(IntegrityError) as exc_info:
        await MetadataCompiler.compile_version(
            session=mock_session,
            version_id=version_id,
            actor="test_actor",
        )
    assert exc_info.value is fk_exc
    assert commit_calls[0] == 1, (
        f"Expected compile_version to NOT retry non-audit IntegrityError;"
        f" got {commit_calls[0]} commit attempts. A genuine FK/UNIQUE"
        f" violation is being masked as audit-chain contention."
    )


async def _setup_metadata_schema_for_sqlite(conn: object) -> None:
    """Create the seven metadata tables that compile_version /
    update_version_status touch, using raw-SQL DDL that SQLite can
    actually compile.

    Why raw SQL: app/api/meta_models.py uses postgresql-specific JSONB
    columns and SQLAlchemy's SQLite type compiler refuses to render
    them, so Base.metadata.create_all() fails immediately. This helper
    issues SQLite-compatible CREATE TABLE statements with TEXT columns
    everywhere; the ORM's UUID and JSONB type processors fall back to
    string/JSON encoding when the storage type is permissive, so the
    round-trip via session.add() / session.execute(select(...)) works
    correctly. Verified empirically before this plan was written.

    The seven tables match the ones the compile_version selectinload
    chain and update_version_status SELECT touch:
      metadata_versions, metadata_tables, metadata_columns,
      metadata_column_values, metadata_relationships,
      compiled_registry_artifacts, metadata_audit
    """
    from sqlalchemy import text

    statements = [
        """CREATE TABLE metadata_versions (
            version_id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            registry_hash TEXT,
            status TEXT NOT NULL DEFAULT 'draft',
            created_by TEXT NOT NULL DEFAULT 'system',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            approved_by TEXT,
            approved_at TEXT,
            change_reason TEXT
        )""",
        """CREATE TABLE metadata_tables (
            table_id TEXT PRIMARY KEY,
            version_id TEXT NOT NULL,
            real_name TEXT NOT NULL,
            alias TEXT NOT NULL,
            description TEXT,
            tenant_id TEXT,
            active INTEGER NOT NULL DEFAULT 1,
            source_database TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE metadata_columns (
            column_id TEXT PRIMARY KEY,
            version_id TEXT NOT NULL,
            table_id TEXT NOT NULL,
            real_name TEXT NOT NULL,
            alias TEXT NOT NULL,
            description TEXT,
            data_type TEXT NOT NULL,
            is_nullable INTEGER NOT NULL DEFAULT 1,
            is_primary_key INTEGER NOT NULL DEFAULT 0,
            is_unique INTEGER NOT NULL DEFAULT 0,
            is_sensitive INTEGER NOT NULL DEFAULT 0,
            allowed_in_select INTEGER NOT NULL DEFAULT 0,
            allowed_in_filter INTEGER NOT NULL DEFAULT 0,
            allowed_in_join INTEGER NOT NULL DEFAULT 0,
            safety_classification TEXT,
            sample_values TEXT,
            sample_values_exhaustive INTEGER NOT NULL DEFAULT 0,
            rag_enabled INTEGER NOT NULL DEFAULT 0,
            rag_cardinality_hint TEXT,
            rag_limit INTEGER,
            rag_sample_strategy TEXT,
            rag_order_by_column TEXT,
            rag_order_direction TEXT,
            refresh_on_compile INTEGER NOT NULL DEFAULT 0
        )""",
        """CREATE TABLE metadata_column_values (
            value_id TEXT PRIMARY KEY,
            column_id TEXT NOT NULL,
            version_id TEXT NOT NULL,
            value TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE metadata_relationships (
            relationship_id TEXT PRIMARY KEY,
            version_id TEXT NOT NULL,
            source_table_id TEXT NOT NULL,
            source_column_id TEXT NOT NULL,
            target_table_id TEXT NOT NULL,
            target_column_id TEXT NOT NULL,
            relationship_type TEXT NOT NULL DEFAULT 'fk',
            cardinality TEXT NOT NULL DEFAULT '1:n',
            bidirectional INTEGER NOT NULL DEFAULT 1,
            active INTEGER NOT NULL DEFAULT 1
        )""",
        """CREATE TABLE compiled_registry_artifacts (
            artifact_id TEXT PRIMARY KEY,
            version_id TEXT NOT NULL UNIQUE,
            tenant_id TEXT NOT NULL,
            artifact_blob TEXT NOT NULL DEFAULT '{}',
            artifact_hash TEXT NOT NULL,
            compiled_at TEXT DEFAULT CURRENT_TIMESTAMP,
            compiler_version TEXT NOT NULL DEFAULT '1.0.0',
            signature TEXT,
            signature_algo TEXT NOT NULL DEFAULT 'hmac-sha256-v1',
            signature_key_id TEXT
        )""",
        """CREATE TABLE metadata_audit (
            audit_id TEXT PRIMARY KEY,
            version_id TEXT,
            actor TEXT NOT NULL,
            action TEXT NOT NULL,
            payload TEXT NOT NULL DEFAULT '{}',
            timestamp TEXT NOT NULL,
            previous_hash TEXT,
            row_hash TEXT NOT NULL UNIQUE,
            hash_algorithm TEXT NOT NULL DEFAULT 'sha256-v1',
            key_id TEXT,
            credential_id TEXT
        )""",
    ]
    for stmt in statements:
        await conn.execute(text(stmt))  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_compile_version_real_session_retries_after_rollback() -> None:
    """Real AsyncSession integration test: force one audit-chain
    IntegrityError on the first commit attempt and verify the second
    attempt succeeds without crashing on expired ORM attributes.

    Reviewer pass 3 finding #1: AsyncSession.rollback() expires loaded
    ORM attributes regardless of expire_on_commit. A plain read of
    version.version_id / version.tenant_id / version.status on attempt 2
    would otherwise raise MissingGreenlet from the implicit refresh
    (verified empirically against an in-memory aiosqlite session). The
    fix in compile_version caches those scalars before the loop and
    uses locals on every attempt — this test catches any regression
    where a future edit reintroduces the expired-attribute read.
    """
    import sqlite3

    from sqlalchemy import event
    from sqlalchemy.exc import IntegrityError as SAIntegrityError
    from sqlalchemy.ext.asyncio import (
        async_sessionmaker,
        create_async_engine,
    )

    from app.api.compiler import MetadataCompiler
    from app.api.meta_models import MetadataVersion

    # Isolated in-memory aiosqlite engine — separate from the
    # conftest-shared SQLite so this test does not interfere with
    # other suites that touch the same schema.
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    try:
        async with engine.begin() as conn:
            await _setup_metadata_schema_for_sqlite(conn)

        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session:
            version = MetadataVersion(
                tenant_id="test_tenant",
                status="active",
                created_by="test",
            )
            session.add(version)
            await session.commit()
            version_id = version.version_id

            # Inject one collision via a before_commit hook on the
            # underlying sync session. Events fire from sync code
            # inside SQLAlchemy's commit handling, so the hook can
            # raise a synchronous IntegrityError that propagates as
            # if the DB had rejected the commit.
            commit_attempts = [0]

            @event.listens_for(session.sync_session, "before_commit")
            def force_one_collision(_sess: object) -> None:
                commit_attempts[0] += 1
                if commit_attempts[0] == 1:
                    orig = sqlite3.IntegrityError(
                        "UNIQUE constraint failed: metadata_audit.previous_hash"
                    )
                    raise SAIntegrityError("INSERT", None, orig)

            # Call the REAL production function. If compile_version
            # reads any expired ORM attribute on attempt 2, this raises
            # MissingGreenlet from the implicit refresh.
            artifact = await MetadataCompiler.compile_version(
                session=session,
                version_id=version_id,
                actor="test_actor",
            )

            assert artifact is not None
            assert commit_attempts[0] == 2, (
                f"expected one rollback + one successful commit, got"
                f" {commit_attempts[0]} commit attempts"
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_update_version_status_real_session_retries_archival_branch() -> None:
    """Real AsyncSession integration test for update_version_status:
    force one audit-chain IntegrityError, verify the second attempt
    exercises the archival audit branch successfully.

    This test catches three reviewer pass 3 findings simultaneously:

      Finding #1: cached_existing_active_id must be a local — reading
      existing_active.version_id on attempt 2 would crash with
      MissingGreenlet because rollback expired the ORM object.

      Finding #2: the chain-tip select must run BEFORE any version /
      existing_active mutations — otherwise default autoflush would
      fire the partial unique index on the SELECT, raising an
      IntegrityError outside our try/except. (We can't directly
      simulate the autoflush race in a single-process test, but the
      reorder fixes it by construction; this test exercises the path
      with the reorder applied and proves it doesn't crash.)

      Finding #3: cached_version_created_at_iso must be a local — the
      success-path return reads version.created_at, which would also
      crash on attempt 2 after rollback expiration if it weren't
      cached.
    """
    import sqlite3
    import uuid as uuid_mod

    from sqlalchemy import event
    from sqlalchemy.exc import IntegrityError as SAIntegrityError
    from sqlalchemy.ext.asyncio import (
        async_sessionmaker,
        create_async_engine,
    )

    from app.api.auth import ResolvedCredential
    from app.api.meta_models import MetadataVersion

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    try:
        async with engine.begin() as conn:
            await _setup_metadata_schema_for_sqlite(conn)

        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session:
            old_version = MetadataVersion(
                tenant_id="test_tenant",
                status="active",
                created_by="test",
            )
            new_version = MetadataVersion(
                tenant_id="test_tenant",
                status="pending_review",
                created_by="test",
            )
            session.add(old_version)
            session.add(new_version)
            await session.commit()
            new_id = new_version.version_id

            commit_attempts = [0]

            @event.listens_for(session.sync_session, "before_commit")
            def force_one_collision(_sess: object) -> None:
                commit_attempts[0] += 1
                if commit_attempts[0] == 1:
                    orig = sqlite3.IntegrityError(
                        "UNIQUE constraint failed: metadata_audit.previous_hash"
                    )
                    raise SAIntegrityError("INSERT", None, orig)

            # Direct invocation of the route handler function. FastAPI's
            # Annotated[X, Depends(...)] is type-hint-only at the function
            # level — we just pass the dependency values directly.
            # The Pydantic request model lives in app.api.models — the
            # exact class name was verified via grep before writing this
            # test. If a future rename breaks the import, search with
            # `grep -n "class.*VersionStatus" app/api/models.py`.
            from app.api.models import VersionStatusUpdateRequest
            from app.api.router import update_version_status

            cred = ResolvedCredential(
                credential_id=str(uuid_mod.uuid4()),
                tenant_id="test_tenant",
                user_id="test_admin",
                scope="admin",
            )
            payload = VersionStatusUpdateRequest(
                status="active",
                reason="real-session activation regression test",
            )

            result = await update_version_status(
                version_id=new_id,
                payload=payload,
                session=session,
                cred=cred,
            )

            assert result is not None
            assert result.status == "active"
            assert commit_attempts[0] == 2, (
                f"expected one rollback + one successful commit, got"
                f" {commit_attempts[0]} commit attempts; the archival"
                f" branch on attempt 2 likely crashed on an expired"
                f" ORM read"
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_compile_version_refreshes_status_after_concurrent_transition() -> None:
    """compile_version must not trust a stale identity-mapped status.

    Validation review (2026-04-08): compile_metadata_version preloads a
    MetadataVersion in the same AsyncSession that compile_version later uses.
    Without populate_existing=True on compile_version's SELECT, SQLAlchemy
    returns the already-loaded ORM object without refreshing it, so a
    concurrent status change (e.g. active -> archived) is missed and the
    archived version still compiles.
    """
    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.api.meta_models import MetadataVersion

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    try:
        async with engine.begin() as conn:
            await _setup_metadata_schema_for_sqlite(conn)

        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session_a, factory() as session_b:
            version = MetadataVersion(
                tenant_id="test_tenant",
                status="active",
                created_by="test",
            )
            session_a.add(version)
            await session_a.commit()
            version_id = version.version_id

            # Preload into session_a's identity map.
            loaded = (
                await session_a.execute(
                    select(MetadataVersion).where(
                        MetadataVersion.version_id == version_id
                    )
                )
            ).scalar_one()
            assert loaded.status == "active"

            # Concurrent admin archives the same version in another session.
            archived = (
                await session_b.execute(
                    select(MetadataVersion).where(
                        MetadataVersion.version_id == version_id
                    )
                )
            ).scalar_one()
            archived.status = "archived"
            await session_b.commit()

            with pytest.raises(ValueError, match="pending_review"):
                await MetadataCompiler.compile_version(
                    session=session_a,
                    version_id=version_id,
                    actor="test_actor",
                )
    finally:
        await engine.dispose()
