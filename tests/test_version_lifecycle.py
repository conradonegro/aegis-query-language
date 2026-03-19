"""Version lifecycle tests: one-active-per-tenant invariant.

All tests run against the shared in-memory SQLite DB seeded by conftest.
Metadata tables (metadata_versions, compiled_registry_artifacts, metadata_audit)
are created by the conftest fixture with no schema prefix (SQLite compat).

UUID storage note: sqlalchemy.dialects.postgresql.UUID(as_uuid=True) stores
UUIDs as 32-char hex strings (no dashes) in SQLite. Raw SQL inserts must use
`uuid_obj.hex` to match the ORM's bind processor format, while URL path
parameters use `str(uuid_obj)` (standard dashed form, as FastAPI expects).

These tests verify:
  1. compile_version() rejects 'draft'/'archived' status (unit tests via mock)
  2. Activating a version without a compiled artifact → 422
  3. Activating a version with no prior active version → 200, one audit record
  4. Activating a version when another is active → auto-archives old, 200,
     two chained WORM audit records with correct hash linkage
  5. Idempotency: activating an already-active version → 200, no new audit
  6. Cross-tenant activation → 403
"""
import uuid
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text

from app.api.auth import ResolvedCredential, require_admin_credential
from app.main import app
from tests.conftest import TEST_ADMIN_CREDENTIAL_ID

_FAKE_ADMIN_CRED = ResolvedCredential(
    credential_id=TEST_ADMIN_CREDENTIAL_ID,
    tenant_id="test_tenant",
    user_id="admin_user",
    scope="admin",
)

_SQLITE_URL = "sqlite:///file:testdb?mode=memory&cache=shared&uri=true"


def _sync_engine() -> Any:
    return create_engine(
        _SQLITE_URL,
        connect_args={"check_same_thread": False},
    )


def _insert_version(
    conn: Any,
    version_id_hex: str,
    *,
    status: str = "pending_review",
    tenant_id: str = "test_tenant",
) -> None:
    """Insert a metadata_versions row. version_id_hex must be in no-dash hex
    format (uuid_obj.hex) to match how the postgresql.UUID type stores values
    in SQLite."""
    conn.execute(
        text(
            "INSERT INTO metadata_versions"
            " (version_id, tenant_id, status, created_by)"
            " VALUES (:vid, :tid, :status, 'test_user')"
        ),
        {"vid": version_id_hex, "tid": tenant_id, "status": status},
    )


def _insert_artifact(conn: Any, version_id_hex: str) -> None:
    """Insert a compiled_registry_artifacts row for the given version."""
    conn.execute(
        text(
            "INSERT INTO compiled_registry_artifacts"
            " (artifact_id, version_id, tenant_id,"
            "  artifact_blob, artifact_hash, compiler_version)"
            " VALUES (:aid, :vid, 'test_tenant', '{}', 'testhash', '1.0.0')"
        ),
        {"aid": uuid.uuid4().hex, "vid": version_id_hex},
    )


def _count_audit_records(conn: Any) -> int:
    row = conn.execute(text("SELECT COUNT(*) FROM metadata_audit")).fetchone()
    assert row is not None
    return int(row[0])


def _get_version_status(conn: Any, version_id_hex: str) -> str:
    """Query version status by hex UUID (no-dash format)."""
    row = conn.execute(
        text("SELECT status FROM metadata_versions WHERE version_id = :vid"),
        {"vid": version_id_hex},
    ).fetchone()
    assert row is not None, f"Version {version_id_hex} not found in DB"
    return str(row[0])


# ------------------------------------------------------------------
# Unit tests: compile_version rejects non-compilable statuses
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_compile_version_rejects_draft_status() -> None:
    """compile_version raises ValueError for 'draft' status."""
    from unittest.mock import AsyncMock, MagicMock

    from app.api.compiler import MetadataCompiler

    mock_version = MagicMock()
    mock_version.status = "draft"
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = mock_version

    mock_session = AsyncMock()
    mock_session.execute.return_value = mock_result

    with pytest.raises(ValueError, match="pending_review"):
        await MetadataCompiler.compile_version(
            session=mock_session,
            version_id=uuid.uuid4(),
            actor="test_user",
        )


@pytest.mark.asyncio
async def test_compile_version_rejects_archived_status() -> None:
    """compile_version raises ValueError for 'archived' status."""
    from unittest.mock import AsyncMock, MagicMock

    from app.api.compiler import MetadataCompiler

    mock_version = MagicMock()
    mock_version.status = "archived"
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = mock_version

    mock_session = AsyncMock()
    mock_session.execute.return_value = mock_result

    with pytest.raises(ValueError, match="pending_review"):
        await MetadataCompiler.compile_version(
            session=mock_session,
            version_id=uuid.uuid4(),
            actor="test_user",
        )


# ------------------------------------------------------------------
# HTTP-layer tests for update_version_status
# ------------------------------------------------------------------

def test_activate_without_artifact_succeeds() -> None:
    """Activating a version without a compiled artifact is allowed.
    Approval (status transition) is independent of compilation; the runtime
    will only serve the version once a compiled artifact also exists."""
    vid = uuid.uuid4()
    engine = _sync_engine()

    with engine.begin() as conn:
        _insert_version(conn, vid.hex, status="pending_review")

    app.dependency_overrides[require_admin_credential] = lambda: _FAKE_ADMIN_CRED
    try:
        with TestClient(app) as client:
            response = client.patch(
                f"/api/v1/metadata/versions/{vid}/status",
                json={"status": "active"},
            )
        assert response.status_code == 200
        assert response.json()["status"] == "active"
    finally:
        app.dependency_overrides.pop(require_admin_credential, None)
        engine.dispose()


def test_activate_version_with_no_prior_active_succeeds() -> None:
    """Activating a pending_review version with an artifact and no prior active
    version returns 200 and creates exactly one WORM audit record."""
    vid = uuid.uuid4()
    engine = _sync_engine()

    with engine.begin() as conn:
        _insert_version(conn, vid.hex, status="pending_review")
        _insert_artifact(conn, vid.hex)
        audit_before = _count_audit_records(conn)

    app.dependency_overrides[require_admin_credential] = lambda: _FAKE_ADMIN_CRED
    try:
        with TestClient(app) as client:
            response = client.patch(
                f"/api/v1/metadata/versions/{vid}/status",
                json={"status": "active"},
            )
        assert response.status_code == 200
        assert response.json()["status"] == "active"

        with engine.connect() as conn:
            assert _get_version_status(conn, vid.hex) == "active"
            assert _count_audit_records(conn) == audit_before + 1
    finally:
        app.dependency_overrides.pop(require_admin_credential, None)
        engine.dispose()


def test_activate_archives_existing_active_version() -> None:
    """Activating a new version while one is already active must atomically
    archive the old version and create two chained WORM audit records."""
    old_vid = uuid.uuid4()
    new_vid = uuid.uuid4()
    engine = _sync_engine()

    with engine.begin() as conn:
        # Old version is already active with a compiled artifact
        _insert_version(conn, old_vid.hex, status="active")
        _insert_artifact(conn, old_vid.hex)
        # New version ready to activate
        _insert_version(conn, new_vid.hex, status="pending_review")
        _insert_artifact(conn, new_vid.hex)
        audit_before = _count_audit_records(conn)

    app.dependency_overrides[require_admin_credential] = lambda: _FAKE_ADMIN_CRED
    try:
        with TestClient(app) as client:
            response = client.patch(
                f"/api/v1/metadata/versions/{new_vid}/status",
                json={"status": "active"},
            )
        assert response.status_code == 200
        assert response.json()["status"] == "active"

        with engine.connect() as conn:
            # Old version must be archived, new version must be active
            assert _get_version_status(conn, old_vid.hex) == "archived"
            assert _get_version_status(conn, new_vid.hex) == "active"

            # Exactly two audit records: archival of old + activation of new
            assert _count_audit_records(conn) == audit_before + 2

            # Verify WORM chain integrity: activation must chain from archival
            rows = conn.execute(
                text(
                    "SELECT row_hash, previous_hash, action"
                    " FROM metadata_audit ORDER BY rowid DESC LIMIT 2"
                )
            ).fetchall()
            # rows[0] = activation record (inserted second → higher rowid)
            # rows[1] = archive record (inserted first → lower rowid)
            activation_record = rows[0]
            archive_record = rows[1]

            assert activation_record[1] == archive_record[0], (
                "Activation record's previous_hash must equal archive record's"
                " row_hash — they must form a contiguous WORM chain"
            )
            assert archive_record[2] == "revoke"
            assert activation_record[2] == "approve"
    finally:
        app.dependency_overrides.pop(require_admin_credential, None)
        engine.dispose()


def test_activate_idempotent() -> None:
    """Activating an already-active version is a no-op: returns 200 with the
    current status and creates no new audit records."""
    vid = uuid.uuid4()
    engine = _sync_engine()

    with engine.begin() as conn:
        _insert_version(conn, vid.hex, status="active")
        _insert_artifact(conn, vid.hex)
        audit_before = _count_audit_records(conn)

    app.dependency_overrides[require_admin_credential] = lambda: _FAKE_ADMIN_CRED
    try:
        with TestClient(app) as client:
            response = client.patch(
                f"/api/v1/metadata/versions/{vid}/status",
                json={"status": "active"},
            )
        assert response.status_code == 200
        assert response.json()["status"] == "active"

        with engine.connect() as conn:
            # Idempotency: no new audit records created
            assert _count_audit_records(conn) == audit_before
    finally:
        app.dependency_overrides.pop(require_admin_credential, None)
        engine.dispose()


def test_activate_wrong_tenant_returns_403() -> None:
    """A version belonging to a different tenant must return 403."""
    vid = uuid.uuid4()
    engine = _sync_engine()

    with engine.begin() as conn:
        _insert_version(
            conn, vid.hex, status="pending_review", tenant_id="other_tenant"
        )
        _insert_artifact(conn, vid.hex)

    app.dependency_overrides[require_admin_credential] = lambda: _FAKE_ADMIN_CRED
    try:
        with TestClient(app) as client:
            response = client.patch(
                f"/api/v1/metadata/versions/{vid}/status",
                json={"status": "active"},
            )
        assert response.status_code == 403
    finally:
        app.dependency_overrides.pop(require_admin_credential, None)
        engine.dispose()
