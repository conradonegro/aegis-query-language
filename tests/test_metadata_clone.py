"""Regression tests for the metadata clone endpoint.

The clone endpoint at POST /api/v1/metadata/versions previously silently
dropped sample_values and curated MetadataColumnValue rows when cloning
a baseline. These tests pin down the fix from code-review finding #7
(2026-04-07):

  - sample_values is copied
  - sample_values_exhaustive is copied
  - active MetadataColumnValue rows are copied (rebound to the new
    version_id and column_id)
  - inactive MetadataColumnValue rows are NOT copied
"""
import json
import uuid
from typing import Any

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


def _seed_baseline_with_values_and_samples(conn: Any) -> str:
    """Insert a baseline version with one table, one column, two values
    (one active, one archived), and a non-empty sample_values list.
    Returns the baseline version_id (hex form, no dashes)."""
    vid = uuid.uuid4()
    tid = uuid.uuid4()
    cid = uuid.uuid4()

    conn.execute(
        text(
            "INSERT INTO metadata_versions"
            " (version_id, tenant_id, status, created_by)"
            " VALUES (:vid, 'test_tenant', 'pending_review', 'baseline_seed')"
        ),
        {"vid": vid.hex},
    )
    # NOTE: conftest.py creates the metadata_versions / artifacts / audit
    # tables but does NOT create metadata_tables / metadata_columns /
    # metadata_relationships. Add ad-hoc CREATE IF NOT EXISTS so this test
    # is self-contained without touching conftest.py for unrelated tests.
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS metadata_tables (
            table_id TEXT PRIMARY KEY,
            version_id TEXT NOT NULL,
            real_name TEXT NOT NULL,
            alias TEXT NOT NULL,
            description TEXT,
            tenant_id TEXT,
            active INTEGER NOT NULL DEFAULT 1,
            source_database TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS metadata_columns (
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
        )
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS metadata_relationships (
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
        )
    """))
    # metadata_column_values is already created by conftest.py.

    # Clean up any rows leftover from a prior test run sharing the same
    # in-memory DB (the autouse fixture clears versions/artifacts/audit but
    # not these ad-hoc tables).
    conn.execute(text("DELETE FROM metadata_relationships"))
    conn.execute(text("DELETE FROM metadata_columns"))
    conn.execute(text("DELETE FROM metadata_tables"))
    conn.execute(text("DELETE FROM metadata_column_values"))

    conn.execute(
        text(
            "INSERT INTO metadata_tables"
            " (table_id, version_id, real_name, alias, description, active)"
            " VALUES (:tid, :vid, 'members', 'members',"
            " 'Test members', 1)"
        ),
        {"tid": tid.hex, "vid": vid.hex},
    )
    conn.execute(
        text(
            "INSERT INTO metadata_columns"
            " (column_id, version_id, table_id, real_name, alias,"
            " data_type, is_nullable, is_primary_key, allowed_in_select,"
            " allowed_in_filter, allowed_in_join, sample_values,"
            " sample_values_exhaustive)"
            " VALUES (:cid, :vid, :tid, 'position', 'position',"
            " 'text', 1, 0, 1, 1, 0, :sv, 1)"
        ),
        {
            "cid": cid.hex,
            "vid": vid.hex,
            "tid": tid.hex,
            "sv": json.dumps(["President", "Vice President", "Member"]),
        },
    )
    # Two values: one active (must be cloned), one archived (must NOT).
    conn.execute(
        text(
            "INSERT INTO metadata_column_values"
            " (value_id, column_id, version_id, value, active)"
            " VALUES (:vid_v, :cid, :vid, 'Treasurer', 1)"
        ),
        {"vid_v": uuid.uuid4().hex, "cid": cid.hex, "vid": vid.hex},
    )
    conn.execute(
        text(
            "INSERT INTO metadata_column_values"
            " (value_id, column_id, version_id, value, active)"
            " VALUES (:vid_v, :cid, :vid, 'Retired', 0)"
        ),
        {"vid_v": uuid.uuid4().hex, "cid": cid.hex, "vid": vid.hex},
    )
    return vid.hex


def test_clone_preserves_sample_values_and_active_curated_values() -> None:
    """Cloning a baseline must copy sample_values, sample_values_exhaustive,
    and active MetadataColumnValue rows. Archived values must NOT be copied.
    """
    engine = _sync_engine()
    with engine.begin() as conn:
        baseline_vid_hex = _seed_baseline_with_values_and_samples(conn)

    app.dependency_overrides[require_admin_credential] = lambda: _FAKE_ADMIN_CRED
    try:
        with TestClient(app) as client:
            # Convert hex to dashed UUID form for the JSON payload.
            baseline_uuid = str(uuid.UUID(baseline_vid_hex))
            response = client.post(
                "/api/v1/metadata/versions",
                json={"baseline_version_id": baseline_uuid},
            )
        assert response.status_code in (200, 201), response.text
        new_version_id = uuid.UUID(response.json()["version_id"]).hex

        with engine.connect() as conn:
            # Sample values and exhaustive flag carried forward
            row = conn.execute(
                text(
                    "SELECT sample_values, sample_values_exhaustive"
                    " FROM metadata_columns"
                    " WHERE version_id = :vid"
                ),
                {"vid": new_version_id},
            ).fetchone()
            assert row is not None
            assert json.loads(row[0]) == [
                "President", "Vice President", "Member"
            ]
            assert int(row[1]) == 1

            # Curated values: only the active one carried forward
            value_rows = conn.execute(
                text(
                    "SELECT value, active FROM metadata_column_values"
                    " WHERE version_id = :vid ORDER BY value"
                ),
                {"vid": new_version_id},
            ).fetchall()
            assert len(value_rows) == 1
            assert value_rows[0][0] == "Treasurer"
            assert int(value_rows[0][1]) == 1
    finally:
        app.dependency_overrides.pop(require_admin_credential, None)
        engine.dispose()
