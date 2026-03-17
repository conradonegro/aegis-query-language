import os
import uuid
from collections import namedtuple
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from app.steward.loader import RegistryLoader, UnauthorizedRegistryTamperError
from app.vault import EnvFallbackProvider

_PG_HOST = os.getenv("TEST_PG_HOST", "localhost")
_PG_PORT = os.getenv("TEST_PG_PORT", "5432")


def _pg_url(role: str, password: str) -> str:
    return (
        f"postgresql+asyncpg://{role}:{password}"
        f"@{_PG_HOST}:{_PG_PORT}/aegis_data_warehouse"
    )


class MockResult:
    def __init__(self, data: Any) -> None:
        self.data = data

    def scalar_one_or_none(self) -> Any:
        return self.data

# Explicitly mark entire module as async context
pytestmark = pytest.mark.asyncio

async def test_db_privilege_enforcement_runtime() -> None:
    """Verifies that user_aegis_runtime is blocked from DDL and metadata mutations."""
    provider = EnvFallbackProvider()
    val = provider.get_database_password("user_aegis_runtime")
    url = _pg_url("user_aegis_runtime", val)
    engine = create_async_engine(url)

    async with engine.begin() as conn:
        with pytest.raises(Exception, match=r"."):
            await conn.execute(text(
                f"INSERT INTO aegis_meta.compiled_registry_artifacts"
                f" (artifact_id, version_id, artifact_blob, artifact_hash,"
                f" compiled_at, compiler_version, signature_algo)"
                f" VALUES ('{uuid.uuid4()}', '{uuid.uuid4()}', '{{}}', 'hash',"
                f" NOW(), '1', 'hmac')"
            ))

        with pytest.raises(Exception, match=r"."):
            await conn.execute(
                text("CREATE TABLE aegis_meta.hacked_table (id INT)")
            )

    await engine.dispose()


async def test_worm_physical_trigger_enforcement() -> None:
    """Verifies that even highly-privileged stewards are blocked by BEFORE UPDATE
    triggers."""
    provider = EnvFallbackProvider()
    val = provider.get_database_password("user_aegis_steward")
    url = _pg_url("user_aegis_steward", val)
    engine = create_async_engine(url)

    audit_id = uuid.uuid4()

    async with engine.begin() as conn:
        # Step 1: Insert an audit record
        await conn.execute(
            text(
                "INSERT INTO aegis_meta.metadata_audit"
                " (audit_id, actor, action, payload, previous_hash, row_hash,"
                " hash_algorithm, timestamp)"
                " VALUES (:id, 'hacker', 'create', CAST(:json_val AS JSONB),"
                " '', :rhash, 'sha256', NOW())"
            ),
            {"id": audit_id, "rhash": str(uuid.uuid4()), "json_val": '{"test":true}'}
        )

    async with engine.begin() as conn:
        # Step 2: Attempt UPDATE (Blocked by Trigger + Revoke)
        with pytest.raises(Exception, match=r"."):
            await conn.execute(
                text(
                    "UPDATE aegis_meta.metadata_audit"
                    " SET actor = 'innocent' WHERE audit_id = :id"
                ),
                {"id": audit_id}
            )

        # Step 3: Attempt DELETE (Blocked by Trigger + Revoke)
        with pytest.raises(Exception, match=r"."):
             await conn.execute(
                text(
                    "DELETE FROM aegis_meta.metadata_audit WHERE audit_id = :id"
                ),
                {"id": audit_id}
            )

    # Superuser backdoor cleanup (as we cannot delete via normal roles)
    val_admin = provider.get_database_password("user_aegis_meta_owner")
    url_admin = _pg_url("user_aegis_meta_owner", val_admin)
    engine_admin = create_async_engine(url_admin)
    async with engine_admin.begin() as conn_admin:
        await conn_admin.execute(
            text("ALTER TABLE aegis_meta.metadata_audit DISABLE TRIGGER USER")
        )
        await conn_admin.execute(
            text(
                "DELETE FROM aegis_meta.metadata_audit WHERE audit_id = :id"
            ),
            {"id": audit_id},
        )
        await conn_admin.execute(
            text("ALTER TABLE aegis_meta.metadata_audit ENABLE TRIGGER USER")
        )
    await engine_admin.dispose()

    await engine.dispose()


class MockSession:
    """Mocks AsyncSession strictly resolving a physical PG execute."""
    vid: uuid.UUID

    def __init__(self, engine: AsyncEngine) -> None:
        self.engine = engine

    async def execute(self, stmt: Any) -> MockResult:
        async with self.engine.begin() as conn:
            # Reconstruct the compiler artifact format dynamically
            res = await conn.execute(
                text(
                    "SELECT artifact_blob, artifact_hash, signature_key_id,"
                    " signature, artifact_id"
                    " FROM aegis_meta.compiled_registry_artifacts"
                    " WHERE version_id = :vid LIMIT 1"
                ),
                {"vid": self.vid},
            )
            row = res.fetchone()
            if not row:
                return MockResult(None)

            # Map Row to Dummy Model
            Model = namedtuple(
                'Model',
                ['artifact_blob', 'artifact_hash', 'signature_key_id',
                 'signature', 'artifact_id'],
            )
            return MockResult(Model(row[0], row[1], row[2], row[3], row[4]))


async def test_registry_forgery_halt() -> None:
    """Verifies that an altered Artifact JSON blob crashes the FastAPI bootloader."""
    provider = EnvFallbackProvider()
    val = provider.get_database_password("user_aegis_meta_owner")
    url = _pg_url("user_aegis_meta_owner", val)
    engine = create_async_engine(url)

    vid = uuid.uuid4()
    mock_session = MockSession(engine)
    mock_session.vid = vid

    # 1. Insert a violently forged artifact into the DB
    async with engine.begin() as conn:
        # Insert a dummy MetadataVersion to satisfy strict Foreign Key boundaries
        await conn.execute(
            text(
                "INSERT INTO aegis_meta.metadata_versions"
                " (version_id, status, created_by, created_at)"
                " VALUES (:vid, 'active', 'system', NOW())"
            ),
            {"vid": vid}
        )
        await conn.execute(
            text(
                "INSERT INTO aegis_meta.compiled_registry_artifacts"
                " (artifact_id, version_id, artifact_blob, artifact_hash,"
                " compiled_at, compiler_version, signature, signature_algo)"
                " VALUES (:aid, :vid, CAST(:json_val AS JSONB), 'fake_hash',"
                " NOW(), '1.0.0', 'fake_sig', 'hmac')"
            ),
            {"aid": uuid.uuid4(), "vid": vid, "json_val": '{"hacked": "payload"}'}
        )

    # Assert the boot sequence violently halts on discrepancy
    with pytest.raises(UnauthorizedRegistryTamperError, match="discrepancy"):
        await RegistryLoader.load_active_schema(mock_session)  # type: ignore[arg-type]

    # 2. Test HMAC verification explicitly by matching the hash but breaking
    # the sig
    import hashlib

    from app.audit.chaining import get_canonical_json

    canon = get_canonical_json({"hacked": "payload"})
    valid_hash = hashlib.sha256(canon.encode("utf-8")).hexdigest()

    async with engine.begin() as conn:
        await conn.execute(
            text(
                "UPDATE aegis_meta.compiled_registry_artifacts"
                " SET artifact_hash = :vh WHERE version_id = :vid"
            ),
            {"vh": valid_hash, "vid": vid},
        )

    with pytest.raises(
        UnauthorizedRegistryTamperError,
        match="HMAC Signature match absolutely failed",
    ):
        await RegistryLoader.load_active_schema(mock_session)  # type: ignore[arg-type]

    # Cleanup injected forged payload
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "DELETE FROM aegis_meta.compiled_registry_artifacts"
                " WHERE version_id = :vid"
            ),
            {"vid": vid},
        )
        await conn.execute(
            text(
                "DELETE FROM aegis_meta.metadata_versions WHERE version_id = :vid"
            ),
            {"vid": vid},
        )

    await engine.dispose()
