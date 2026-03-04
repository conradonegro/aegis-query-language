import uuid
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.vault import EnvFallbackProvider
from app.steward.loader import RegistryLoader, UnauthorizedRegistryTamperError

# Explicitly mark entire module as async context
pytestmark = pytest.mark.asyncio

async def test_db_privilege_enforcement_runtime():
    """Verifies that user_aegis_runtime is utterly blocked from DDL and metadata mutations."""
    provider = EnvFallbackProvider()
    val = provider.get_database_password("user_aegis_runtime")
    url = f"postgresql+asyncpg://user_aegis_runtime:{val}@localhost:5432/aegis_data_warehouse"
    engine = create_async_engine(url)
    
    async with engine.begin() as conn:
        with pytest.raises(Exception):
            await conn.execute(text("INSERT INTO aegis_meta.compiled_registry_artifacts (artifact_id, version_id, artifact_blob, artifact_hash, compiled_at, compiler_version, signature_algo) VALUES ('{}', '{}', '{{}}', 'hash', NOW(), '1', 'hmac')".format(uuid.uuid4(), uuid.uuid4())))
            
        with pytest.raises(Exception):
            await conn.execute(text("CREATE TABLE aegis_meta.hacked_table (id INT)"))
            
    await engine.dispose()


async def test_worm_physical_trigger_enforcement():
    """Verifies that even highly-priviledged stewards are violently blocked by BEFORE UPDATE triggers."""
    provider = EnvFallbackProvider()
    val = provider.get_database_password("user_aegis_steward")
    url = f"postgresql+asyncpg://user_aegis_steward:{val}@localhost:5432/aegis_data_warehouse"
    engine = create_async_engine(url)
    
    audit_id = uuid.uuid4()
    
    async with engine.begin() as conn:
        # Step 1: Insert an audit record
        await conn.execute(
            text("""
            INSERT INTO aegis_meta.metadata_audit (audit_id, actor, action, payload, previous_hash, row_hash, hash_algorithm, timestamp)
            VALUES (:id, 'hacker', 'create', CAST(:json_val AS JSONB), '', :rhash, 'sha256', NOW())
            """),
            {"id": audit_id, "rhash": str(uuid.uuid4()), "json_val": '{"test":true}'}
        )
        
    async with engine.begin() as conn:
        # Step 2: Attempt UPDATE (Blocked by Trigger + Revoke)
        with pytest.raises(Exception):
            await conn.execute(
                text("UPDATE aegis_meta.metadata_audit SET actor = 'innocent' WHERE audit_id = :id"),
                {"id": audit_id}
            )
            
        # Step 3: Attempt DELETE (Blocked by Trigger + Revoke)
        with pytest.raises(Exception):
             await conn.execute(
                text("DELETE FROM aegis_meta.metadata_audit WHERE audit_id = :id"),
                {"id": audit_id}
            )

    # Superuser backdoor cleanup (as we cannot delete via normal roles)
    val_admin = provider.get_database_password("user_aegis_meta_owner")
    url_admin = f"postgresql+asyncpg://user_aegis_meta_owner:{val_admin}@localhost:5432/aegis_data_warehouse"
    engine_admin = create_async_engine(url_admin)
    async with engine_admin.begin() as conn_admin:
        await conn_admin.execute(text("ALTER TABLE aegis_meta.metadata_audit DISABLE TRIGGER USER"))
        await conn_admin.execute(text("DELETE FROM aegis_meta.metadata_audit WHERE audit_id = :id"), {"id": audit_id})
        await conn_admin.execute(text("ALTER TABLE aegis_meta.metadata_audit ENABLE TRIGGER USER"))
    await engine_admin.dispose()

    await engine.dispose()


class MockSession:
    """Mocks AsyncSession strictly resolving a physical PG execute to avoid extensive ORM boilerplate."""
    def __init__(self, engine):
        self.engine = engine
        
    async def execute(self, stmt):
        class MockResult:
            def __init__(self, data):
                self.data = data
            def scalar_one_or_none(self):
                return self.data
                
        async with self.engine.begin() as conn:
            # Reconstruct the compiler artifact format dynamically
            res = await conn.execute(text("SELECT artifact_blob, artifact_hash, signature_key_id, signature, artifact_id FROM aegis_meta.compiled_registry_artifacts WHERE version_id = :vid LIMIT 1"), {"vid": self.vid})
            row = res.fetchone()
            if not row: return MockResult(None)
            
            # Map Row to Dummy Model
            from collections import namedtuple
            Model = namedtuple('Model', ['artifact_blob', 'artifact_hash', 'signature_key_id', 'signature', 'artifact_id'])
            return MockResult(Model(row[0], row[1], row[2], row[3], row[4]))


async def test_registry_forgery_halt():
    """Verifies that an altered Artifact JSON blob natively crashes the FastAPI bootloader."""
    provider = EnvFallbackProvider()
    val = provider.get_database_password("user_aegis_meta_owner")
    url = f"postgresql+asyncpg://user_aegis_meta_owner:{val}@localhost:5432/aegis_data_warehouse"
    engine = create_async_engine(url)
    
    vid = uuid.uuid4()
    mock_session = MockSession(engine)
    mock_session.vid = vid
    
    # 1. Insert a violently forged artifact into the DB
    async with engine.begin() as conn:
        # Insert a dummy MetadataVersion to satisfy strict Foreign Key boundaries
        await conn.execute(
            text("INSERT INTO aegis_meta.metadata_versions (version_id, status, created_by, created_at) VALUES (:vid, 'active', 'system', NOW())"),
            {"vid": vid}
        )
        await conn.execute(
            text("""
            INSERT INTO aegis_meta.compiled_registry_artifacts (artifact_id, version_id, artifact_blob, artifact_hash, compiled_at, compiler_version, signature, signature_algo)
            VALUES (:aid, :vid, CAST(:json_val AS JSONB), 'fake_hash', NOW(), '1.0.0', 'fake_sig', 'hmac')
            """),
            {"aid": uuid.uuid4(), "vid": vid, "json_val": '{"hacked": "payload"}'}
        )
    
    # Assert the boot sequence violently halts on discrepancy
    with pytest.raises(UnauthorizedRegistryTamperError, match="discrepancy"):
        await RegistryLoader.load_active_schema(mock_session)
        
    # 2. Test HMAC verification explicitly by matching the hash but breaking the sig
    from app.audit.chaining import get_canonical_json
    import hashlib
    
    canon = get_canonical_json({"hacked": "payload"})
    valid_hash = hashlib.sha256(canon.encode("utf-8")).hexdigest()
    
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE aegis_meta.compiled_registry_artifacts SET artifact_hash = :vh WHERE version_id = :vid"), {"vh": valid_hash, "vid": vid})
    
    with pytest.raises(UnauthorizedRegistryTamperError, match="HMAC Signature match absolutely failed"):
        await RegistryLoader.load_active_schema(mock_session)
        
    # Cleanup injected forged payload
    async with engine.begin() as conn:
        await conn.execute(text("DELETE FROM aegis_meta.compiled_registry_artifacts WHERE version_id = :vid"), {"vid": vid})
        await conn.execute(text("DELETE FROM aegis_meta.metadata_versions WHERE version_id = :vid"), {"vid": vid})
        
    await engine.dispose()
