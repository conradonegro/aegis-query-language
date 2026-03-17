import hashlib
import hmac
import logging
import os
from collections.abc import Generator

import pytest
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

# Must be set globally BEFORE pytest collections import app modules
os.environ["TESTING"] = "true"

# ------------------------------------------------------------------
# Shared auth test constants — imported by test_auth.py / test_api.py
# ------------------------------------------------------------------
TEST_HMAC_SECRET = "test_hmac_secret_dev"
TEST_QUERY_RAW_KEY = "aegis_test_query_key"
TEST_ADMIN_RAW_KEY = "aegis_test_admin_key"
TEST_QUERY_CREDENTIAL_ID = "00000000-0000-0000-0000-000000000001"
TEST_ADMIN_CREDENTIAL_ID = "00000000-0000-0000-0000-000000000002"


# Force SQLite in-memory overrides for all tests before `main.py` is imported
@pytest.fixture(autouse=True, scope="function")
def override_database_url_for_tests(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Overrides the DATABASE_URL environment variable exactly as requested by
    the user. This hooks into `app.main:lifespan` enforcing
    `aiosqlite:///:memory:` seamlessly.
    """
    # We must use a named shared memory URI so the async tests and sync seeder
    # hit the identical instance.
    test_db_url = (
        "sqlite+aiosqlite:///file:testdb?mode=memory&cache=shared&uri=true"
    )
    monkeypatch.setenv("DATABASE_URL", test_db_url)
    monkeypatch.setenv("DB_URL_RUNTIME", test_db_url)
    monkeypatch.setenv("DB_URL_REGISTRY_RUNTIME", test_db_url)
    monkeypatch.setenv("DB_URL_STEWARD", test_db_url)
    monkeypatch.setenv("DB_URL_REGISTRY_ADMIN", test_db_url)
    monkeypatch.setenv("LLM_PROVIDER", "mock")
    monkeypatch.setenv("OPENAI_API_KEY", "test_key_sandbox")
    monkeypatch.setenv("TESTING", "true")
    monkeypatch.setenv("API_KEY_HMAC_SECRET", TEST_HMAC_SECRET)


@pytest.fixture(autouse=True, scope="function")
def seed_memory_db_for_tests() -> Generator[None, None, None]:
    """
    Automatically creates the isolated mock tables that were removed from
    main.py for all integration and API tests that rely on an ExecutionEngine
    resolving rows. Uses synchronous execution to prevent pytest_asyncio scope
    strict failures on sync endpoints.
    """
    test_db_url = "sqlite:///file:testdb?mode=memory&cache=shared&uri=true"

    # We must enforce check_same_thread=False for the shared URI so Pytest
    # workers don't lock
    engine = create_engine(
        test_db_url, echo=False, connect_args={"check_same_thread": False}
    )

    with engine.begin() as conn:
        logger = logging.getLogger(__name__)
        logger.debug("Seeding SQLite Pytest Mock Environment...")

        # Idempotency lock: drop existing rows when the shared memory pool
        # persists across function scopes
        conn.execute(text(
            "CREATE TABLE IF NOT EXISTS users"
            " (id INTEGER, name TEXT, active BOOLEAN, created_at TEXT)"
        ))
        conn.execute(text(
            "CREATE TABLE IF NOT EXISTS orders"
            " (id INTEGER, user_id INTEGER, total_amount REAL)"
        ))
        conn.execute(text("DELETE FROM orders"))
        conn.execute(text("DELETE FROM users"))

        # Chat history tables (mirrors PostgreSQL ORM models, using TEXT for
        # UUIDs in SQLite)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS chat_sessions (
                session_id TEXT PRIMARY KEY,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                tenant_id TEXT,
                user_id TEXT,
                title TEXT,
                metadata TEXT
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS chat_messages (
                message_id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                sequence_number INTEGER NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                provider_id TEXT,
                prompt_tokens INTEGER,
                completion_tokens INTEGER,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                timestamp TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """))
        conn.execute(text("DELETE FROM chat_messages"))
        conn.execute(text("DELETE FROM chat_sessions"))

        # Tenant credentials table (no schema prefix, TEXT scope for SQLite)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS tenant_credentials (
                credential_id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                key_hash TEXT NOT NULL UNIQUE,
                scope TEXT NOT NULL,
                description TEXT,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """))
        conn.execute(text("DELETE FROM tenant_credentials"))

        # Pre-hash test keys using the same algorithm as _hash_api_key
        _q_hash = hmac.new(
            TEST_HMAC_SECRET.encode("utf-8"),
            TEST_QUERY_RAW_KEY.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        _a_hash = hmac.new(
            TEST_HMAC_SECRET.encode("utf-8"),
            TEST_ADMIN_RAW_KEY.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        conn.execute(text(
            "INSERT INTO tenant_credentials"
            " (credential_id, tenant_id, user_id, key_hash, scope, is_active)"
            f" VALUES ('{TEST_QUERY_CREDENTIAL_ID}', 'test_tenant',"
            f" 'test_user', '{_q_hash}', 'query', 1)"
        ))
        conn.execute(text(
            "INSERT INTO tenant_credentials"
            " (credential_id, tenant_id, user_id, key_hash, scope, is_active)"
            f" VALUES ('{TEST_ADMIN_CREDENTIAL_ID}', 'test_tenant',"
            f" 'admin_user', '{_a_hash}', 'admin', 1)"
        ))

        # RAG curated-values table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS metadata_column_values (
                value_id TEXT PRIMARY KEY,
                column_id TEXT NOT NULL,
                version_id TEXT NOT NULL,
                value TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """))

        conn.execute(text("INSERT INTO users VALUES (1, 'Alice', 1, '2025-01-01')"))
        conn.execute(text("INSERT INTO users VALUES (2, 'Bob', 1, '2025-01-02')"))
        conn.execute(
            text("INSERT INTO users VALUES (3, 'Charlie', 0, '2025-01-03')")
        )

        conn.execute(text(
            "CREATE TABLE IF NOT EXISTS orders"
            " (id INTEGER, user_id INTEGER, total_amount REAL)"
        ))
        conn.execute(text("INSERT INTO orders VALUES (101, 1, 99.99)"))
        conn.execute(text("INSERT INTO orders VALUES (102, 1, 150.00)"))
        conn.execute(text("INSERT INTO orders VALUES (103, 2, 45.50)"))

    # Do not call dispose here, the executing test lifespan will use this
    # memory pool implicitly
    yield
    engine.dispose()
