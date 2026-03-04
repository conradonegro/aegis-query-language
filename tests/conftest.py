import os
import logging
import pytest
import pytest_asyncio
from sqlalchemy import create_engine, text

from app.steward.models import (
    AbstractColumnDef,
    AbstractRelationshipDef,
    AbstractTableDef,
    RegistrySchema,
    SafetyClassification,
)

# Force SQLite in-memory overrides for all tests before `main.py` is imported
@pytest.fixture(autouse=True, scope="function")
def override_database_url_for_tests(monkeypatch):
    """
    Overrides the DATABASE_URL environment variable exactly as requested by the user.
    This hooks into `app.main:lifespan` enforcing `aiosqlite:///:memory:` seamlessly.
    """
    # We must use a named shared memory URI so the async tests and sync seeder hit the identical instance.
    test_db_url = "sqlite+aiosqlite:///file:testdb?mode=memory&cache=shared&uri=true"
    monkeypatch.setenv("DATABASE_URL", test_db_url)
    monkeypatch.setenv("DB_URL_RUNTIME", test_db_url)
    monkeypatch.setenv("DB_URL_REGISTRY_RUNTIME", test_db_url)
    monkeypatch.setenv("DB_URL_STEWARD", test_db_url)
    monkeypatch.setenv("DB_URL_REGISTRY_ADMIN", test_db_url)
    monkeypatch.setenv("LLM_PROVIDER", "mock")
    monkeypatch.setenv("OPENAI_API_KEY", "test_key_sandbox")
    monkeypatch.setenv("TESTING", "true")

@pytest.fixture(autouse=True, scope="function")
def seed_memory_db_for_tests():
    """
    Automatically creates the isolated mock tables that were removed from main.py
    for all integration and API tests that rely on an ExecutionEngine resolving rows.
    Uses synchronous execution to prevent pytest_asyncio scope strict failures on sync endpoints.
    """
    test_db_url = "sqlite:///file:testdb?mode=memory&cache=shared&uri=true"
    
    # We must enforce check_same_thread=False for the shared URI so Pytest workers don't lock
    engine = create_engine(test_db_url, echo=False, connect_args={"check_same_thread": False})
    
    with engine.begin() as conn:
        logger = logging.getLogger(__name__)
        logger.debug("Seeding SQLite Pytest Mock Environment...")
        
        # Idempotency lock: drop existing rows when the shared memory pool persists across function scopes
        conn.execute(text("CREATE TABLE IF NOT EXISTS users (id INTEGER, name TEXT, active BOOLEAN, created_at TEXT)"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS orders (id INTEGER, user_id INTEGER, total_amount REAL)"))
        conn.execute(text("DELETE FROM orders"))
        conn.execute(text("DELETE FROM users"))
        
        conn.execute(text("INSERT INTO users VALUES (1, 'Alice', 1, '2025-01-01')"))
        conn.execute(text("INSERT INTO users VALUES (2, 'Bob', 1, '2025-01-02')"))
        conn.execute(text("INSERT INTO users VALUES (3, 'Charlie', 0, '2025-01-03')"))
        
        conn.execute(text("CREATE TABLE IF NOT EXISTS orders (id INTEGER, user_id INTEGER, total_amount REAL)"))
        conn.execute(text("INSERT INTO orders VALUES (101, 1, 99.99)"))
        conn.execute(text("INSERT INTO orders VALUES (102, 1, 150.00)"))
        conn.execute(text("INSERT INTO orders VALUES (103, 2, 45.50)"))
    
    # Do not call dispose here, the executing test lifespan will use this memory pool implicitly
    yield
    engine.dispose()
