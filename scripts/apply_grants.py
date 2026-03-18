"""Apply aegis_meta table-level grants after Alembic has created the tables.

Run this script immediately after `alembic upgrade head`.  The migrate service
in docker-compose.yml chains both commands so they always execute together.

Connects as user_aegis_meta_owner (the Alembic owner) which owns all
aegis_meta tables and therefore has the right to GRANT on them.
"""

import asyncio
import os

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL must be set.")

engine = create_async_engine(DATABASE_URL)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)

# Each string is executed as a separate statement.
_GRANTS: list[str] = [
    # role_aegis_runtime — FastAPI / Aegis proxy during request execution
    "GRANT SELECT ON aegis_meta.compiled_registry_artifacts TO role_aegis_runtime",
    "GRANT SELECT, INSERT, UPDATE ON aegis_meta.chat_sessions TO role_aegis_runtime",
    "GRANT SELECT, INSERT ON aegis_meta.chat_messages TO role_aegis_runtime",
    # role_aegis_registry_runtime — compiler loader at startup / reload
    "GRANT SELECT ON aegis_meta.compiled_registry_artifacts"
    " TO role_aegis_registry_runtime",
    "GRANT SELECT ON aegis_meta.metadata_versions TO role_aegis_registry_runtime",
    # role_aegis_steward — Steward UI / metadata editors
    "GRANT SELECT ON ALL TABLES IN SCHEMA aegis_meta TO role_aegis_steward",
    "GRANT INSERT, UPDATE ON aegis_meta.metadata_tables TO role_aegis_steward",
    "GRANT INSERT, UPDATE ON aegis_meta.metadata_columns TO role_aegis_steward",
    "GRANT INSERT, UPDATE ON aegis_meta.metadata_relationships TO role_aegis_steward",
    "GRANT INSERT ON aegis_meta.metadata_versions TO role_aegis_steward",
    "GRANT INSERT ON aegis_meta.metadata_audit TO role_aegis_steward",
    "GRANT INSERT, UPDATE, DELETE ON aegis_meta.metadata_column_values"
    " TO role_aegis_steward",
    # Revoke any spurious direct-user grants that may have been applied manually
    # (idempotent — REVOKE is a no-op if the privilege doesn't exist)
    "REVOKE INSERT, UPDATE, DELETE ON aegis_meta.metadata_column_values"
    " FROM user_aegis_steward",
    # role_aegis_registry_admin — controlled deployment pipeline / senior operator
    "GRANT SELECT ON ALL TABLES IN SCHEMA aegis_meta TO role_aegis_registry_admin",
    "GRANT UPDATE (status, registry_hash, approved_by, approved_at)"
    " ON aegis_meta.metadata_versions TO role_aegis_registry_admin",
    # DELETE is required: compile_version() deletes the old artifact before insert.
    "GRANT INSERT, DELETE ON aegis_meta.compiled_registry_artifacts"
    " TO role_aegis_registry_admin",
    "GRANT INSERT ON aegis_meta.metadata_audit TO role_aegis_registry_admin",
]


async def apply_grants() -> None:
    async with AsyncSessionLocal() as session:
        for stmt in _GRANTS:
            await session.execute(text(stmt))
        await session.commit()
    print("[*] aegis_meta table grants applied successfully.")


if __name__ == "__main__":
    asyncio.run(apply_grants())
