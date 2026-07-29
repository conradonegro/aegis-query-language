"""A column that becomes ineligible for RAG must have its extracted values
purged from the registry.

_run_strategy_refresh only visits eligible columns, so its per-column DELETE
never runs for a column that just became sensitive or had rag_enabled turned
off — leaving previously extracted production values at rest indefinitely.

Tables are created with raw DDL rather than Base.metadata.create_all because
several registry columns are PostgreSQL JSONB, which the SQLite dialect
cannot render. This mirrors tests/test_metadata_clone.py.
"""

import uuid
from typing import Any

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.meta_models import MetadataColumn, MetadataColumnValue
from app.api.router import _purge_ineligible_rag_values

_DDL_COLUMNS = """
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
"""

_DDL_VALUES = """
    CREATE TABLE IF NOT EXISTS metadata_column_values (
        value_id TEXT PRIMARY KEY,
        column_id TEXT NOT NULL,
        version_id TEXT NOT NULL,
        value TEXT NOT NULL,
        active INTEGER NOT NULL DEFAULT 1,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
"""


def _column(
    version_id: uuid.UUID, table_id: uuid.UUID, name: str, **kw: Any
) -> MetadataColumn:
    defaults: dict[str, Any] = {
        "rag_enabled": True,
        "is_sensitive": False,
        "refresh_on_compile": True,
    }
    defaults.update(kw)
    return MetadataColumn(
        column_id=uuid.uuid4(),
        version_id=version_id,
        table_id=table_id,
        real_name=name,
        alias=name,
        data_type="text",
        **defaults,
    )


@pytest.mark.asyncio
async def test_purge_removes_values_for_sensitive_and_disabled_columns() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.execute(text(_DDL_COLUMNS))
        await conn.execute(text(_DDL_VALUES))
    maker = async_sessionmaker(engine, expire_on_commit=False)

    version_id = uuid.uuid4()
    table_id = uuid.uuid4()
    async with maker() as session:
        keep = _column(version_id, table_id, "city")
        sensitive = _column(version_id, table_id, "displayname", is_sensitive=True)
        disabled = _column(version_id, table_id, "location", rag_enabled=False)
        session.add_all([keep, sensitive, disabled])
        for col in (keep, sensitive, disabled):
            for val in ("alpha", "beta"):
                session.add(
                    MetadataColumnValue(
                        column_id=col.column_id,
                        version_id=version_id,
                        value=val,
                    )
                )
        await session.commit()
        keep_id = keep.column_id

    async with maker() as session:
        deleted = await _purge_ineligible_rag_values(session, version_id)
        await session.commit()

    # Two values each from the sensitive column and the rag-disabled column.
    assert deleted == 4

    async with maker() as session:
        rows = (
            (await session.execute(select(MetadataColumnValue.column_id)))
            .scalars()
            .all()
        )
    assert set(rows) == {keep_id}
    assert len(rows) == 2

    await engine.dispose()
