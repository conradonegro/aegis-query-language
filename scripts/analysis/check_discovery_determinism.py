"""Verify whether metadata discovery's sample_values selection is deterministic.

``discover_metadata.py`` picks sample values with ``ORDER BY COUNT(*) DESC``
and no tie-break, so whenever two values share a frequency, PostgreSQL is free
to return them in any order. That matters well beyond tidiness: sample values
are rendered into the system prompt, so a re-discovery silently rewrites
prompts for databases that did not change, which invalidates every cached
replay response and forces a full paid regeneration.

This re-runs discovery's EXACT sampling SQL for every column that has stored
sample_values, applies the same truncation, and diffs against what is stored.
It also measures the structural risk surface: COUNT(*) ties at the top-8
boundary (which make the *membership* of the sample unspecified) and ties
within the sample (which make its *order* unspecified).

Needs a superuser connection because it reads the ``aegis_meta`` schema.

Usage::

    uv run python scripts/analysis/check_discovery_determinism.py
"""

from __future__ import annotations

import asyncio
import json
import os
from collections import Counter
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

DB_URL = os.environ.get(
    "AEGIS_META_DB_URL",
    "postgresql+asyncpg://postgres:postgrespassword"
    "@127.0.0.1:5433/aegis_data_warehouse",
)
MAX_SAMPLE_VALUE_LEN = 80
SAMPLE_LIMIT = 8

_COLUMNS_SQL = """
    SELECT t.source_database, t.real_name AS tbl, c.real_name AS col,
           c.sample_values, c.sample_values_exhaustive
    FROM aegis_meta.metadata_columns c
    JOIN aegis_meta.metadata_tables t ON t.table_id = c.table_id
    JOIN aegis_meta.metadata_versions v ON v.version_id = t.version_id
    WHERE c.sample_values IS NOT NULL AND v.status = 'active'
"""


def _truncate(values: list[str]) -> list[str]:
    """Mirror discover_metadata._truncate_samples exactly."""
    return [
        v if len(v) <= MAX_SAMPLE_VALUE_LEN else v[:MAX_SAMPLE_VALUE_LEN] + "..."
        for v in values
    ]


def _sample_sql(table: str, column: str, exhaustive: bool) -> str:
    base = (
        f'SELECT "{column}", COUNT(*) FROM "{table}"'
        f' WHERE "{column}" IS NOT NULL GROUP BY "{column}"'
        f" ORDER BY COUNT(*) DESC"
    )
    return base if exhaustive else f"{base} LIMIT {SAMPLE_LIMIT}"


async def _boundary_tie(
    conn: AsyncConnection, table: str, column: str, last_count: int
) -> bool:
    """True when the value just outside the top-N ties with the last one in."""
    result = await conn.execute(
        text(
            f'SELECT COUNT(*) AS n FROM "{table}" WHERE "{column}" IS NOT NULL'
            f' GROUP BY "{column}" ORDER BY COUNT(*) DESC'
            f" OFFSET {SAMPLE_LIMIT} LIMIT 1"
        )
    )
    rows = result.fetchall()
    return bool(rows) and rows[0][0] == last_count


async def main() -> None:
    engine = create_async_engine(DB_URL)
    mismatches: list[tuple[str, str, str, list[str], list[str]]] = []
    tie_boundary: list[tuple[str, str, str]] = []
    tie_within: list[tuple[str, str, str]] = []
    errors: list[tuple[str, str, str]] = []
    checked = 0

    try:
        async with engine.connect() as conn:
            columns = (await conn.execute(text(_COLUMNS_SQL))).fetchall()
            print(f"columns with sample_values (active version): {len(columns)}")

            for source_db, table, column, stored_raw, exhaustive in columns:
                if stored_raw is None:  # jsonb 'null' still passes IS NOT NULL
                    continue
                stored: list[str] = (
                    stored_raw
                    if isinstance(stored_raw, list)
                    else json.loads(stored_raw)
                )
                checked += 1
                try:
                    result = await conn.execute(
                        text(_sample_sql(table, column, bool(exhaustive)))
                    )
                    rows: list[Any] = list(result.fetchall())
                except Exception as exc:  # noqa: BLE001 - skip, keep sweeping
                    errors.append((table, column, type(exc).__name__))
                    continue

                fresh = _truncate([str(row[0]) for row in rows])
                if fresh != stored:
                    mismatches.append((source_db, table, column, stored, fresh))

                counts = [row[1] for row in rows]
                if len(set(counts)) < len(counts):
                    tie_within.append((source_db, table, column))
                if (
                    not exhaustive
                    and len(rows) == SAMPLE_LIMIT
                    and await _boundary_tie(conn, table, column, counts[-1])
                ):
                    tie_boundary.append((source_db, table, column))
    finally:
        await engine.dispose()

    print(f"\nre-run differs from stored : {len(mismatches)}/{checked}")
    print(f"ties WITHIN the sample (order unspecified)         : {len(tie_within)}")
    print(f"ties AT the top-{SAMPLE_LIMIT} BOUNDARY (membership unspecified): "
          f"{len(tie_boundary)}")
    print(f"query errors: {len(errors)}")

    if mismatches:
        print("\nfirst mismatches:")
        for source_db, table, column, stored, fresh in mismatches[:8]:
            print(f"  {source_db}.{table}.{column}")
            print(f"    stored: {stored[:SAMPLE_LIMIT]}")
            print(f"    fresh : {fresh[:SAMPLE_LIMIT]}")
    if tie_boundary:
        per_db = Counter(db for db, _, _ in tie_boundary)
        print(f"\nboundary-tie columns by database: {per_db.most_common()}")
        examples = [f"{t}.{c}" for _, t, c in tie_boundary[:10]]
        print(f"  examples: {examples}")


if __name__ == "__main__":
    asyncio.run(main())
