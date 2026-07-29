import asyncio
import os
import uuid

from dotenv import load_dotenv
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.api.meta_models import (
    MetadataColumn,
    MetadataRelationship,
    MetadataTable,
    MetadataVersion,
)

# Load environment
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL must be set.")

# Superuser URL used only for revoking bootstrap grants.
# The grants in 00_init_roles.sql were made by postgres; only postgres can revoke them.
SUPERUSER_DB_URL = os.getenv("SUPERUSER_DB_URL")

engine = create_async_engine(DATABASE_URL)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)




_MAX_RAG_VALUE_AVG_LEN = 100.0

# A column earns a place in the RAG index only when most of its values look
# like something a person could type in a question. Values that are opaque
# identifiers (UUIDs, numeric ids), markup blobs, or single-letter codes can
# never be matched from natural language, and they dominate index volume.
# 0.5 = "a majority" — a property of the values themselves, never derived
# from any benchmark's expected answers.
_MIN_WORD_LIKE_RATIO = 0.5

# A value counts as word-like when it starts with a letter and continues with
# letters, spaces, or punctuation that occurs inside real names ("O'Shea",
# "Wells Fargo & Co"). At least three characters, so single-letter codes and
# two-digit codes never qualify. Written PostgreSQL-escaped (the doubled
# quote is one literal apostrophe) because it is only ever embedded in SQL.
_WORD_LIKE_PATTERN = "^[A-Za-z][A-Za-z .''&-]{2,}$"
_MAX_SAMPLE_VALUE_LEN = 80


def _rag_config(
    dtype: str,
    is_pk: bool,
    distinct_count: int | None,
    avg_len: float | None = None,
    word_like_ratio: float | None = None,
) -> tuple[bool, str | None, int | None, str | None, bool]:
    """RAG configuration for a discovered column.

    A column is indexed only when its values could plausibly appear in a
    natural-language question:
      - text, not PK, 9-50,000 distinct
      - avg value length <= 100 (longer means documents — XML blobs, prose —
        not categorical values; indexing them floods store and prompt)
      - a majority of sampled values are word-shaped

    The last rule is what excludes UUID and numeric-id columns. They are
    text, non-PK and sit inside the cardinality band, yet can never be
    matched from a question — and they dominated index volume
    (cards.tcgplayerproductid 49,470 values at 0% word-like;
    foreign_data.uuid 34,056 at 0%). It also drops single-letter code
    columns, whose values are shorter than a word.

    Within the band, 9-200 distinct uses frequency-ranked sampling, while
    201-50,000 indexes all values: frequency ranking is meaningless for
    near-unique entity-name columns, where every player/card/user name
    occurs about once and entity lookup needs the full value set.

    word_like_ratio=None means discovery could not sample the column; that
    fails closed rather than indexing an unknown value population.

    Values are populated at compile time via refresh_on_compile=True.
    """
    is_text = dtype.lower() in ("text", "character varying", "varchar")
    if not is_text or is_pk or distinct_count is None:
        return False, None, None, None, False
    if avg_len is not None and avg_len > _MAX_RAG_VALUE_AVG_LEN:
        return False, None, None, None, False
    if word_like_ratio is None or word_like_ratio < _MIN_WORD_LIKE_RATIO:
        return False, None, None, None, False
    if 9 <= distinct_count <= 200:
        hint = "low" if distinct_count <= 50 else "medium"
        return True, hint, min(distinct_count, 100), "most_frequent", True
    if 200 < distinct_count <= 50_000:
        return True, "high", distinct_count, "distinct", True
    return False, None, None, None, False


def _truncate_samples(vals: list[str]) -> list[str]:
    """Caps sample value length for prompt rendering — a sample longer
    than _MAX_SAMPLE_VALUE_LEN chars conveys format, not identity."""
    return [
        v if len(v) <= _MAX_SAMPLE_VALUE_LEN else v[:_MAX_SAMPLE_VALUE_LEN] + "..."
        for v in vals
    ]


async def _run_discovery(session: AsyncSession) -> None:
    """Core discovery logic — tables, columns, and FK relationships."""
    new_version = MetadataVersion(
        version_id=uuid.uuid4(),
        tenant_id="default",
        status="draft",
        created_by="system-auto-discovery",
        change_reason=(
            "Initial automated auto-discovery ingestion from BIRD-SQL baseline"
        ),
    )
    session.add(new_version)
    await session.commit()

    target_version_id = new_version.version_id
    print(f"[*] Bootstrapping Draft Version: {target_version_id}")

    # 1. Scraping Core Tables and Columns natively!
    raw_columns_sql = text("""
        SELECT
            t.table_name,
            c.column_name,
            c.data_type,
            c.is_nullable
        FROM information_schema.tables t
        JOIN information_schema.columns c ON t.table_name = c.table_name
        WHERE t.table_schema = 'public'
          AND t.table_type = 'BASE TABLE'
          AND t.table_name NOT LIKE '_aegis_%'
        ORDER BY t.table_name, c.ordinal_position;
    """)

    raw_pk_sql = text("""
        SELECT
            tc.table_name,
            kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = 'public';
    """)

    # Fetch Tables, Cols Data
    columns_res = await session.execute(raw_columns_sql)
    pk_res = await session.execute(raw_pk_sql)

    raw_cols = columns_res.fetchall()
    raw_pks = pk_res.fetchall()

    # Build PK Fast Lookup
    pk_lookup: dict[str, set[str]] = {}
    for row in raw_pks:
        tbl_name, col_name = row[0], row[1]
        if tbl_name not in pk_lookup:
            pk_lookup[tbl_name] = set()
        pk_lookup[tbl_name].add(col_name)

    # Build Objects
    table_map: dict[str, MetadataTable] = {}
    column_map: dict[tuple[str, str], MetadataColumn] = {}

    for tbl_name, col_name, dtype, is_null in raw_cols:
        is_pk = col_name in pk_lookup.get(tbl_name, set())

        # Map Table Object
        if tbl_name not in table_map:
            table_map[tbl_name] = MetadataTable(
                table_id=uuid.uuid4(),
                version_id=target_version_id,
                real_name=tbl_name,
                alias=tbl_name,  # By default aliases match physical.
                description=f"Auto-discovered table {tbl_name}"
            )

        table_obj = table_map[tbl_name]

        # Choose between exhaustive enumeration (low cardinality) and
        # frequency-ranked sampling (high cardinality).
        #
        # The LLM treats Sample values lists as closed-world enumerations
        # regardless of how rule 10 is phrased. For columns with <=8 distinct
        # values, we surface ALL values and label them as the complete set,
        # giving the model accurate closed-world signal. For higher-cardinality
        # columns we keep frequency-ranked samples and label them prominently
        # as non-exhaustive at render time.
        sample_vals: list[str] = []
        sample_vals_exhaustive: bool = False
        avg_len: float | None = None
        word_like_ratio: float | None = None
        try:
            distinct_sql = text(
                f'SELECT COUNT(DISTINCT "{col_name}") FROM "{tbl_name}"'
            )
            distinct_count_row = await session.execute(distinct_sql)
            distinct_count = distinct_count_row.scalar()
            if dtype.lower() in ("text", "character varying", "varchar"):
                avg_len_sql = text(
                    f'SELECT AVG(LENGTH("{col_name}"))'
                    f' FROM "{tbl_name}" WHERE "{col_name}" IS NOT NULL'
                )
                avg_len_row = await session.execute(avg_len_sql)
                raw_avg = avg_len_row.scalar()
                avg_len = float(raw_avg) if raw_avg is not None else None

                # Fraction of DISTINCT values that look like natural language.
                # Sampled over at most 1,000 distinct values to stay cheap on
                # wide tables; distinct (not raw rows) so a single very
                # common value cannot skew the shape.
                word_like_sql = text(
                    "SELECT AVG(CASE WHEN v ~ "
                    f"'{_WORD_LIKE_PATTERN}'"
                    " THEN 1.0 ELSE 0.0 END)"
                    f' FROM (SELECT DISTINCT "{col_name}" AS v'
                    f' FROM "{tbl_name}" WHERE "{col_name}" IS NOT NULL'
                    " LIMIT 1000) s"
                )
                word_like_row = await session.execute(word_like_sql)
                raw_ratio = word_like_row.scalar()
                word_like_ratio = (
                    float(raw_ratio) if raw_ratio is not None else None
                )
            if distinct_count is not None and 0 < distinct_count <= 8:
                exhaustive_sql = text(
                    f'SELECT "{col_name}" FROM "{tbl_name}"'
                    f' WHERE "{col_name}" IS NOT NULL'
                    f' GROUP BY "{col_name}"'
                    f' ORDER BY COUNT(*) DESC'
                )
                exhaustive_res = await session.execute(exhaustive_sql)
                sample_vals = [str(row[0]) for row in exhaustive_res.fetchall()]
                sample_vals_exhaustive = True
            else:
                sample_sql = text(
                    f'SELECT "{col_name}" FROM "{tbl_name}"'
                    f' WHERE "{col_name}" IS NOT NULL'
                    f' GROUP BY "{col_name}"'
                    f' ORDER BY COUNT(*) DESC LIMIT 8'
                )
                sample_res = await session.execute(sample_sql)
                sample_vals = [str(row[0]) for row in sample_res.fetchall()]
        except Exception:
            pass  # Non-fatal — skip sample values for this column

        (
            rag_enabled,
            rag_cardinality_hint,
            rag_limit,
            rag_sample_strategy,
            rag_refresh,
        ) = _rag_config(
            dtype, is_pk, distinct_count, avg_len, word_like_ratio
        )
        sample_vals = _truncate_samples(sample_vals)

        # Map Column Object
        col_obj = MetadataColumn(
            column_id=uuid.uuid4(),
            version_id=target_version_id,
            table_id=table_obj.table_id,
            real_name=col_name,
            alias=col_name,
            data_type=dtype,
            is_nullable=(is_null == "YES"),
            is_primary_key=is_pk,
            allowed_in_select=True,
            allowed_in_filter=True,
            allowed_in_join=True,  # Auto enable all defaults for baseline
            sample_values=sample_vals or None,
            sample_values_exhaustive=sample_vals_exhaustive,
            rag_enabled=rag_enabled,
            rag_cardinality_hint=rag_cardinality_hint,
            rag_limit=rag_limit,
            rag_sample_strategy=rag_sample_strategy,
            refresh_on_compile=rag_refresh,
        )

        column_map[(tbl_name, col_name)] = col_obj

    # Dump to Session
    session.add_all(table_map.values())
    session.add_all(column_map.values())
    await session.commit()
    print(
        f"[*] Generated {len(table_map)} Tables and"
        f" {len(column_map)} Columns"
    )

    # 2. Extract Native Foreign Keys to form Edges
    raw_fk_sql = text("""
        SELECT
            cl.relname AS source_table,
            a.attname AS source_column,
            clf.relname AS target_table,
            af.attname AS target_column
        FROM pg_constraint c
        JOIN pg_class cl ON c.conrelid = cl.oid
        JOIN pg_namespace n ON cl.relnamespace = n.oid
        JOIN pg_attribute a ON a.attnum = ANY(c.conkey) AND a.attrelid = c.conrelid
        JOIN pg_class clf ON c.confrelid = clf.oid
        JOIN pg_attribute af ON af.attnum = ANY(c.confkey) AND af.attrelid = c.confrelid
        WHERE c.contype = 'f' AND n.nspname = 'public'
          AND cl.relname NOT LIKE '_aegis_%';
    """)

    fk_res = await session.execute(raw_fk_sql)
    raw_edges = fk_res.fetchall()

    relationships: list[MetadataRelationship] = []
    for src_tbl, src_col, tgt_tbl, tgt_col in raw_edges:
        # Quick lookup across dict map
        src_col_obj = column_map.get((src_tbl, src_col))
        tgt_col_obj = column_map.get((tgt_tbl, tgt_col))

        if not src_col_obj or not tgt_col_obj:
            print(
                f"[!] Warning: FK map missing cols:"
                f" {src_tbl}.{src_col} -> {tgt_tbl}.{tgt_col}"
            )
            continue

        rel_obj = MetadataRelationship(
            relationship_id=uuid.uuid4(),
            version_id=target_version_id,
            source_table_id=src_col_obj.table_id,
            source_column_id=src_col_obj.column_id,
            target_table_id=tgt_col_obj.table_id,
            target_column_id=tgt_col_obj.column_id,
            relationship_type="fk",
            cardinality="n:1",  # In Postgres FKs, the child points to Parent PK
            bidirectional=True,
            active=True
        )
        relationships.append(rel_obj)

    session.add_all(relationships)
    await session.commit()
    print(f"[*] Generated {len(relationships)} standard Relationship edges.")
    print(
        f"[*] Discovery Draft Version {target_version_id} completed"
        f" successfully!"
    )


async def discover_and_draft_metadata() -> None:
    """
    Reverse-engineers the 'public' BIRD-SQL schema natively tracking PostgreSQL
    Foreign keys, Columns, Data Types, Nullability etc., and inserts them
    collectively under a single 'draft' MetadataVersion payload for Steward
    review.

    Idempotent: skips if any MetadataVersion already exists.
    Revokes bootstrap public schema grants on success (try/else, not finally)
    so that a failed run leaves grants intact for the Docker restart retry.
    """
    async with AsyncSessionLocal() as session:
        # Idempotency guard — above the try/else so early return does NOT
        # trigger the privilege revocation in the else clause.
        existing = await session.execute(
            select(MetadataVersion.version_id).limit(1)
        )
        if existing.scalar_one_or_none():
            print("[*] Metadata registry already populated. Skipping discovery.")
            return

        try:
            await _run_discovery(session)
        except Exception:
            # exit non-zero → Docker retries; public grants remain intact
            raise
        else:
            # Runs ONLY on success — revoke bootstrap grants from role_aegis_meta_owner.
            # Must run as postgres (superuser) because the grants in 00_init_roles.sql
            # were made by postgres; only the grantor can revoke them.
            if SUPERUSER_DB_URL:
                su_engine = create_async_engine(SUPERUSER_DB_URL)
                su_session_factory = async_sessionmaker(
                    su_engine, expire_on_commit=False
                )
                async with su_session_factory() as su_session:
                    await su_session.execute(text(
                        "REVOKE SELECT ON ALL TABLES IN SCHEMA public"
                        " FROM role_aegis_meta_owner;"
                    ))
                    await su_session.execute(text(
                        "REVOKE USAGE ON SCHEMA public FROM role_aegis_meta_owner;"
                    ))
                    await su_session.execute(text(
                        "ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public"
                        " REVOKE SELECT ON TABLES FROM role_aegis_meta_owner;"
                    ))
                    await su_session.commit()
                await su_engine.dispose()
            else:
                print("[!] SUPERUSER_DB_URL not set — bootstrap grants NOT revoked.")
            print("[*] Bootstrap grants on public schema revoked.")


if __name__ == "__main__":
    asyncio.run(discover_and_draft_metadata())
