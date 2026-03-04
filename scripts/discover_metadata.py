import asyncio
import os
import uuid
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

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

engine = create_async_engine(DATABASE_URL)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)


async def discover_and_draft_metadata():
    """
    Reverse-engineers the 'public' BIRD-SQL schema natively tracking PostgreSQL
    Foreign keys, Columns, Data Types, Nullability etc., and inserts them collectively
    under a single 'draft' MetadataVersion payload for Steward review.
    """
    async with AsyncSessionLocal() as session:
        # Create the top-level Container Version
        new_version = MetadataVersion(
            version_id=uuid.uuid4(),
            status="draft",
            created_by="system-auto-discovery",
            change_reason="Initial automated auto-discovery ingestion from BIRD-SQL baseline",
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
                    alias=tbl_name, # By default aliases match physical.
                    description=f"Auto-discovered table {tbl_name}"
                )
            
            table_obj = table_map[tbl_name]

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
                allowed_in_join=True # Auto enable all defaults for baseline
            )
            
            column_map[(tbl_name, col_name)] = col_obj

        # Dump to Session
        session.add_all(table_map.values())
        session.add_all(column_map.values())
        await session.commit()
        print(f"[*] Generated {len(table_map)} Tables and {len(column_map)} Columns")

        # 2. Extract Native Foreign Keys to form Edges
        raw_fk_sql = text("""
            SELECT
                tc.table_name AS source_table,
                kcu.column_name AS source_column,
                ccu.table_name AS target_table,
                ccu.column_name AS target_column
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name
              AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = 'public';
        """)

        fk_res = await session.execute(raw_fk_sql)
        raw_edges = fk_res.fetchall()

        relationships: list[MetadataRelationship] = []
        for src_tbl, src_col, tgt_tbl, tgt_col in raw_edges:
            # Quick lookup across dict map
            src_col_obj = column_map.get((src_tbl, src_col))
            tgt_col_obj = column_map.get((tgt_tbl, tgt_col))

            if not src_col_obj or not tgt_col_obj:
                print(f"[!] Warning: FK map missing cols: {src_tbl}.{src_col} -> {tgt_tbl}.{tgt_col}")
                continue

            rel_obj = MetadataRelationship(
                relationship_id=uuid.uuid4(),
                version_id=target_version_id,
                source_table_id=src_col_obj.table_id,
                source_column_id=src_col_obj.column_id,
                target_table_id=tgt_col_obj.table_id,
                target_column_id=tgt_col_obj.column_id,
                relationship_type="fk",
                cardinality="n:1", # In Postgres FKs, the child points to Parent Pk
                bidirectional=True,
                active=True
            )
            relationships.append(rel_obj)

        session.add_all(relationships)
        await session.commit()
        print(f"[*] Generated {len(relationships)} standard Relationship edges.")
        print(f"[*] Discovery Draft Version {target_version_id} completed successfully!")


if __name__ == "__main__":
    asyncio.run(discover_and_draft_metadata())
