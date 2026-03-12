import asyncio
import csv
import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import selectinload

load_dotenv()

from app.api.meta_models import MetadataTable, MetadataVersion  # noqa: E402

# Connect to the steward database
steward_db_url = os.getenv("DB_URL_STEWARD", os.getenv("DATABASE_URL"))
if not steward_db_url:
    raise ValueError("DB_URL_STEWARD must be set")

engine = create_async_engine(steward_db_url)
async_session = async_sessionmaker(engine, expire_on_commit=False)

def find_all_csvs(root_dir) -> dict[str, str]:
    # Returns mapping of physical_table_name (lowercase) to filepath
    csv_map = {}
    for path in Path(root_dir).rglob("*.csv"):
        tbl_name = path.stem.lower()
        csv_map[tbl_name] = str(path)
    return csv_map

async def populate_metadata():
    csv_map = find_all_csvs("./data/minidev/MINIDEV/dev_databases")

    async with async_session() as session:
        # Get the latest version that actually has tables tied to it
        from sqlalchemy import func
        subq = (
            select(MetadataTable.version_id)
            .group_by(MetadataTable.version_id)
            .having(func.count(MetadataTable.table_id) > 0)
        )

        result = await session.execute(
            select(MetadataVersion)
            .where(MetadataVersion.version_id.in_(subq))
            .order_by(MetadataVersion.created_at.desc())
        )
        version = result.scalars().first()
        if not version:
            print("No metadata versions found. Please load initial metadata first.")
            return

        print(f"Updating Registry Artifact {version.version_id} with Description Text...")

        result = await session.execute(
            select(MetadataTable)
            .where(MetadataTable.version_id == version.version_id)
            .options(selectinload(MetadataTable.columns))
        )
        tables = result.scalars().all()

        tables_updated_count = 0
        columns_updated_count = 0

        for table in tables:
            physical_table_name = table.real_name.lower()

            # Default table description to physical name
            table.description = physical_table_name
            tables_updated_count += 1

            # Prepare column descriptions map from CSV
            col_desc_map = {}
            if physical_table_name in csv_map:
                try:
                    # utf-8-sig to automatically handle any stray Byte Order Marks (BOM) in the flat files
                    with open(csv_map[physical_table_name], encoding='utf-8-sig') as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            # Strip all keys and values to ensure clean lookup
                            row = {
                                k.strip() if k else k: v.strip() if isinstance(v, str) else v
                                for k, v in row.items()
                            }

                            col_name_raw = row.get("original_column_name", "")
                            if not col_name_raw:
                                continue

                            col_name = col_name_raw.lower()
                            col_desc = row.get("column_description", "")

                            if col_desc:
                                col_desc_map[col_name] = col_desc
                except Exception as e:
                    print(f"Warning: Error parsing CSV {csv_map[physical_table_name]}: {e}")

            for col in table.columns:
                physical_col_name = col.real_name.lower()

                if physical_col_name in col_desc_map and col_desc_map[physical_col_name]:
                    col.description = col_desc_map[physical_col_name]
                else:
                    col.description = physical_col_name

                columns_updated_count += 1

        await session.commit()
        print(
            f"Success! Populated semantic descriptions for"
            f" {tables_updated_count} tables and {columns_updated_count} columns."
        )

if __name__ == "__main__":
    asyncio.run(populate_metadata())
