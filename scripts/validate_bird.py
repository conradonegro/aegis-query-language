import asyncio

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


async def validate_database_rows():
    print("=== Phase 4: Validating BIRD-SQL Import ===")

    # Connect directly to the aegis_data_warehouse instance
    target_db = "postgresql+asyncpg://postgres:postgrespassword@127.0.0.1:5432/aegis_data_warehouse"
    engine = create_async_engine(target_db, echo=False)

    try:
        async with engine.begin() as conn:
            # 1. Get all public tables
            print("Fetching public table schemas...")
            tables_query = text("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            """)
            result = await conn.execute(tables_query)
            tables = [row[0] for row in result.all()]

            print(f"Total Tables Detected in PostgreSQL: {len(tables)}\n")

            # 2. Extract Row Counts
            table_counts = {}
            for table in tables:
                count_query = text(f'SELECT count(*) FROM "{table}"')
                count_result = await conn.execute(count_query)
                table_counts[table] = count_result.scalar_one()

            # Quick summary report
            sorted_tables = sorted(table_counts.items())
            total_rows = sum(table_counts.values())

            print(f"Total Combined Rows in Container: {total_rows:,}\n")

            if len(sorted_tables) > 25:
                print("[Sample Inserted Tables]")
                for table, count in sorted_tables[:10]:
                    print(f" {table}: {count:,} rows")
                print(" ...")
                for table, count in sorted_tables[-5:]:
                    print(f" {table}: {count:,} rows")
            else:
                for table, count in sorted_tables:
                    print(f" {table}: {count:,} rows")

    except Exception as e:
        print(f"Validation Failed. Ensure the docker container is running. Error: {e}")
    finally:
        await engine.dispose()

if __name__ == "__main__":
    asyncio.run(validate_database_rows())
