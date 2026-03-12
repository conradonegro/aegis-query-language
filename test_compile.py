import asyncio
import uuid

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.compiler import MetadataCompiler


async def main():
    db_url = "postgresql+asyncpg://user_aegis_steward:steward_pass@localhost:5432/aegis_data_warehouse"
    engine = create_async_engine(db_url)
    SessionLocal = async_sessionmaker(autocommit=False, autoflush=False, bind=engine)
    async with SessionLocal() as session:
        try:
            await MetadataCompiler.compile_version(
                session, uuid.UUID("ca65a834-b27c-4ad1-9845-f535bd4900e0"), "api_user"
            )
            print("Success")
        except Exception:
            import traceback
            traceback.print_exc()

asyncio.run(main())
