import asyncio
import os

from dotenv import load_dotenv
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

load_dotenv()
from app.api.meta_models import MetadataTable, MetadataVersion  # noqa: E402

steward_db_url = os.getenv("DB_URL_STEWARD", os.getenv("DATABASE_URL"))
engine = create_async_engine(steward_db_url)
async_session = async_sessionmaker(engine, expire_on_commit=False)

async def check():
    async with async_session() as session:
        # Get all versions
        res = await session.execute(select(MetadataVersion).order_by(MetadataVersion.created_at.desc()))
        versions = res.scalars().all()
        for v in versions:
            count = await session.execute(
                select(func.count()).select_from(MetadataTable).where(MetadataTable.version_id == v.version_id)
            )
            tbl_count = count.scalar()
            print(f"Version {v.version_id} ({v.status}) -> {tbl_count} tables")

asyncio.run(check())
