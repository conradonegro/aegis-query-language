import asyncio
import os
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from dotenv import load_dotenv

load_dotenv()
from app.api.meta_models import MetadataVersion, MetadataTable

steward_db_url = os.getenv("DB_URL_STEWARD", os.getenv("DATABASE_URL"))
engine = create_async_engine(steward_db_url)
async_session = async_sessionmaker(engine, expire_on_commit=False)

async def check():
    async with async_session() as session:
        # Get all versions
        res = await session.execute(select(MetadataVersion).order_by(MetadataVersion.created_at.desc()))
        versions = res.scalars().all()
        for v in versions:
            count = await session.execute(select(func.count()).select_from(MetadataTable).where(MetadataTable.version_id == v.version_id))
            tbl_count = count.scalar()
            print(f"Version {v.version_id} ({v.status}) -> {tbl_count} tables")

asyncio.run(check())
