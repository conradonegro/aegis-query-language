"""Populate source_database on metadata_tables for the BIRD benchmark.

Connects to the steward database, finds the latest MetadataVersion that has
tables, and bulk-updates source_database on each row whose real_name appears
in SOURCE_DATABASE_MAP.

Idempotent — re-running overwrites with the same values.
"""

import asyncio
import os

from dotenv import load_dotenv
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

load_dotenv()

from app.api.meta_models import MetadataTable, MetadataVersion  # noqa: E402

steward_db_url = os.getenv("DB_URL_STEWARD", os.getenv("DATABASE_URL"))
if not steward_db_url:
    raise ValueError("DB_URL_STEWARD must be set")

engine = create_async_engine(steward_db_url)
async_session = async_sessionmaker(engine, expire_on_commit=False)

# All keys are lowercased; matching is done via tbl.real_name.lower() so
# mixed-case physical table names (e.g. "Player", "lapTimes") are handled.
SOURCE_DATABASE_MAP: dict[str, str] = {
    # debit_card_specializing (5)
    "customers": "debit_card_specializing",
    "gasstations": "debit_card_specializing",
    "products": "debit_card_specializing",
    "yearmonth": "debit_card_specializing",
    "transactions_1k": "debit_card_specializing",
    # financial (8)
    "loan": "financial",
    "client": "financial",
    "district": "financial",
    "trans": "financial",
    "account": "financial",
    "card": "financial",
    "order": "financial",
    "disp": "financial",
    # formula_1 (13)
    "circuits": "formula_1",
    "status": "formula_1",
    "drivers": "formula_1",
    "driverstandings": "formula_1",
    "races": "formula_1",
    "constructors": "formula_1",
    "constructorresults": "formula_1",
    "laptimes": "formula_1",
    "qualifying": "formula_1",
    "pitstops": "formula_1",
    "seasons": "formula_1",
    "constructorstandings": "formula_1",
    "results": "formula_1",
    # california_schools (3)
    "schools": "california_schools",
    "satscores": "california_schools",
    "frpm": "california_schools",
    # card_games (6)
    "legalities": "card_games",
    "cards": "card_games",
    "rulings": "card_games",
    "set_translations": "card_games",
    "sets": "card_games",
    "foreign_data": "card_games",
    # european_football_2 (7)
    "team_attributes": "european_football_2",
    "player": "european_football_2",
    "match": "european_football_2",
    "league": "european_football_2",
    "country": "european_football_2",
    "player_attributes": "european_football_2",
    "team": "european_football_2",
    # thrombosis_prediction (3)
    "laboratory": "thrombosis_prediction",
    "patient": "thrombosis_prediction",
    "examination": "thrombosis_prediction",
    # toxicology (4)
    "bond": "toxicology",
    "molecule": "toxicology",
    "atom": "toxicology",
    "connected": "toxicology",
    # student_club (8)
    "income": "student_club",
    "budget": "student_club",
    "zip_code": "student_club",
    "expense": "student_club",
    "member": "student_club",
    "attendance": "student_club",
    "event": "student_club",
    "major": "student_club",
    # superhero (10)
    "gender": "superhero",
    "superpower": "superhero",
    "publisher": "superhero",
    "superhero": "superhero",
    "colour": "superhero",
    "attribute": "superhero",
    "hero_power": "superhero",
    "race": "superhero",
    "alignment": "superhero",
    "hero_attribute": "superhero",
    # codebase_community (8)
    "postlinks": "codebase_community",
    "posthistory": "codebase_community",
    "badges": "codebase_community",
    "posts": "codebase_community",
    "users": "codebase_community",
    "tags": "codebase_community",
    "votes": "codebase_community",
    "comments": "codebase_community",
}


async def populate() -> None:
    async with async_session() as session:
        # Find the latest version that has tables
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
            print("No metadata versions found — run discover_metadata.py first.")
            return

        print(f"Updating version {version.version_id} ...")

        rows = await session.execute(
            select(MetadataTable).where(
                MetadataTable.version_id == version.version_id
            )
        )
        tables = list(rows.scalars().all())

        updated = 0
        skipped = 0
        for tbl in tables:
            db_name = SOURCE_DATABASE_MAP.get(tbl.real_name.lower())
            if db_name:
                tbl.source_database = db_name
                updated += 1
            else:
                skipped += 1

        await session.commit()
        print(f"Done — updated: {updated}, skipped (no mapping): {skipped}")


if __name__ == "__main__":
    asyncio.run(populate())
