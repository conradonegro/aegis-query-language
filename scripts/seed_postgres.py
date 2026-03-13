import asyncio
import logging
import os

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_DB = "postgresql+asyncpg://postgres:postgrespassword@localhost:5432/aegis"
DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_DB)

async def seed_database() -> None:
    """Seeds the Aegis proxy database with default Alice, Bob, and Charlie mock data."""
    logger.info(f"Connecting to database: {DATABASE_URL}")
    engine = create_async_engine(DATABASE_URL, echo=False)

    async with engine.begin() as conn:
        logger.info("Dropping existing tables if they exist...")
        await conn.execute(text("DROP TABLE IF EXISTS orders;"))
        await conn.execute(text("DROP TABLE IF EXISTS users;"))

        logger.info("Creating users table...")
        await conn.execute(text("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                active BOOLEAN,
                created_at TEXT
            )
        """))

        logger.info("Creating orders table...")
        await conn.execute(text("""
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                user_id INTEGER REFERENCES users(id),
                total_amount REAL
            )
        """))

        logger.info("Inserting mock user data...")
        await conn.execute(
            text("INSERT INTO users VALUES (1, 'Alice', true, '2025-01-01')")
        )
        await conn.execute(
            text("INSERT INTO users VALUES (2, 'Bob', true, '2025-01-02')")
        )
        await conn.execute(
            text("INSERT INTO users VALUES (3, 'Charlie', false, '2025-01-03')")
        )

        logger.info("Inserting mock order data...")
        await conn.execute(text("INSERT INTO orders VALUES (101, 1, 99.99)"))
        await conn.execute(text("INSERT INTO orders VALUES (102, 1, 150.00)"))
        await conn.execute(text("INSERT INTO orders VALUES (103, 2, 45.50)"))

    await engine.dispose()
    logger.info("Database seeding completed securely!")

if __name__ == "__main__":
    asyncio.run(seed_database())
