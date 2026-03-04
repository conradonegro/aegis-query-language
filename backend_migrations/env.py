import asyncio
import os
from logging.config import fileConfig

from sqlalchemy import pool
import sqlalchemy
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from alembic import context
from dotenv import load_dotenv

from sqlalchemy.engine.url import make_url

from app.api.meta_models import Base
from app.vault import get_secrets_manager

# Load environment variables
load_dotenv()

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Dynamically override the alembic.ini url with our securely fetched application context!
db_url = os.getenv("DATABASE_URL")
if not db_url:
    db_url = "postgresql+asyncpg://user_aegis_meta_owner:meta_owner_pass@localhost:5432/aegis_data_warehouse"
    os.environ["DATABASE_URL"] = db_url

secrets_mgr = get_secrets_manager()
url_obj = make_url(db_url)

if url_obj.get_dialect().name not in ["sqlite", "sqlite+aiosqlite"]:
    pwd = secrets_mgr.get_database_password("user_aegis_meta_owner")
    url_obj = url_obj.set(password=pwd)
    
    if os.getenv("ENVIRONMENT") == "production":
        new_query = dict(url_obj.query)
        new_query["ssl"] = "require"
        url_obj = url_obj.set(query=new_query)
        
secure_db_url = url_obj.render_as_string(hide_password=False)
config.set_main_option("sqlalchemy.url", secure_db_url)

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
target_metadata = Base.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        include_schemas=True,
        include_name=lambda name, type_, parent_names: (type_ == "schema" and name == "aegis_meta") or (parent_names and parent_names["schema_name"] == "aegis_meta"),
        version_table_schema="aegis_meta",
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """In this scenario we need to create an Engine
    and associate a connection with the context.

    """

    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""

    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
