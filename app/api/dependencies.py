from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession
from starlette.requests import Request


async def get_registry_runtime_db_session(
    request: Request,
) -> AsyncGenerator[AsyncSession, None]:
    async with request.app.state.registry_runtime_session_factory() as session:
        yield session


async def get_registry_admin_db_session(
    request: Request,
) -> AsyncGenerator[AsyncSession, None]:
    async with request.app.state.registry_admin_session_factory() as session:
        yield session


async def get_runtime_db_session(
    request: Request,
) -> AsyncGenerator[AsyncSession, None]:
    async with request.app.state.runtime_session_factory() as session:
        yield session


async def get_steward_db_session(
    request: Request,
) -> AsyncGenerator[AsyncSession, None]:
    async with request.app.state.steward_session_factory() as session:
        yield session
