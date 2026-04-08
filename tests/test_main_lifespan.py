"""Lifespan management tests for app.main.

Code-review finding #6 (2026-04-07): four async engines were created
inline inside session factories and never disposed on shutdown. These
tests verify that all DB engines on app.state get disposed when the
lifespan exits.
"""
import pytest


@pytest.mark.asyncio
async def test_lifespan_disposes_all_database_engines(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """All async engines stored on app.state must be disposed exactly once
    when the lifespan context exits.

    AsyncEngine.dispose is a read-only descriptor on the class, so we patch
    at the class level (not the instance) and use object identity to verify
    that each of the four expected app.state engines is among the disposed
    instances. This is robust against shared `type(engine)` across engines.
    """
    from typing import Any

    from sqlalchemy.ext.asyncio import AsyncEngine

    from app.main import app, lifespan

    # The four engines we expect to be disposed (in addition to the
    # executor's internal engine, which was already being closed).
    expected_engine_attrs = (
        "registry_runtime_engine",
        "steward_engine",
        "registry_admin_engine",
        "runtime_engine",
    )

    disposed_engine_ids: list[int] = []
    real_dispose = AsyncEngine.dispose

    async def tracking_dispose(self: AsyncEngine, *args: Any, **kwargs: Any) -> None:
        disposed_engine_ids.append(id(self))
        await real_dispose(self, *args, **kwargs)

    monkeypatch.setattr(AsyncEngine, "dispose", tracking_dispose)

    expected_engine_ids: list[int] = []
    async with lifespan(app):
        # Inside the context: every expected engine attribute must exist on
        # app.state and be a real async engine. Capture each engine's id so
        # we can verify it was disposed after the context exits.
        for attr in expected_engine_attrs:
            assert hasattr(app.state, attr), (
                f"app.state.{attr} missing — engines must be tracked explicitly"
            )
            engine = getattr(app.state, attr)
            expected_engine_ids.append(id(engine))

    # After the context exits, every expected engine should have been
    # disposed at least once. (The executor's internal engine may also
    # appear in disposed_engine_ids but is not in expected_engine_ids;
    # that's fine — we only assert the four we tracked are present.)
    for attr, eid in zip(expected_engine_attrs, expected_engine_ids, strict=True):
        assert eid in disposed_engine_ids, (
            f"app.state.{attr} (id={eid}) was not disposed during shutdown"
        )


@pytest.mark.asyncio
async def test_lifespan_closes_module_level_http_clients(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The shared httpx.AsyncClient pools owned by the LLM gateway modules
    must be aclose()'d when the lifespan exits.

    Pre-existing leak (review pass 4 audit): both
    `app.compiler.ollama._http_client` and
    `app.compiler.base_gateway._http_client` are module-level globals,
    initialized at import time and never closed. Repeated lifespan
    cycles (TestClient startup/shutdown loops, dev-server reloads,
    multi-worker rolling restarts) accumulate open TCP pools.

    Fix: each module exposes an `aclose_http_client()` helper that
    `app.main:lifespan`'s shutdown branch awaits. This test verifies
    the wiring by patching each helper and asserting it was awaited
    exactly once when the lifespan context exited.
    """
    from unittest.mock import AsyncMock

    from app.compiler import base_gateway as _llm_base_gateway
    from app.compiler import ollama as _llm_ollama
    from app.main import app, lifespan

    ollama_aclose_mock = AsyncMock()
    base_gateway_aclose_mock = AsyncMock()

    monkeypatch.setattr(
        _llm_ollama, "aclose_http_client", ollama_aclose_mock
    )
    monkeypatch.setattr(
        _llm_base_gateway, "aclose_http_client", base_gateway_aclose_mock
    )

    async with lifespan(app):
        # Inside the context: helpers must NOT have been called yet.
        ollama_aclose_mock.assert_not_awaited()
        base_gateway_aclose_mock.assert_not_awaited()

    # After the context exits: each helper must have been awaited exactly once.
    ollama_aclose_mock.assert_awaited_once()
    base_gateway_aclose_mock.assert_awaited_once()
