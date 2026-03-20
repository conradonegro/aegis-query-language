"""
Tests for app/compiler/session_store.py

Covers:
- Local (in-memory) backend: get/set/delete/close, backend property
- Eviction when at _LOCAL_MAX capacity
- Redis backend: setex, get, delete, close
- Circuit breaker: failure opens circuit, log suppression, recovery
"""
import logging
import time
from unittest.mock import AsyncMock, MagicMock

import pytest
from redis.exceptions import ConnectionError as RedisConnError

import app.compiler.session_store as ss_mod
from app.compiler.models import FilteredSchema, SessionQueryContext
from app.compiler.session_store import _DEGRADED_COOLDOWN, _LOCAL_MAX, SessionStore


def _ctx(sql: str = "SELECT 1", ts: float | None = None) -> SessionQueryContext:
    return SessionQueryContext(
        last_filtered_schema=FilteredSchema(
            version="1", tables=[], relationships=[], omitted_columns={}
        ),
        last_successful_sql=sql,
        timestamp=ts if ts is not None else time.time(),
    )


# ─── Local backend ────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_local_backend_property() -> None:
    assert SessionStore().backend == "local"


@pytest.mark.asyncio
async def test_local_set_and_get() -> None:
    store = SessionStore()
    await store.set("s1", _ctx("SELECT * FROM users"))
    result = await store.get("s1")
    assert result is not None
    assert result.last_successful_sql == "SELECT * FROM users"


@pytest.mark.asyncio
async def test_local_get_missing_returns_none() -> None:
    assert await SessionStore().get("nonexistent") is None


@pytest.mark.asyncio
async def test_local_delete_removes_entry() -> None:
    store = SessionStore()
    await store.set("s1", _ctx())
    await store.delete("s1")
    assert await store.get("s1") is None


@pytest.mark.asyncio
async def test_local_delete_nonexistent_is_noop() -> None:
    # Must not raise
    await SessionStore().delete("nonexistent")


@pytest.mark.asyncio
async def test_local_overwrite_updates_value() -> None:
    store = SessionStore()
    await store.set("s1", _ctx("SELECT 1"))
    await store.set("s1", _ctx("SELECT 2"))
    result = await store.get("s1")
    assert result is not None
    assert result.last_successful_sql == "SELECT 2"


@pytest.mark.asyncio
async def test_local_eviction_removes_oldest_at_capacity() -> None:
    store = SessionStore()
    # Fill to capacity; timestamps increase so s0 is the oldest
    for i in range(_LOCAL_MAX):
        await store.set(f"s{i}", _ctx(ts=float(i)))

    assert len(store._local) == _LOCAL_MAX

    # One more entry should evict the oldest (ts=0.0 → "s0")
    await store.set("overflow", _ctx(ts=float(_LOCAL_MAX + 1)))

    assert len(store._local) == _LOCAL_MAX
    assert await store.get("s0") is None         # oldest evicted
    assert await store.get("overflow") is not None  # new entry present


@pytest.mark.asyncio
async def test_local_close_is_noop() -> None:
    # Must not raise when there is no Redis client
    await SessionStore().close()


# ─── Redis backend ────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_redis_backend_property() -> None:
    store = SessionStore(redis_client=MagicMock())
    assert store.backend == "redis"


@pytest.mark.asyncio
async def test_redis_set_calls_setex_with_correct_key_and_ttl() -> None:
    redis = AsyncMock()
    store = SessionStore(redis_client=redis, ttl=90)
    await store.set("mysession", _ctx("SELECT 1"))

    redis.setex.assert_called_once()
    key, ttl, _ = redis.setex.call_args[0]
    assert key == "aegis:session:mysession"
    assert ttl == 90


@pytest.mark.asyncio
async def test_redis_get_hit_deserialises_correctly() -> None:
    redis = AsyncMock()
    ctx = _ctx("SELECT 42")
    redis.get.return_value = ctx.model_dump_json()

    store = SessionStore(redis_client=redis)
    result = await store.get("mysession")

    assert result is not None
    assert result.last_successful_sql == "SELECT 42"
    redis.get.assert_called_once_with("aegis:session:mysession")


@pytest.mark.asyncio
async def test_redis_get_miss_returns_none() -> None:
    redis = AsyncMock()
    redis.get.return_value = None
    assert await SessionStore(redis_client=redis).get("missing") is None


@pytest.mark.asyncio
async def test_redis_delete_calls_delete() -> None:
    redis = AsyncMock()
    await SessionStore(redis_client=redis).delete("mysession")
    redis.delete.assert_called_once_with("aegis:session:mysession")


@pytest.mark.asyncio
async def test_redis_close_calls_aclose() -> None:
    redis = AsyncMock()
    await SessionStore(redis_client=redis).close()
    redis.aclose.assert_called_once()


# ─── Circuit breaker ──────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_circuit_breaker_opens_on_failure_and_suppresses_repeated_logs(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """First Redis failure opens the circuit and logs once; subsequent calls
    within the cooldown window use local silently (no extra warnings)."""
    fake_now = [0.0]
    fake_time = MagicMock()
    fake_time.monotonic = lambda: fake_now[0]
    monkeypatch.setattr(ss_mod, "time", fake_time)

    redis_mock = AsyncMock()
    redis_mock.get.side_effect = RedisConnError("down")
    store = SessionStore(redis_client=redis_mock)

    with caplog.at_level(logging.WARNING, logger="app.compiler.session_store"):
        await store.get("s1")  # First failure — opens circuit, logs warning
        n_after_first = sum(1 for r in caplog.records if r.levelno == logging.WARNING)

        await store.get("s1")  # Within cooldown — suppressed
        n_after_second = sum(1 for r in caplog.records if r.levelno == logging.WARNING)

    assert n_after_first == 1
    assert n_after_second == 1  # No additional log emitted
    assert store.backend == "redis-degraded"


@pytest.mark.asyncio
async def test_circuit_breaker_recovers_after_cooldown(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """After the cooldown window expires the store retries Redis; on success it
    logs 'Redis recovered', resets _degraded_until, and backend returns 'redis'."""
    fake_now = [0.0]
    fake_time = MagicMock()
    fake_time.monotonic = lambda: fake_now[0]
    monkeypatch.setattr(ss_mod, "time", fake_time)

    redis_mock = AsyncMock()
    redis_mock.get.side_effect = RedisConnError("down")
    store = SessionStore(redis_client=redis_mock)

    await store.get("s1")  # Opens circuit
    assert store.backend == "redis-degraded"

    # Advance clock past the cooldown; Redis is now healthy
    fake_now[0] = _DEGRADED_COOLDOWN + 1.0
    redis_mock.get.side_effect = None
    redis_mock.get.return_value = None  # Cache miss, but Redis is up

    with caplog.at_level(logging.INFO, logger="app.compiler.session_store"):
        await store.get("s1")  # Should probe Redis, succeed, close circuit

    assert store.backend == "redis"
    assert store._degraded_until == 0.0
    assert any("recovered" in r.getMessage().lower() for r in caplog.records)
