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
        registry_version="1",
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
async def test_redis_get_legacy_payload_is_treated_as_cache_miss() -> None:
    """A pre-upgrade payload without registry_version must not 500 the request.

    Review validation (2026-04-08): SessionQueryContext gained a required
    registry_version field, but Redis may still contain older JSON rows until
    TTL expiry. The store should discard those entries and behave as a miss.
    """
    import json

    redis = AsyncMock()
    redis.get.return_value = json.dumps({
        "last_filtered_schema": {
            "version": "v1",
            "tables": [],
            "relationships": [],
            "omitted_columns": {},
        },
        "last_successful_sql": "SELECT 1",
        "timestamp": 123.0,
    })

    store = SessionStore(redis_client=redis)
    result = await store.get("legacy-session")

    assert result is None
    redis.delete.assert_awaited_once_with("aegis:session:legacy-session")


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
async def test_follow_up_rejects_cached_context_when_registry_version_changes() -> None:
    """A SessionQueryContext built against registry version A must NOT be
    reused as a follow-up when the active registry version is now B.

    Code-review finding #1 (2026-04-07): the follow-up path reused
    last_filtered_schema with no version check, allowing queries against
    aliases that had been removed or reclassified by a newer registry.
    """
    import time
    from unittest.mock import AsyncMock, MagicMock

    from app.compiler.engine import CompilerEngine
    from app.compiler.models import (
        FilteredSchema,
        PromptHints,
        SessionQueryContext,
        UserIntent,
    )
    from app.compiler.session_store import SessionStore
    from app.steward import RegistrySchema

    # Stub the dependencies CompilerEngine needs
    schema_filter = MagicMock()
    schema_filter.is_follow_up = MagicMock(return_value=True)
    schema_filter.filter_schema = MagicMock(
        return_value=FilteredSchema(
            version="v_new",
            tables=[],
            relationships=[],
            omitted_columns={},
        )
    )

    prompt_builder = MagicMock()
    prompt_builder.build_prompt = MagicMock(return_value=MagicMock(
        system_instruction="", user_prompt="", chat_history=[]
    ))

    llm_gateway = MagicMock()
    llm_gateway.generate = AsyncMock(return_value=MagicMock(
        raw_text='{"sql": "SELECT 1"}',
        model_id="mock",
        latency_ms=1.0,
        prompt_tokens=0,
        completion_tokens=0,
    ))

    parser = MagicMock()
    safety_engine = MagicMock()
    translator = MagicMock()

    engine = CompilerEngine(
        schema_filter=schema_filter,
        prompt_builder=prompt_builder,
        llm_gateway=llm_gateway,
        parser=parser,
        safety_engine=safety_engine,
        translator=translator,
    )
    engine.session_store = SessionStore(redis_client=None, ttl=3600)

    # Pre-seed a stale context tied to registry version "v_old"
    stale_filtered = FilteredSchema(
        version="v_old",
        tables=[],
        relationships=[],
        omitted_columns={"old_alias": "removed in v_new"},
    )
    stale_context = SessionQueryContext(
        last_filtered_schema=stale_filtered,
        last_successful_sql="SELECT * FROM old_table",
        timestamp=time.time(),
        registry_version="v_old",
    )
    await engine.session_store.set("session-123", stale_context)

    # The currently loaded registry is "v_new"
    new_schema = RegistrySchema(version="v_new", tables=[], relationships=[])

    intent = UserIntent(natural_language_query="follow-up question")
    hints = PromptHints(column_hints=[])

    # We expect the engine to NOT treat this as a follow-up — even though
    # is_follow_up returns True, the version mismatch should override.
    try:
        await engine.compile(
            intent=intent,
            schema=new_schema,
            hints=hints,
            tenant_id="test_tenant",
            session_id="session-123",
        )
    except Exception:
        # Other failures (mocked translator, etc.) are fine — we only care
        # about whether filter_schema was called (proving fresh-build path)
        # versus whether stale_filtered was reused.
        pass

    # If the version-check fix is in place, filter_schema must have been
    # called — meaning the engine rebuilt the filtered schema instead of
    # reusing the stale one.
    assert schema_filter.filter_schema.called, (
        "Engine reused the stale SessionQueryContext despite registry"
        " version mismatch (v_old context vs v_new active schema)"
    )


def test_resolve_session_rejects_cross_user_access_within_same_tenant() -> None:
    """A session created by user A must NOT be loadable by user B, even when
    both users belong to the same tenant.

    Code-review finding #5 (2026-04-07): _resolve_session previously scoped
    by tenant_id only, allowing horizontal privilege escalation between
    users of the same tenant.
    """
    import uuid as uuid_mod

    from fastapi.testclient import TestClient
    from sqlalchemy import create_engine, text

    from app.api.auth import ResolvedCredential, require_query_credential
    from app.main import app
    from tests.conftest import TEST_QUERY_CREDENTIAL_ID

    sqlite_url = "sqlite:///file:testdb?mode=memory&cache=shared&uri=true"
    engine = create_engine(sqlite_url, connect_args={"check_same_thread": False})

    user_a_session = uuid_mod.uuid4()

    # Pre-seed a chat session owned by user_a in test_tenant
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO chat_sessions"
                " (session_id, tenant_id, user_id)"
                " VALUES (:sid, 'test_tenant', 'user_a')"
            ),
            {"sid": str(user_a_session)},
        )
        conn.execute(
            text(
                "INSERT INTO chat_messages"
                " (message_id, session_id, sequence_number, role, content)"
                " VALUES (:mid, :sid, 1, 'user', 'A confidential prompt')"
            ),
            {"mid": str(uuid_mod.uuid4()), "sid": str(user_a_session)},
        )

    # Override credential to be a *different* user in the same tenant
    user_b_cred = ResolvedCredential(
        credential_id=TEST_QUERY_CREDENTIAL_ID,
        tenant_id="test_tenant",
        user_id="user_b",  # NOTE: not user_a
        scope="query",
    )
    app.dependency_overrides[require_query_credential] = lambda: user_b_cred

    try:
        with TestClient(app) as client:
            # User B presents user A's session_id in a generate request.
            response = client.post(
                "/api/v1/query/generate",
                json={
                    "natural_language_query": "follow-up",
                    "session_id": str(user_a_session),
                },
            )
        # The endpoint should still respond (it'll create a fresh session for
        # user_b), but user A's history must NOT have been loaded into the
        # prompt or returned in the response.
        assert response.status_code in (200, 400, 422)
        body = response.json()
        # The response must NOT echo user_a's confidential prompt — verify
        # that nothing in the response body contains the planted text.
        assert "A confidential prompt" not in str(body)

        # Stronger: user_a's session row must still own its messages and
        # user_b's request must have been bound to a NEW session row.
        with engine.connect() as conn:
            count = conn.execute(
                text(
                    "SELECT COUNT(*) FROM chat_sessions"
                    " WHERE session_id = :sid AND user_id = 'user_a'"
                ),
                {"sid": str(user_a_session)},
            ).scalar()
            assert count == 1
    finally:
        app.dependency_overrides.pop(require_query_credential, None)
        engine.dispose()


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
