"""Tests for app.reload._perform_reload retry semantics.

Code-review finding #3 (2026-04-07): both reload paths advanced
loaded_artifact_hashes before the RAG rebuild succeeded, so a failed
rebuild left the worker in a degraded state that subsequent polls
considered "up to date" and refused to retry.
"""
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.mark.asyncio
async def test_perform_reload_does_not_advance_hash_when_rag_rebuild_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If build_from_artifact() raises during _perform_reload, the tenant's
    loaded_artifact_hashes entry must NOT advance to the new hash. The next
    poll cycle must see a mismatch and retry the rebuild."""
    from app import reload as reload_mod

    # Build a fake app with the minimum state _perform_reload touches
    fake_app: Any = MagicMock()
    fake_app.state.loaded_artifact_hashes = {"tenant_a": "old_hash"}
    fake_app.state.registries = {}
    fake_app.state.vector_stores = {}
    fake_app.state.reload_locks = {}

    # Stub the artifact loader to return a fresh artifact for tenant_a.
    # _load_active_artifact_and_values is defined at module level in
    # app/reload.py so patching reload_mod.<name> intercepts the call.
    fake_artifact = MagicMock()
    fake_artifact.artifact_hash = "new_hash"
    fake_artifact.artifact_blob = {"tables": []}
    fake_artifact.version_id = "00000000-0000-0000-0000-000000000001"

    async def fake_load(_app: Any, _tenant: str) -> tuple[Any, dict[str, Any]]:
        return (fake_artifact, {})

    monkeypatch.setattr(
        reload_mod, "_load_active_artifact_and_values", fake_load
    )

    # Stub the schema loader at the SOURCE module. _perform_reload does
    # `from app.steward.loader import RegistryLoader` inside the function
    # body, so the function-local import resolves from app.steward.loader
    # at call time. Patching reload_mod.RegistryLoader would be a no-op.
    fake_schema = MagicMock()
    monkeypatch.setattr(
        "app.steward.loader.RegistryLoader.load_active_schema",
        AsyncMock(return_value=fake_schema),
    )

    # Stub registry_runtime_session_factory as a no-op async context manager
    class _NoopSession:
        async def __aenter__(self) -> Any:
            return self
        async def __aexit__(self, *_: Any) -> None:
            pass

    fake_app.state.registry_runtime_session_factory = _NoopSession

    # Force build_from_artifact to raise. Same reasoning as above:
    # `from app.rag.builder import ... build_from_artifact` is a function-
    # local import, so we patch app.rag.builder.build_from_artifact
    # directly at its source.
    monkeypatch.setattr(
        "app.rag.builder.build_from_artifact",
        AsyncMock(side_effect=RuntimeError("boom")),
    )

    # Run the reload
    await reload_mod._perform_reload(fake_app, "tenant_a")

    # The schema swap is allowed (it's idempotent) but the hash MUST stay at
    # the old value so the next poll retries.
    assert fake_app.state.loaded_artifact_hashes["tenant_a"] == "old_hash", (
        "loaded_artifact_hashes advanced to new_hash despite RAG failure — "
        "the next poll will skip this tenant and the RAG store stays stale"
    )


@pytest.mark.asyncio
async def test_rebuild_helper_and_perform_reload_serialize_via_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The compile-path RAG rebuild and a concurrent pub/sub echo path
    must serialize through app.state.reload_locks[tenant_id]. Whichever
    runs first advances loaded_artifact_hashes; the second path sees the
    hash match inside the lock and skips.

    This races the REAL extracted helper (_rebuild_rag_index_for_tenant)
    against the REAL _perform_reload, so the test will fail before the
    fix lands and pass after.

    Regression: reviewer's Open Question #1 after the Task 2.2 fix
    deferred the hash advance, breaking the original hash-match skip.
    """
    import asyncio
    import uuid

    from app import reload as reload_mod
    from app.api.router import _rebuild_rag_index_for_tenant

    # Counter for how many times build_from_artifact actually runs.
    build_calls = [0]

    async def counting_build(**_kwargs: Any) -> Any:
        build_calls[0] += 1
        # Sleep so the concurrent task has time to queue on the lock —
        # without this, the first task may finish entirely before the
        # second starts and the test wouldn't exercise the race window.
        await asyncio.sleep(0.05)
        return MagicMock()

    monkeypatch.setattr(
        "app.rag.builder.build_from_artifact", counting_build
    )

    # Fake app state shared by both paths.
    fake_app: Any = MagicMock()
    fake_app.state.loaded_artifact_hashes = {"tenant_a": "old_hash"}
    fake_app.state.registries = {}
    fake_app.state.vector_stores = {}
    fake_app.state.reload_locks = {}
    fake_app.state.compiler.set_vector_store = MagicMock()

    fake_artifact = MagicMock()
    fake_artifact.artifact_hash = "new_hash"
    fake_artifact.artifact_blob = {"tables": []}
    fake_artifact.version_id = uuid.UUID(
        "00000000-0000-0000-0000-000000000001"
    )

    # Stub _perform_reload's dependencies so it reaches build_from_artifact.
    async def fake_load(_app: Any, _tenant: str) -> tuple[Any, dict[str, Any]]:
        return (fake_artifact, {})

    monkeypatch.setattr(
        reload_mod, "_load_active_artifact_and_values", fake_load
    )
    monkeypatch.setattr(
        "app.steward.loader.RegistryLoader.load_active_schema",
        AsyncMock(return_value=MagicMock()),
    )

    class _NoopSession:
        async def __aenter__(self) -> Any:
            return self
        async def __aexit__(self, *_: Any) -> None:
            pass

    fake_app.state.registry_runtime_session_factory = _NoopSession

    # Race the compile-path helper against the pub/sub echo path.
    # asyncio.gather schedules both coroutines; the counting_build sleep
    # guarantees both reach the lock before either completes its rebuild.
    await asyncio.gather(
        _rebuild_rag_index_for_tenant(
            app=fake_app,
            tenant_id="tenant_a",
            artifact_blob=fake_artifact.artifact_blob,
            artifact_hash="new_hash",
            artifact_version_id=str(fake_artifact.version_id),
            column_values={},
        ),
        reload_mod._perform_reload(fake_app, "tenant_a"),
    )

    # Exactly ONE build_from_artifact call — whichever path acquired the
    # lock first did the rebuild, and the other saw the hash match inside
    # the lock and skipped.
    assert build_calls[0] == 1, (
        f"expected exactly 1 build_from_artifact call, got {build_calls[0]};"
        " the compile-path helper and pub/sub echo are not serializing"
        " on app.state.reload_locks[tenant_id]"
    )
    assert fake_app.state.loaded_artifact_hashes["tenant_a"] == "new_hash"
