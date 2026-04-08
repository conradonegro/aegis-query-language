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
