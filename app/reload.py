"""Cross-worker schema hot-reload via Redis pub/sub + periodic DB polling.

Architecture
------------
* compile endpoint  →  publish_reload()  →  Redis channel
* each worker       →  _listen_loop()    →  _perform_reload()  (fast path)
* each worker       →  _poll_loop()      →  _perform_reload()  (missed-signal net)

Design decisions
----------------
* publish_reload() always publishes regardless of local state. The publishing
  worker's own listener will receive the echo and skip via the idempotency
  check inside _perform_reload().
* _perform_reload() reads the active artifact from DB (source of truth),
  never from the pub/sub payload or Redis, so stale signals cannot poison state.
* A per-tenant asyncio.Lock prevents concurrent reload tasks from racing and
  interleaving schema + RAG swaps for the same tenant.
* The old RAG vector store is kept live until the rebuild succeeds; on failure
  the old store is retained and loaded_artifact_hashes is NOT updated.
* A dedicated Redis client is used for pub/sub so it cannot block the shared
  client used for session storage.
"""

import asyncio
import json
import logging
from typing import Any

import redis.asyncio as aioredis
import redis.exceptions

logger = logging.getLogger(__name__)

RELOAD_CHANNEL = "aegis:registry:reload"
_RECONNECT_BACKOFF_MAX = 60.0  # seconds
_POLL_INTERVAL = 30.0  # seconds


# ---------------------------------------------------------------------------
# Publishing
# ---------------------------------------------------------------------------

async def publish_reload(
    redis_client: Any | None,
    tenant_id: str,
    artifact_hash: str,
) -> None:
    """Publish a reload signal to all workers after a successful compile.

    Always called regardless of local state — other workers need the signal.
    The calling worker's own listener will receive the echo and skip it via
    the idempotency check in _perform_reload().
    """
    if redis_client is None:
        return
    payload = json.dumps({"tenant_id": tenant_id, "artifact_hash": artifact_hash})
    try:
        await redis_client.publish(RELOAD_CHANNEL, payload)
        logger.info(
            "reload: published signal for tenant %s artifact %s",
            tenant_id, artifact_hash[:12],
        )
    except redis.exceptions.RedisError as exc:
        logger.warning(
            "reload: publish failed (%s) — other workers will catch up via DB poll",
            exc,
        )


# ---------------------------------------------------------------------------
# Shared DB loader (source of truth)
# ---------------------------------------------------------------------------

async def _load_active_artifact_and_values(
    app: Any,
    tenant_id: str,
) -> "tuple[Any, dict[str, list[str]]] | None":
    """Load the active artifact and its RAG column values from DB.

    Returns (artifact, column_values) or None if no active artifact exists.
    Always reads from DB — never from the pub/sub payload or Redis keys.
    """
    from sqlalchemy import select

    from app.api.meta_models import (
        CompiledRegistryArtifact,
        MetadataColumnValue,
        MetadataVersion,
    )

    async with app.state.registry_admin_session_factory() as session:
        artifact_stmt = (
            select(CompiledRegistryArtifact)
            .join(
                MetadataVersion,
                CompiledRegistryArtifact.version_id == MetadataVersion.version_id,
            )
            .where(
                MetadataVersion.tenant_id == tenant_id,
                MetadataVersion.status == "active",
            )
            .order_by(CompiledRegistryArtifact.compiled_at.desc())
            .limit(1)
        )
        artifact = (await session.execute(artifact_stmt)).scalar_one_or_none()
        if artifact is None:
            return None

        val_stmt = (
            select(MetadataColumnValue.column_id, MetadataColumnValue.value)
            .where(
                MetadataColumnValue.active.is_(True),
                MetadataColumnValue.version_id == artifact.version_id,
            )
            .order_by(MetadataColumnValue.column_id, MetadataColumnValue.value)
        )
        groups: dict[str, list[str]] = {}
        for col_id, value in (await session.execute(val_stmt)).all():
            groups.setdefault(str(col_id), []).append(value)

        return artifact, groups


# ---------------------------------------------------------------------------
# Reload executor
# ---------------------------------------------------------------------------

async def _perform_reload(app: Any, tenant_id: str) -> None:
    """Reload schema and RAG for one tenant. Idempotent and concurrency-safe.

    Reads the active artifact from DB so the source of truth is always the
    database. loaded_artifact_hashes is only updated on full success; on any
    failure the old state is preserved.
    """
    from app.rag.builder import RagDivergenceError, build_from_artifact
    from app.steward.loader import RegistryLoader

    # Per-tenant lock: prevents concurrent reload tasks from racing.
    locks: dict[str, asyncio.Lock] = app.state.reload_locks
    if tenant_id not in locks:
        locks[tenant_id] = asyncio.Lock()

    async with locks[tenant_id]:
        result = await _load_active_artifact_and_values(app, tenant_id)
        if result is None:
            logger.warning(
                "reload: no active artifact for tenant %s — skipping", tenant_id
            )
            return

        artifact, column_values = result
        db_hash = artifact.artifact_hash

        # Idempotency: skip if this worker already has this artifact loaded.
        # This handles the publishing worker's own pub/sub echo.
        if app.state.loaded_artifact_hashes.get(tenant_id) == db_hash:
            logger.debug(
                "reload: tenant %s already at artifact %s — skipping",
                tenant_id, db_hash[:12],
            )
            return

        # --- Schema reload ---
        try:
            async with app.state.registry_runtime_session_factory() as rt_session:
                schema = await RegistryLoader.load_active_schema(rt_session, tenant_id)
            if schema is None:
                logger.warning(
                    "reload: no schema for tenant %s — skipping", tenant_id
                )
                return
            app.state.registries[tenant_id] = schema
        except Exception:
            logger.exception(
                "reload: schema reload failed for tenant %s — state unchanged",
                tenant_id,
            )
            return  # Do NOT update loaded_artifact_hashes on failure

        # Schema is in place. RAG rebuild is next; loaded_artifact_hashes
        # is intentionally NOT advanced yet — if the RAG rebuild fails, the
        # next poll cycle must see a hash mismatch and retry. Schema reload
        # is idempotent so re-running it on the next poll is cheap.
        logger.info(
            "reload: tenant %s schema swapped to artifact %s",
            tenant_id, db_hash[:12],
        )

        # --- RAG rebuild (keeps old store on failure) ---
        try:
            new_store = await build_from_artifact(
                artifact_blob=artifact.artifact_blob,
                version_id=str(artifact.version_id),
                tenant_id=tenant_id,
                artifact_version=db_hash,
                column_values=column_values,
            )
            app.state.vector_stores[tenant_id] = new_store
            app.state.compiler.set_vector_store(new_store, tenant_id)
        except RagDivergenceError:
            logger.warning(
                "reload: RAG divergence for tenant %s — old index retained,"
                " hash NOT advanced (will retry)",
                tenant_id,
            )
            return
        except Exception:
            logger.exception(
                "reload: RAG rebuild failed for tenant %s — old index"
                " retained, hash NOT advanced (will retry)",
                tenant_id,
            )
            return

        # Both schema and RAG are now live for this artifact. Advance the
        # loaded hash so the next poll skips this tenant until the next
        # genuine version change.
        app.state.loaded_artifact_hashes[tenant_id] = db_hash
        logger.info(
            "reload: tenant %s fully reloaded to artifact %s",
            tenant_id, db_hash[:12],
        )


# ---------------------------------------------------------------------------
# Pub/sub listener
# ---------------------------------------------------------------------------

def _parse_reload_message(raw: bytes | str) -> str | None:
    """Parse a pub/sub payload. Returns tenant_id or None on invalid payload."""
    try:
        data = json.loads(raw)
        tenant_id = str(data["tenant_id"])
        _ = str(data["artifact_hash"])  # validate field exists
        return tenant_id
    except (json.JSONDecodeError, KeyError, TypeError) as exc:
        logger.warning("reload: invalid pub/sub payload (%s) — skipped", exc)
        return None


async def _subscribe_and_dispatch(app: Any, redis_url: str) -> None:
    """Open one pub/sub connection and dispatch messages until it drops."""
    pubsub_client = aioredis.from_url(redis_url)
    try:
        pubsub = pubsub_client.pubsub()
        await pubsub.subscribe(RELOAD_CHANNEL)
        logger.info("reload: subscribed to channel %s", RELOAD_CHANNEL)
        async for message in pubsub.listen():
            if message["type"] != "message":
                continue
            tenant_id = _parse_reload_message(message["data"])
            if tenant_id is not None:
                asyncio.create_task(
                    _perform_reload(app, tenant_id),
                    name=f"reload-{tenant_id[:8]}",
                )
    finally:
        try:
            await pubsub_client.aclose()
        except Exception:
            pass


async def _listen_loop(app: Any, redis_url: str) -> None:
    """Reconnecting pub/sub loop. Reconnects with exponential backoff on errors.

    Uses a dedicated Redis client so pub/sub blocking never starves the shared
    client used by SessionStore.
    """
    backoff = 1.0
    while True:
        try:
            await _subscribe_and_dispatch(app, redis_url)
            backoff = 1.0
        except asyncio.CancelledError:
            return
        except redis.exceptions.RedisError as exc:
            logger.warning(
                "reload: pub/sub error (%s) — reconnecting in %.0fs", exc, backoff
            )
        except Exception as exc:
            logger.warning(
                "reload: listener error (%s) — reconnecting in %.0fs", exc, backoff
            )
        try:
            await asyncio.sleep(backoff)
        except asyncio.CancelledError:
            return
        backoff = min(backoff * 2, _RECONNECT_BACKOFF_MAX)


# ---------------------------------------------------------------------------
# DB polling safety net
# ---------------------------------------------------------------------------

async def _poll_loop(app: Any) -> None:
    """Periodic DB poll to catch reloads missed due to pub/sub drops.

    Runs a batched query for all known tenants' active artifact hashes and
    triggers _perform_reload for any that have drifted from the loaded state.
    The DB is the source of truth — Redis keys are not consulted here.
    """
    from sqlalchemy import select

    from app.api.meta_models import CompiledRegistryArtifact, MetadataVersion

    while True:
        try:
            await asyncio.sleep(_POLL_INTERVAL)
        except asyncio.CancelledError:
            return

        try:
            known_tenants = list(app.state.loaded_artifact_hashes.keys())
            if not known_tenants:
                continue

            async with app.state.registry_admin_session_factory() as session:
                stmt = (
                    select(
                        MetadataVersion.tenant_id,
                        CompiledRegistryArtifact.artifact_hash,
                    )
                    .join(
                        CompiledRegistryArtifact,
                        CompiledRegistryArtifact.version_id
                        == MetadataVersion.version_id,
                    )
                    .where(
                        MetadataVersion.tenant_id.in_(known_tenants),
                        MetadataVersion.status == "active",
                    )
                    .distinct(MetadataVersion.tenant_id)
                    .order_by(
                        MetadataVersion.tenant_id,
                        CompiledRegistryArtifact.compiled_at.desc(),
                    )
                )
                db_hashes = dict((await session.execute(stmt)).all())

            for tenant_id, db_hash in db_hashes.items():
                current = app.state.loaded_artifact_hashes.get(tenant_id)
                if current != db_hash:
                    logger.info(
                        "reload: poll drift for tenant %s (loaded=%s, db=%s)",
                        tenant_id,
                        (current or "none")[:12],
                        db_hash[:12],
                    )
                    asyncio.create_task(
                        _perform_reload(app, tenant_id),
                        name=f"reload-poll-{tenant_id[:8]}",
                    )

        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("reload: poll cycle failed — will retry next interval")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def start_reload_listener(
    app: Any, redis_url: str
) -> "list[asyncio.Task[None]]":
    """Spawn the pub/sub listener and DB poller. Returns tasks for lifespan cleanup."""
    listen_task: asyncio.Task[None] = asyncio.create_task(
        _listen_loop(app, redis_url), name="reload-listener"
    )
    poll_task: asyncio.Task[None] = asyncio.create_task(
        _poll_loop(app), name="reload-poller"
    )
    logger.info("reload: cross-worker hot-reload listener started")
    return [listen_task, poll_task]
