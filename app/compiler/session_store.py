import logging
import os
from typing import Any

import redis.exceptions

from app.compiler.models import SessionQueryContext

logger = logging.getLogger(__name__)

_LOCAL_MAX = 10_000
_DEFAULT_TTL = 3600  # 1 hour

_IS_PRODUCTION = os.getenv("ENVIRONMENT") == "production"


class SessionStore:
    """
    Per-session compilation context store with TTL.

    Uses Redis when a client is provided (multi-worker safe, TTL enforced by the
    server). Falls back to an in-memory dict for local / single-worker deployments,
    with a simple size cap and oldest-entry eviction to prevent unbounded growth.
    """

    def __init__(
        self, redis_client: Any | None = None, ttl: int = _DEFAULT_TTL
    ) -> None:
        self._redis = redis_client
        self._ttl = ttl
        self._local: dict[str, SessionQueryContext] = {}

    @property
    def backend(self) -> str:
        return "redis" if self._redis is not None else "local"

    def _redis_error(self, exc: Exception) -> None:
        """Log a Redis failure. Re-raise in production; otherwise warn and fall back."""
        msg = (
            "Redis unreachable — falling back to in-memory session store. "
            "Multi-worker session continuity is broken. Error: %s"
        )
        if _IS_PRODUCTION:
            logger.error(msg, exc)
            raise
        logger.warning(msg, exc)

    async def get(self, session_id: str) -> SessionQueryContext | None:
        if self._redis is not None:
            try:
                raw = await self._redis.get(f"aegis:session:{session_id}")
            except redis.exceptions.RedisError as exc:
                self._redis_error(exc)
                return self._local.get(session_id)
            if raw is None:
                return None
            return SessionQueryContext.model_validate_json(raw)
        return self._local.get(session_id)

    async def set(self, session_id: str, context: SessionQueryContext) -> None:
        if self._redis is not None:
            try:
                await self._redis.setex(
                    f"aegis:session:{session_id}",
                    self._ttl,
                    context.model_dump_json(),
                )
            except redis.exceptions.RedisError as exc:
                self._redis_error(exc)
                # fall through to local store
            else:
                return
        if len(self._local) >= _LOCAL_MAX:
            oldest = min(self._local, key=lambda k: self._local[k].timestamp)
            del self._local[oldest]
        self._local[session_id] = context

    async def delete(self, session_id: str) -> None:
        if self._redis is not None:
            try:
                await self._redis.delete(f"aegis:session:{session_id}")
            except redis.exceptions.RedisError as exc:
                self._redis_error(exc)
                self._local.pop(session_id, None)
            return
        self._local.pop(session_id, None)

    async def close(self) -> None:
        if self._redis is not None:
            await self._redis.aclose()
