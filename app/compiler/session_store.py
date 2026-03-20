import logging
import os
import time
from typing import Any

import redis.exceptions

from app.compiler.models import SessionQueryContext

logger = logging.getLogger(__name__)

_LOCAL_MAX = 10_000
_DEFAULT_TTL = 3600  # 1 hour
_DEGRADED_COOLDOWN = 30.0  # seconds between "Redis unreachable" warnings

_IS_PRODUCTION = os.getenv("ENVIRONMENT") == "production"


class SessionStore:
    """
    Per-session compilation context store with TTL.

    Uses Redis when a client is provided (multi-worker safe, TTL enforced by the
    server). Falls back to an in-memory dict for local / single-worker deployments,
    with a simple size cap and oldest-entry eviction to prevent unbounded growth.

    Circuit breaker (non-production only): after a Redis failure, Redis is skipped
    for _DEGRADED_COOLDOWN seconds to avoid hammering an unavailable server and
    suppress repeated log noise. After the cooldown, the next operation probes Redis
    again; success closes the circuit and logs a recovery message.

    In production, failures are re-raised immediately — no silent fallback.
    """

    def __init__(
        self, redis_client: Any | None = None, ttl: int = _DEFAULT_TTL
    ) -> None:
        self._redis = redis_client
        self._ttl = ttl
        self._local: dict[str, SessionQueryContext] = {}
        self._degraded_until: float = 0.0  # monotonic; 0 = healthy

    @property
    def backend(self) -> str:
        if self._redis is None:
            return "local"
        if self._degraded_until > 0.0:
            return "redis-degraded"
        return "redis"

    def _circuit_open(self) -> bool:
        """True when the circuit breaker is open (Redis should be skipped).

        Only meaningful in non-production; always returns False in production so
        Redis is never silently bypassed in prod environments.
        """
        now = time.monotonic()
        degraded = self._degraded_until > 0.0 and now < self._degraded_until
        return not _IS_PRODUCTION and degraded

    def _redis_error(self, exc: Exception) -> None:
        """Handle a Redis operation failure.

        Production: logs error and re-raises — no silent fallback.
        Non-production: opens the circuit breaker on the first failure in a
        cooldown window; subsequent failures within the window are silent.
        """
        if _IS_PRODUCTION:
            logger.error(
                "Redis unreachable — session store unavailable. Error: %s", exc
            )
            raise
        now = time.monotonic()
        if now >= self._degraded_until:
            # First failure (or first after cooldown expired): log and open circuit.
            logger.warning(
                "Redis unreachable — falling back to in-memory session store. "
                "Multi-worker session continuity is broken. Error: %s",
                exc,
            )
            self._degraded_until = now + _DEGRADED_COOLDOWN

    def _redis_ok(self) -> None:
        """Called on a successful Redis operation; closes the circuit if it was open."""
        if self._degraded_until > 0.0:
            logger.info("Redis recovered — resuming Redis-backed session store.")
            self._degraded_until = 0.0

    async def get(self, session_id: str) -> SessionQueryContext | None:
        if self._redis is not None and not self._circuit_open():
            try:
                raw = await self._redis.get(f"aegis:session:{session_id}")
                self._redis_ok()
            except redis.exceptions.RedisError as exc:
                self._redis_error(exc)
                return self._local.get(session_id)
            if raw is None:
                return None
            return SessionQueryContext.model_validate_json(raw)
        return self._local.get(session_id)

    async def set(self, session_id: str, context: SessionQueryContext) -> None:
        if self._redis is not None and not self._circuit_open():
            try:
                await self._redis.setex(
                    f"aegis:session:{session_id}",
                    self._ttl,
                    context.model_dump_json(),
                )
                self._redis_ok()
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
        if self._redis is not None and not self._circuit_open():
            try:
                await self._redis.delete(f"aegis:session:{session_id}")
                self._redis_ok()
            except redis.exceptions.RedisError as exc:
                self._redis_error(exc)
                self._local.pop(session_id, None)
            return
        self._local.pop(session_id, None)

    async def close(self) -> None:
        if self._redis is not None:
            await self._redis.aclose()
