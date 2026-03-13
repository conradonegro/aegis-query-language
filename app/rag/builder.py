"""Deterministic RAG index builder.

Builds an InMemoryVectorStore from a compiled registry artifact and
pre-fetched metadata_column_values rows. The artifact is the source of truth:
- Only rag_enabled columns are indexed
- is_sensitive=True columns are always skipped (even if rag_enabled=True)
- rag_cardinality_hint="high" columns are skipped
- Values are fetched from metadata_column_values by the caller and passed in
- A SHA256 hash of the sorted values is stored in the artifact and verified
  here to catch divergence between the artifact and the DB

Multi-process note: Each worker process builds its own in-memory index at
startup via the lifespan boot hook. Atomic swap is asyncio-safe (single-
threaded cooperative scheduling). On worker restart, the index is rebuilt from
the same artifact, so the index is deterministic across workers.
"""

import asyncio
import hashlib
import json
import logging
from typing import Any

from app.rag.models import CategoricalValue
from app.rag.normalizer import normalize
from app.rag.store import InMemoryVectorStore

logger = logging.getLogger(__name__)

_DEFAULT_RAG_LIMIT = 100
_LIMIT_BY_CARDINALITY: dict[str | None, int] = {
    "low": 500,
    "medium": _DEFAULT_RAG_LIMIT,
    None: _DEFAULT_RAG_LIMIT,
}

# Per-version_id build locks prevent two concurrent compilations for the
# same version from racing.
_build_locks: dict[str, asyncio.Lock] = {}
_build_locks_mutex = asyncio.Lock()


async def _get_build_lock(version_id: str) -> asyncio.Lock:
    async with _build_locks_mutex:
        if version_id not in _build_locks:
            _build_locks[version_id] = asyncio.Lock()
        return _build_locks[version_id]


class RagDivergenceError(Exception):
    """Raised when DB values do not match the hash recorded in the artifact."""


def _compute_values_hash(values: list[str]) -> str:
    """SHA256 of sorted, JSON-encoded normalized values list."""
    sorted_vals = sorted(values)
    canonical = json.dumps(
        sorted_vals, ensure_ascii=False, separators=(",", ":")
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _resolve_limit(col_dict: dict[str, Any]) -> int:
    per_col = col_dict.get("rag_limit")
    if isinstance(per_col, int) and per_col > 0:
        return per_col
    hint = col_dict.get("rag_cardinality_hint")
    return _LIMIT_BY_CARDINALITY.get(hint, _DEFAULT_RAG_LIMIT)


async def build_from_artifact(
    artifact_blob: dict[str, Any],
    version_id: str,
    tenant_id: str,
    artifact_version: str,
    column_values: dict[str, list[str]],
) -> InMemoryVectorStore:
    """Build a populated InMemoryVectorStore.

    column_values: pre-fetched active values per column_id string,
                   fetched by the caller from metadata_column_values.

    Acquires a per-version_id lock so concurrent hot-reloads for the same
    version serialize rather than race.

    Raises RagDivergenceError if DB values do not match artifact hash.
    """
    lock = await _get_build_lock(version_id)
    async with lock:
        return _build_inner(
            artifact_blob, tenant_id, artifact_version, column_values
        )


class _IndexStats:
    """Mutable counters threaded through the build pass."""

    def __init__(self) -> None:
        self.indexed = 0
        self.skipped_sensitive = 0
        self.skipped_cardinality = 0
        self.skipped_not_enabled = 0


def _index_column(
    store: InMemoryVectorStore,
    col_dict: dict[str, Any],
    abstract_col: str,
    table_tenant: str,
    artifact_version: str,
    column_values: dict[str, list[str]],
    stats: _IndexStats,
) -> None:
    """Index one column's values into the store.

    Mutates *store* and *stats* in place.
    Raises RagDivergenceError if the hash check fails.
    """
    if not col_dict.get("rag_enabled", False):
        stats.skipped_not_enabled += 1
        return

    if col_dict.get("is_sensitive", False):
        stats.skipped_sensitive += 1
        logger.warning(
            "RAG: skipping %s — is_sensitive=True overrides rag_enabled",
            abstract_col,
        )
        return

    if col_dict.get("rag_cardinality_hint") == "high":
        stats.skipped_cardinality += 1
        logger.warning(
            "RAG: skipping %s — rag_cardinality_hint=high", abstract_col
        )
        return

    col_id_str: str = col_dict.get("id", "")
    expected_hash: str = col_dict.get("rag_values_hash", "")
    limit = _resolve_limit(col_dict)
    db_values = column_values.get(col_id_str, [])[:limit]

    if expected_hash:
        actual_hash = _compute_values_hash(db_values)
        if actual_hash != expected_hash:
            raise RagDivergenceError(
                f"RAG divergence for {abstract_col}: "
                f"artifact hash {expected_hash!r} != "
                f"DB hash {actual_hash!r}. "
                f"Re-compile the artifact after editing values."
            )

    seen_normalized: set[str] = set()
    for raw_val in db_values:
        try:
            norm = normalize(raw_val)
        except ValueError as exc:
            logger.warning(
                "RAG: value skipped for %s — %s", abstract_col, exc
            )
            continue

        if norm is None:
            continue
        norm_lower = norm.lower()
        if norm_lower in seen_normalized:
            continue
        seen_normalized.add(norm_lower)

        store.index_value(
            CategoricalValue(
                value=raw_val,
                abstract_column=abstract_col,
                tenant_id=table_tenant,
                artifact_version=artifact_version,
            )
        )
        stats.indexed += 1


def _build_inner(
    artifact_blob: dict[str, Any],
    tenant_id: str,
    artifact_version: str,
    column_values: dict[str, list[str]],
) -> InMemoryVectorStore:
    """Synchronous inner builder — no DB access."""
    store = InMemoryVectorStore()
    stats = _IndexStats()

    for tbl_dict in artifact_blob.get("tables", []):
        table_alias: str = tbl_dict.get("alias", "")
        table_tenant: str = tbl_dict.get("tenant_id", tenant_id)

        for col_dict in tbl_dict.get("columns", []):
            col_alias: str = col_dict.get("alias", "")
            abstract_col = f"{table_alias}.{col_alias}"
            _index_column(
                store,
                col_dict,
                abstract_col,
                table_tenant,
                artifact_version,
                column_values,
                stats,
            )

    store.set_artifact_version(artifact_version)
    logger.info(
        "RAG index built: version=%s tenant=%s indexed=%d "
        "skipped(sensitive=%d cardinality=%d not_enabled=%d)",
        artifact_version[:12],
        tenant_id,
        stats.indexed,
        stats.skipped_sensitive,
        stats.skipped_cardinality,
        stats.skipped_not_enabled,
    )
    return store


def build_test_store(
    entries: list[tuple[str, str, str]] | None = None,
) -> InMemoryVectorStore:
    """Build a minimal deterministic store for TESTING mode.

    entries: list of (value, abstract_column, tenant_id)
    Defaults to a set matching the test schema descriptions and sample values
    defined in main.py (mirrors what _warm_rag_store used to produce).
    """
    store = InMemoryVectorStore()
    defaults: list[tuple[str, str, str]] = entries or [
        # Table-level description hints (mirrors _warm_rag_store behaviour)
        ("User details", "users.users", "default_tenant"),
        ("Customer orders", "orders.orders", "default_tenant"),
        # Column description hints
        ("PK", "users.id", "default_tenant"),
        ("Name", "users.name", "default_tenant"),
        ("Active", "users.active", "default_tenant"),
        ("Creation", "users.created_at", "default_tenant"),
        ("FK", "orders.user_id", "default_tenant"),
        ("Total", "orders.total_amount", "default_tenant"),
        # Sample categorical values
        ("Alice", "users.name", "default_tenant"),
        ("Bob", "users.name", "default_tenant"),
        ("Charlie", "users.name", "default_tenant"),
    ]
    for value, abstract_column, t_id in defaults:
        store.index_value(
            CategoricalValue(
                value=value,
                abstract_column=abstract_column,
                tenant_id=t_id,
                artifact_version="test",
            )
        )
    store.set_artifact_version("test")
    return store
