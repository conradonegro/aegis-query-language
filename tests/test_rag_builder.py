"""Tests for app.rag.builder and app.rag.normalizer."""

import pytest

from app.rag.builder import (
    RagDivergenceError,
    _compute_values_hash,
    build_from_artifact,
    build_test_store,
)
from app.rag.models import CategoricalValue, RAGOutcome
from app.rag.normalizer import normalize
from app.rag.store import InMemoryVectorStore

# ---------------------------------------------------------------------------
# normalizer
# ---------------------------------------------------------------------------


def test_normalize_nfc() -> None:
    # é as two code points → single NFC code point
    result = normalize("\u0065\u0301")  # e + combining acute
    assert result == "\xe9"  # é precomposed


def test_normalize_strips_whitespace() -> None:
    assert normalize("  hello  ") == "hello"


def test_normalize_empty_returns_none() -> None:
    assert normalize("") is None
    assert normalize("   ") is None


def test_normalize_exceeds_max_length() -> None:
    with pytest.raises(ValueError, match="max length"):
        normalize("x" * 201)


def test_normalize_200_chars_ok() -> None:
    assert normalize("x" * 200) == "x" * 200


# ---------------------------------------------------------------------------
# store
# ---------------------------------------------------------------------------


def test_store_clear_all_tenant() -> None:
    store = InMemoryVectorStore()
    store.index_value(
        CategoricalValue(
            value="A",
            abstract_column="t.c",
            tenant_id="t1",
            artifact_version="v1",
        )
    )
    store.index_value(
        CategoricalValue(
            value="B",
            abstract_column="t.c",
            tenant_id="t2",
            artifact_version="v1",
        )
    )
    store.clear("t1")
    assert store.search("A", tenant_id="t1").outcome == RAGOutcome.NO_MATCH
    assert (
        store.search("B", tenant_id="t2").outcome
        == RAGOutcome.SINGLE_HIGH_CONFIDENCE_MATCH
    )


def test_store_clear_by_artifact_version() -> None:
    store = InMemoryVectorStore()
    store.index_value(
        CategoricalValue(
            value="old",
            abstract_column="t.c",
            tenant_id="t1",
            artifact_version="v1",
        )
    )
    store.index_value(
        CategoricalValue(
            value="new",
            abstract_column="t.c",
            tenant_id="t1",
            artifact_version="v2",
        )
    )
    store.clear("t1", artifact_version="v1")
    assert store.search("old", tenant_id="t1").outcome == RAGOutcome.NO_MATCH
    assert (
        store.search("new", tenant_id="t1").outcome
        == RAGOutcome.SINGLE_HIGH_CONFIDENCE_MATCH
    )


def test_store_index_ready_false_initially() -> None:
    store = InMemoryVectorStore()
    assert store.index_ready is False


def test_store_index_ready_true_after_set() -> None:
    store = InMemoryVectorStore()
    store.set_artifact_version("v1")
    assert store.index_ready is True
    assert store.indexed_artifact_version == "v1"


# ---------------------------------------------------------------------------
# builder helpers
# ---------------------------------------------------------------------------


def _make_blob(
    col_id: str = "col-1",
    rag_enabled: bool = True,
    is_sensitive: bool = False,
    cardinality: str | None = None,
    rag_values_hash: str = "",
) -> dict[str, object]:
    return {
        "tables": [
            {
                "alias": "orders",
                "tenant_id": "default_tenant",
                "columns": [
                    {
                        "id": col_id,
                        "alias": "status",
                        "rag_enabled": rag_enabled,
                        "is_sensitive": is_sensitive,
                        "rag_cardinality_hint": cardinality,
                        "rag_limit": None,
                        "rag_values_hash": rag_values_hash,
                    }
                ],
            }
        ]
    }


# ---------------------------------------------------------------------------
# builder async tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_builder_indexes_rag_enabled_values() -> None:
    col_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    values = ["PENDING", "SHIPPED"]
    expected_hash = _compute_values_hash(values)
    blob = _make_blob(col_id=col_id, rag_values_hash=expected_hash)
    store = await build_from_artifact(
        artifact_blob=blob,
        version_id="v1",
        tenant_id="default_tenant",
        artifact_version="hash123",
        column_values={col_id: values},
    )
    assert store.index_ready
    result = store.search("PENDING", tenant_id="default_tenant")
    assert result.outcome == RAGOutcome.SINGLE_HIGH_CONFIDENCE_MATCH


@pytest.mark.asyncio
async def test_builder_skips_rag_disabled_columns() -> None:
    col_id = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    blob = _make_blob(col_id=col_id, rag_enabled=False, rag_values_hash="")
    store = await build_from_artifact(
        artifact_blob=blob,
        version_id="v1",
        tenant_id="default_tenant",
        artifact_version="hash123",
        column_values={col_id: ["PENDING"]},
    )
    result = store.search("PENDING", tenant_id="default_tenant")
    assert result.outcome == RAGOutcome.NO_MATCH


@pytest.mark.asyncio
async def test_builder_skips_sensitive_columns() -> None:
    col_id = "cccccccc-cccc-cccc-cccc-cccccccccccc"
    values = ["ACTIVE"]
    expected_hash = _compute_values_hash(values)
    blob = _make_blob(col_id=col_id, is_sensitive=True, rag_values_hash=expected_hash)
    store = await build_from_artifact(
        artifact_blob=blob,
        version_id="v1",
        tenant_id="default_tenant",
        artifact_version="hash123",
        column_values={col_id: values},
    )
    result = store.search("ACTIVE", tenant_id="default_tenant")
    assert result.outcome == RAGOutcome.NO_MATCH


@pytest.mark.asyncio
async def test_builder_skips_high_cardinality() -> None:
    col_id = "dddddddd-dddd-dddd-dddd-dddddddddddd"
    blob = _make_blob(col_id=col_id, cardinality="high", rag_values_hash="")
    store = await build_from_artifact(
        artifact_blob=blob,
        version_id="v1",
        tenant_id="default_tenant",
        artifact_version="hash123",
        column_values={col_id: ["anything"]},
    )
    result = store.search("anything", tenant_id="default_tenant")
    assert result.outcome == RAGOutcome.NO_MATCH


@pytest.mark.asyncio
async def test_builder_empty_values_no_index() -> None:
    col_id = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"
    expected_hash = _compute_values_hash([])
    blob = _make_blob(col_id=col_id, rag_values_hash=expected_hash)
    store = await build_from_artifact(
        artifact_blob=blob,
        version_id="v1",
        tenant_id="default_tenant",
        artifact_version="hash123",
        column_values={},
    )
    assert store.index_ready
    assert (
        store.search("anything", tenant_id="default_tenant").outcome
        == RAGOutcome.NO_MATCH
    )


@pytest.mark.asyncio
async def test_builder_divergence_raises() -> None:
    col_id = "ffffffff-ffff-ffff-ffff-ffffffffffff"
    blob = _make_blob(col_id=col_id, rag_values_hash="wrong_hash")
    with pytest.raises(RagDivergenceError, match="divergence"):
        await build_from_artifact(
            artifact_blob=blob,
            version_id="v1",
            tenant_id="default_tenant",
            artifact_version="hash123",
            column_values={col_id: ["PENDING"]},
        )


@pytest.mark.asyncio
async def test_builder_tenant_isolation() -> None:
    col_id_1 = "11111111-1111-1111-1111-111111111111"
    col_id_2 = "22222222-2222-2222-2222-222222222222"
    values_1 = ["NYC"]
    values_2 = ["LA"]
    blob: dict[str, object] = {
        "tables": [
            {
                "alias": "cities",
                "tenant_id": "tenant_a",
                "columns": [
                    {
                        "id": col_id_1,
                        "alias": "city",
                        "rag_enabled": True,
                        "is_sensitive": False,
                        "rag_cardinality_hint": None,
                        "rag_limit": None,
                        "rag_values_hash": _compute_values_hash(values_1),
                    }
                ],
            },
            {
                "alias": "locations",
                "tenant_id": "tenant_b",
                "columns": [
                    {
                        "id": col_id_2,
                        "alias": "loc",
                        "rag_enabled": True,
                        "is_sensitive": False,
                        "rag_cardinality_hint": None,
                        "rag_limit": None,
                        "rag_values_hash": _compute_values_hash(values_2),
                    }
                ],
            },
        ]
    }
    column_values = {col_id_1: values_1, col_id_2: values_2}
    store = await build_from_artifact(
        artifact_blob=blob,
        version_id="v1",
        tenant_id="default_tenant",
        artifact_version="hash123",
        column_values=column_values,
    )
    # tenant_a can find NYC but not LA
    assert (
        store.search("NYC", tenant_id="tenant_a").outcome
        == RAGOutcome.SINGLE_HIGH_CONFIDENCE_MATCH
    )
    assert (
        store.search("LA", tenant_id="tenant_a").outcome == RAGOutcome.NO_MATCH
    )
    # tenant_b can find LA but not NYC
    assert (
        store.search("LA", tenant_id="tenant_b").outcome
        == RAGOutcome.SINGLE_HIGH_CONFIDENCE_MATCH
    )
    assert (
        store.search("NYC", tenant_id="tenant_b").outcome == RAGOutcome.NO_MATCH
    )


def test_build_test_store() -> None:
    store = build_test_store()
    assert store.index_ready
    result = store.search("Alice", tenant_id="default_tenant")
    assert result.outcome == RAGOutcome.SINGLE_HIGH_CONFIDENCE_MATCH


def test_build_test_store_custom_entries() -> None:
    store = build_test_store([("ACTIVE", "status.state", "t1")])
    result = store.search("ACTIVE", tenant_id="t1")
    assert result.outcome == RAGOutcome.SINGLE_HIGH_CONFIDENCE_MATCH
