import pytest

from app.rag.models import CategoricalValue, RAGOutcome
from app.rag.store import InMemoryVectorStore


@pytest.fixture
def store() -> InMemoryVectorStore:
    s = InMemoryVectorStore()
    s.index_value(CategoricalValue(value="Nvidia", abstract_column="companies", tenant_id="t1"))
    s.index_value(CategoricalValue(value="AMD", abstract_column="companies", tenant_id="t1"))
    s.index_value(CategoricalValue(value="Apple", abstract_column="companies", tenant_id="t2"))
    return s


def test_rag_no_match(store: InMemoryVectorStore) -> None:
    res = store.search("Tell me about Microsoft", tenant_id="t1")
    assert res.outcome == RAGOutcome.NO_MATCH
    assert res.match is None
    assert res.reason == "No candidates met the threshold (0.85)."


def test_rag_single_high_confidence_match(store: InMemoryVectorStore) -> None:
    res = store.search("Show me the Nvidia stocks", tenant_id="t1")
    assert res.outcome == RAGOutcome.SINGLE_HIGH_CONFIDENCE_MATCH
    assert res.match is not None
    assert res.match.categorical_value.value == "Nvidia"
    assert res.match.similarity_score >= 0.85
    assert res.reason == "Exactly one high confidence match found."


def test_rag_tenant_isolation(store: InMemoryVectorStore) -> None:
    # Apple is in t2, so asking about Apple in t1 should yield NO_MATCH
    res = store.search("Show me Apple", tenant_id="t1")
    assert res.outcome == RAGOutcome.NO_MATCH

    res2 = store.search("Show me Apple", tenant_id="t2")
    assert res2.outcome == RAGOutcome.SINGLE_HIGH_CONFIDENCE_MATCH
    assert res2.match is not None
    assert res2.match.categorical_value.value == "Apple"


def test_rag_ambiguous_match(store: InMemoryVectorStore) -> None:
    # Add another value that is extremely similar "Nvidia Corporation", such that querying "Nvidia" matches both strongly.
    store.index_value(CategoricalValue(value="Nvidia Corporation", abstract_column="companies", tenant_id="t1"))
    
    res = store.search("Show me Nvidia or Nvidia Corporation", tenant_id="t1")
    # Because both have substring match in the query, both score high
    assert res.outcome == RAGOutcome.AMBIGUOUS_MATCH
    assert res.match is None
    assert res.candidates is not None
    assert {c.categorical_value.value for c in res.candidates} == {
        "Nvidia",
        "Nvidia Corporation",
    }
    assert "Ambiguous: 2 competing matches breached the threshold." in res.reason


def test_rag_below_threshold(store: InMemoryVectorStore) -> None:
    # Query has a typo that is close, but fuzzy matches under the strict 0.85 threshold limit.
    res = store.search("Show me Nvdia", tenant_id="t1")
    assert res.outcome == RAGOutcome.NO_MATCH
    assert res.match is None
    assert res.reason == "No candidates met the threshold (0.85)."
