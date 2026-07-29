import difflib

import pytest

from app.compiler.engine import CompilerEngine, RAGUncertaintyError
from app.compiler.filter import DeterministicSchemaFilter
from app.compiler.gateway import MockLLMGateway
from app.compiler.models import PromptHints, RAGIncludedColumns, UserIntent
from app.compiler.parser import SQLParser
from app.compiler.prompting import PromptBuilder
from app.compiler.safety import SafetyEngine
from app.compiler.translator import DeterministicTranslator
from app.rag.models import CategoricalValue, RAGOutcome
from app.rag.store import InMemoryVectorStore


@pytest.fixture
def store() -> InMemoryVectorStore:
    s = InMemoryVectorStore()
    s.index_value(
        CategoricalValue(value="Nvidia", abstract_column="companies", tenant_id="t1")
    )
    s.index_value(
        CategoricalValue(value="AMD", abstract_column="companies", tenant_id="t1")
    )
    s.index_value(
        CategoricalValue(value="Apple", abstract_column="companies", tenant_id="t2")
    )
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
    # Add another value that is extremely similar "Nvidia Corporation",
    # such that querying "Nvidia" matches both strongly.
    store.index_value(
        CategoricalValue(
            value="Nvidia Corporation",
            abstract_column="companies",
            tenant_id="t1",
        )
    )

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
    # Query has a typo that is close, but fuzzy matches under the strict 0.85
    # threshold limit.
    res = store.search("Show me Nvdia", tenant_id="t1")
    assert res.outcome == RAGOutcome.NO_MATCH
    assert res.match is None
    assert res.reason == "No candidates met the threshold (0.85)."


def test_rag_empty_tenant_returns_no_match() -> None:
    """Querying a tenant with zero indexed values must return NO_MATCH immediately."""
    store = InMemoryVectorStore()
    res = store.search("Show me anything", tenant_id="unknown_tenant")
    assert res.outcome == RAGOutcome.NO_MATCH
    assert res.match is None


def test_rag_exact_word_match_scores_1() -> None:
    """An exact word match (query word == value) must yield similarity_score == 1.0."""
    store = InMemoryVectorStore()
    store.index_value(
        CategoricalValue(value="Nvidia", abstract_column="brands", tenant_id="t")
    )
    res = store.search("Nvidia", tenant_id="t")
    assert res.outcome == RAGOutcome.SINGLE_HIGH_CONFIDENCE_MATCH
    assert res.match is not None
    assert res.match.similarity_score == 1.0


def test_rag_substring_match_scores_0_9() -> None:
    """A substring match (value is a substring of the query or vice-versa)
    scores 0.9."""
    store = InMemoryVectorStore()
    # "NvidiaGPU" is not an exact word in the query but is a substring case
    store.index_value(
        CategoricalValue(value="Nvidia", abstract_column="brands", tenant_id="t")
    )
    # Query contains "NvidiaGPU" which has "Nvidia" as a substring — score 0.9
    res = store.search("NvidiaGPU", tenant_id="t")
    assert res.outcome == RAGOutcome.SINGLE_HIGH_CONFIDENCE_MATCH
    assert res.match is not None
    assert res.match.similarity_score == 0.9


# ------------------------------------------------------------------
# RAG_STRICT_MODE tests
# ------------------------------------------------------------------

def _make_engine(store: InMemoryVectorStore) -> CompilerEngine:
    engine = CompilerEngine(
        schema_filter=DeterministicSchemaFilter(),
        prompt_builder=PromptBuilder(),
        llm_gateway=MockLLMGateway(),
        parser=SQLParser(),
        safety_engine=SafetyEngine(),
        translator=DeterministicTranslator(),
    )
    engine.set_vector_store(store, "t1")
    return engine


def test_strict_mode_raises_on_ambiguous_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RAG_STRICT_MODE=true must raise RAGUncertaintyError on AMBIGUOUS_MATCH."""
    monkeypatch.setenv("RAG_STRICT_MODE", "true")

    store = InMemoryVectorStore()
    store.index_value(
        CategoricalValue(value="Nvidia", abstract_column="companies", tenant_id="t1")
    )
    store.index_value(
        CategoricalValue(
            value="Nvidia Corporation", abstract_column="companies", tenant_id="t1"
        )
    )
    engine = _make_engine(store)

    with pytest.raises(RAGUncertaintyError, match="Ambiguous RAG match"):
        engine._apply_rag_hints(
            intent=UserIntent(
                natural_language_query="Show me Nvidia or Nvidia Corporation"
            ),
            hints=PromptHints(column_hints=[]),
            included_cols=RAGIncludedColumns(columns=[]),
            explain_context={},
            tenant_id="t1",
        )


def test_strict_mode_raises_on_no_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RAG_STRICT_MODE=true must raise RAGUncertaintyError on NO_MATCH."""
    monkeypatch.setenv("RAG_STRICT_MODE", "true")

    store = InMemoryVectorStore()
    store.index_value(
        CategoricalValue(value="Nvidia", abstract_column="companies", tenant_id="t1")
    )
    engine = _make_engine(store)

    with pytest.raises(RAGUncertaintyError, match="No RAG match"):
        engine._apply_rag_hints(
            intent=UserIntent(natural_language_query="Tell me about Microsoft"),
            hints=PromptHints(column_hints=[]),
            included_cols=RAGIncludedColumns(columns=[]),
            explain_context={},
            tenant_id="t1",
        )


def test_strict_mode_off_allows_ambiguous_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without RAG_STRICT_MODE, ambiguous matches proceed and surface candidates."""
    monkeypatch.delenv("RAG_STRICT_MODE", raising=False)

    store = InMemoryVectorStore()
    store.index_value(
        CategoricalValue(value="Nvidia", abstract_column="companies", tenant_id="t1")
    )
    store.index_value(
        CategoricalValue(
            value="Nvidia Corporation", abstract_column="companies", tenant_id="t1"
        )
    )
    engine = _make_engine(store)
    hints = PromptHints(column_hints=[])

    # Must not raise
    engine._apply_rag_hints(
        intent=UserIntent(
            natural_language_query="Show me Nvidia or Nvidia Corporation"
        ),
        hints=hints,
        included_cols=RAGIncludedColumns(columns=[]),
        explain_context={},
        tenant_id="t1",
    )
    assert len(hints.column_hints) == 1
    assert "Nvidia" in hints.column_hints[0]


# ---------------------------------------------------------------------------
# Fuzzy-fallback word gate — required to index high-cardinality columns
# ---------------------------------------------------------------------------


def test_fuzzy_fallback_runs_when_word_shared() -> None:
    s = InMemoryVectorStore()
    s.index_value(CategoricalValue(
        tenant_id="t", abstract_column="p.name",
        artifact_version="v1", value="Anna Sartorri",
    ))
    # 'anna' is shared; the slightly-off full match resolves via fuzzy.
    res = s.search("records for Anna Sartorri please", tenant_id="t")
    assert res.outcome != RAGOutcome.NO_MATCH


def test_fuzzy_fallback_gated_without_shared_word() -> None:
    """Values sharing no whole word with the query skip the expensive
    difflib comparison — the enabler for ~120k indexed values."""
    s = InMemoryVectorStore()
    s.index_value(CategoricalValue(
        tenant_id="t", abstract_column="p.name",
        artifact_version="v1", value="an sartori",
    ))
    # near the full query textually, but zero shared words
    res = s.search("ana sartorri", tenant_id="t")
    assert res.outcome == RAGOutcome.NO_MATCH


def test_fuzzy_fallback_still_matches_short_query() -> None:
    """The difflib fallback is live for SHORT queries: 'nvidia korp' has
    neither an exact nor a substring match against 'Nvidia Corp', so only
    the fuzzy path can find it (ratio 0.909). This must survive the
    upper-bound optimisation — the optimisation must not become a deletion."""
    s = InMemoryVectorStore()
    s.index_value(
        CategoricalValue(
            value="Nvidia Corp", abstract_column="c.name", tenant_id="t1"
        )
    )
    res = s.search("nvidia korp", tenant_id="t1")
    assert res.outcome == RAGOutcome.SINGLE_HIGH_CONFIDENCE_MATCH
    assert res.match is not None
    assert res.match.categorical_value.value == "Nvidia Corp"


def test_fuzzy_fallback_not_called_for_long_queries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A short value can never clear the threshold against a long query:
    ratio() <= 2*len(val)/(len(val)+len(query)). difflib.ratio() must not be
    called at all. Each seeded value shares a whole word with the query (so
    the shared-word gate passes) but is NOT a substring of it (so the 0.9
    substring path does not short-circuit first) — this pins the precheck
    itself, not the gate."""
    s = InMemoryVectorStore()
    for name in ("aaron o'brien", "doran mcgregor", "rating specialist"):
        s.index_value(
            CategoricalValue(
                value=name, abstract_column="player.name", tenant_id="t1"
            )
        )

    calls = 0
    real_ratio = difflib.SequenceMatcher.ratio

    def counting_ratio(self: "difflib.SequenceMatcher[str]") -> float:
        nonlocal calls
        calls += 1
        return real_ratio(self)

    monkeypatch.setattr(difflib.SequenceMatcher, "ratio", counting_ratio)

    long_query = (
        "What is the overall rating for the football player aaron doran "
        "and how does that compare with the average rating of all other "
        "players in the same league during the 2015 season?"
    )
    res = s.search(long_query, tenant_id="t1")

    assert calls == 0
    assert res.outcome == RAGOutcome.NO_MATCH
