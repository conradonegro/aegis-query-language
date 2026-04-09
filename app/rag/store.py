import difflib
import re
from collections import defaultdict

from app.rag.interfaces import VectorStoreProtocol
from app.rag.models import CategoricalValue, RAGOutcome, RAGResult, ValueMatch

_QUOTED_RE = re.compile(r"""['"]([^'"]{2,})['"]""")

# Minimum word count in the query before single-word matches get penalized.
# Short queries like "Nvidia revenue" should still give 1.0 for "Nvidia".
_SHORT_QUERY_WORD_LIMIT = 4


def _extract_quoted_phrases(query: str) -> list[str]:
    """Return lowercased phrases found between single or double quotes."""
    return [m.strip().lower() for m in _QUOTED_RE.findall(query) if m.strip()]


def _score_value(
    val: str,
    query_words: set[str],
    query_full: str,
    quoted_phrases: list[str],
) -> float:
    """Score a single normalized value against the pre-processed query.

    Returns the best score across three strategies:
      1. Quoted-phrase matching (exact or substring → 0.95-1.0)
      2. Word/substring matching against the full query (0.88-1.0)
      3. Fuzzy difflib ratio as fallback
    """
    val_word_count = len(val.split())

    # 1. Quoted-phrase matching — highest signal.
    best = 0.0
    for phrase in quoted_phrases:
        if val == phrase:
            return 1.0
        if val in phrase or phrase in val:
            best = max(best, 0.95)

    # 2. Word / substring matching against full query.
    if val in query_words:
        # Single-word values matching in a long query are penalized to
        # reduce noise from common English ("status", "event"). In short
        # queries the word IS the signal, so keep 1.0.
        if val_word_count > 1 or len(query_words) < _SHORT_QUERY_WORD_LIMIT:
            best = max(best, 1.0)
        else:
            best = max(best, 0.88)
    elif val in query_full:
        best = max(best, 0.9)

    # 3. Fuzzy fallback.
    if best < 0.85:
        best = max(
            best,
            difflib.SequenceMatcher(None, val, query_full).ratio(),
        )
    return best


class InMemoryVectorStore(VectorStoreProtocol):
    """
    A lightweight, zero-dependency vector store using Python's difflib
    for fuzzy string matching to simulate embeddings.
    """

    def __init__(self) -> None:
        # Maps tenant_id -> list of CategoricalValue
        self._store: dict[str, list[CategoricalValue]] = defaultdict(list)
        self._artifact_version: str | None = None

    def index_value(self, value: CategoricalValue) -> None:
        self._store[value.tenant_id].append(value)

    def search(
        self,
        query: str,
        tenant_id: str,
        limit: int = 5,
        threshold: float = 0.85,
    ) -> RAGResult:
        tenant_values = self._store.get(tenant_id, [])
        if not tenant_values:
            return RAGResult(
                outcome=RAGOutcome.NO_MATCH,
                reason="Tenant vector store is empty.",
            )

        query_normalized = query.lower().strip()
        query_words = set(query_normalized.split())
        quoted_phrases = _extract_quoted_phrases(query_normalized)

        matches: list[ValueMatch] = []
        for cat_val in tenant_values:
            val_normalized = cat_val.value.lower().strip()
            score = _score_value(
                val_normalized, query_words, query_normalized, quoted_phrases
            )
            if score >= threshold:
                matches.append(
                    ValueMatch(
                        categorical_value=cat_val,
                        similarity_score=score,
                    )
                )

        matches.sort(key=lambda x: x.similarity_score, reverse=True)
        matches = matches[:limit]

        if not matches:
            return RAGResult(
                outcome=RAGOutcome.NO_MATCH,
                reason=f"No candidates met the threshold ({threshold}).",
            )
        if len(matches) == 1:
            return RAGResult(
                outcome=RAGOutcome.SINGLE_HIGH_CONFIDENCE_MATCH,
                match=matches[0],
                reason="Exactly one high confidence match found.",
            )
        return RAGResult(
            outcome=RAGOutcome.AMBIGUOUS_MATCH,
            candidates=matches,
            reason=(
                f"Ambiguous: {len(matches)} competing matches breached "
                f"the threshold."
            ),
        )

    def clear(
        self,
        tenant_id: str,
        artifact_version: str | None = None,
    ) -> None:
        """Remove indexed values for a tenant.

        If artifact_version is given, only entries matching that version are
        removed. Otherwise all entries for the tenant are cleared.
        """
        if artifact_version is None:
            self._store.pop(tenant_id, None)
        else:
            current = self._store.get(tenant_id, [])
            self._store[tenant_id] = [
                v for v in current if v.artifact_version != artifact_version
            ]

    def set_artifact_version(self, v: str) -> None:
        """Record the artifact version that was used to build this index."""
        self._artifact_version = v

    @property
    def index_ready(self) -> bool:
        """True once an artifact version has been recorded."""
        return self._artifact_version is not None

    @property
    def indexed_artifact_version(self) -> str | None:
        """The artifact version the index was built from, or None."""
        return self._artifact_version
