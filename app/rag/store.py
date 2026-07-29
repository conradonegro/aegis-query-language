import difflib
import re
from collections import defaultdict

from app.rag.interfaces import VectorStoreProtocol
from app.rag.models import CategoricalValue, RAGOutcome, RAGResult, ValueMatch

_QUOTED_RE = re.compile(r"""['"]([^'"]{2,})['"]""")

# Minimum word count in the query before single-word matches get penalized.
# Short queries like "Nvidia revenue" should still give 1.0 for "Nvidia".
_SHORT_QUERY_WORD_LIMIT = 4

# A substring shorter than this carries no evidence that the user meant the
# value — single letters are substrings of almost any English question.
_MIN_SUBSTRING_MATCH_LEN = 3


def _is_word_char(ch: str) -> bool:
    """Mirrors regex \\w: alphanumeric (Unicode-aware) or underscore."""
    return ch.isalnum() or ch == "_"


def _is_word_boundary_match(val: str, query_full: str) -> bool:
    """True when *val* occurs in *query_full* delimited by word boundaries.

    Guards against coincidental infixes ('art' inside 'artifact'), which are
    not evidence that the user meant that value.

    Deliberately hand-rolled rather than
    re.search(rf"(?<!\\w){re.escape(val)}(?!\\w)", query_full): the pattern
    embeds the value, so every distinct value is a distinct pattern. Python's
    512-entry compiled-pattern cache thrashes completely against an index of
    this size and recompiles on every call — measured 289x slower, 27.2s vs
    94ms for a single query against 81,632 values.
    """
    start = query_full.find(val)
    while start != -1:
        end = start + len(val)
        before_ok = start == 0 or not _is_word_char(query_full[start - 1])
        after_ok = end == len(query_full) or not _is_word_char(query_full[end])
        if before_ok and after_ok:
            return True
        start = query_full.find(val, start + 1)
    return False


def _extract_quoted_phrases(query: str) -> list[str]:
    """Return lowercased phrases found between single or double quotes."""
    return [m.strip().lower() for m in _QUOTED_RE.findall(query) if m.strip()]


def _score_value(
    val: str,
    query_words: set[str],
    query_full: str,
    quoted_phrases: list[str],
    matcher: "difflib.SequenceMatcher[str]",
    threshold: float,
) -> float:
    """Score a single normalized value against the pre-processed query.

    Returns the best score across three strategies:
      1. Quoted-phrase matching (exact or substring → 0.95-1.0)
      2. Word/substring matching against the full query (0.88-1.0)
      3. Fuzzy difflib ratio as fallback

    *matcher* must already have seq2 set to *query_full* by the caller.
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
    elif (
        len(val) >= _MIN_SUBSTRING_MATCH_LEN
        and _is_word_boundary_match(val, query_full)
    ):
        best = max(best, 0.9)

    # 3. Fuzzy fallback — gated on sharing at least one whole word with
    # the query. Entity lookups virtually always share a word ("Aaron
    # Doran" ↔ "aaron doran"); the gate lets the store index six-figure
    # value counts without running difflib against every non-candidate.
    #
    # The shared-word gate alone is not enough: it still admitted ~65k
    # values per query. real_quick_ratio() is 2*min(la,lb)/(la+lb) and
    # quick_ratio() is a character-multiset bound; both are guaranteed
    # >= ratio(), so skipping when either falls below the caller's
    # threshold cannot change which values clear it. A short value can
    # never approach a long query — clearing 0.85 needs
    # len(val) >= 0.739 * len(query) — which made this branch ~98% of
    # scan time while contributing no matches at BIRD query lengths. It
    # stays live for short queries, where it does real work.
    if best < 0.85 and set(val.split()) & query_words:
        matcher.set_seq1(val)
        if (
            matcher.real_quick_ratio() >= threshold
            and matcher.quick_ratio() >= threshold
        ):
            best = max(best, matcher.ratio())
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
        source_database: str | None = None,
    ) -> RAGResult:
        tenant_values = self._store.get(tenant_id, [])
        if not tenant_values:
            return RAGResult(
                outcome=RAGOutcome.NO_MATCH,
                reason="Tenant vector store is empty.",
            )

        # Scope to one logical database when the caller has resolved it.
        # None means unscoped — the deliberate fallback for queries where
        # database detection was not confident, since withholding hints
        # when the compiler is least certain is the worse failure mode.
        if source_database is not None:
            tenant_values = [
                v for v in tenant_values if v.source_database == source_database
            ]
            if not tenant_values:
                return RAGResult(
                    outcome=RAGOutcome.NO_MATCH,
                    reason=(
                        f"No indexed values for source database "
                        f"'{source_database}'."
                    ),
                )

        query_normalized = query.lower().strip()
        query_words = set(query_normalized.split())
        quoted_phrases = _extract_quoted_phrases(query_normalized)

        # One matcher for the whole scan: set_seq2 caches the b2j index of
        # the long query string, which SequenceMatcher(None, val, query)
        # would otherwise rebuild on every value. autojunk keeps its default
        # (True) so ratio() results are identical to the previous form.
        matcher = difflib.SequenceMatcher()
        matcher.set_seq2(query_normalized)

        matches: list[ValueMatch] = []
        for cat_val in tenant_values:
            val_normalized = cat_val.value.lower().strip()
            score = _score_value(
                val_normalized,
                query_words,
                query_normalized,
                quoted_phrases,
                matcher,
                threshold,
            )
            if score >= threshold:
                matches.append(
                    ValueMatch(
                        categorical_value=cat_val,
                        similarity_score=score,
                    )
                )

        # Rank by score, then by specificity. Sorting on score alone is
        # stable, which meant equal-scoring values were ordered by artifact
        # insertion order — the same question could surface different hints
        # purely from table ordering.
        matches.sort(
            key=lambda x: (x.similarity_score, len(x.categorical_value.value)),
            reverse=True,
        )
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
