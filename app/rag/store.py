import difflib
from collections import defaultdict

from app.rag.interfaces import VectorStoreProtocol
from app.rag.models import CategoricalValue, RAGOutcome, RAGResult, ValueMatch


class InMemoryVectorStore(VectorStoreProtocol):
    """
    A lightweight, zero-dependency mock vector store using Python's difflib
    for fuzzy string matching to simulate embeddings.
    """

    def __init__(self) -> None:
        # Maps tenant_id -> list of CategoricalValue
        self._store: dict[str, list[CategoricalValue]] = defaultdict(list)

    def index_value(self, value: CategoricalValue) -> None:
        self._store[value.tenant_id].append(value)

    def search(
        self, query: str, tenant_id: str, limit: int = 5, threshold: float = 0.85
    ) -> RAGResult:
        tenant_values = self._store.get(tenant_id, [])
        if not tenant_values:
            return RAGResult(
                outcome=RAGOutcome.NO_MATCH,
                reason="Tenant vector store is empty."
            )

        query_normalized = query.lower().strip()

        matches: list[ValueMatch] = []
        for cat_val in tenant_values:
            val_normalized = cat_val.value.lower().strip()

            # Substring match is strong confidence
            if val_normalized in query_normalized.split(): # Exact word match gets 1.0
                score = 1.0
            elif val_normalized in query_normalized: # Partial word match gets 0.9
                score = 0.9
            else:
                # Fallback to difflib
                score = difflib.SequenceMatcher(
                    None, val_normalized, query_normalized
                ).ratio()

            if score >= threshold:
                matches.append(
                    ValueMatch(categorical_value=cat_val, similarity_score=score)
                )

        # Sort by best score descending
        matches.sort(key=lambda x: x.similarity_score, reverse=True)
        matches = matches[:limit]

        if not matches:
            return RAGResult(
                outcome=RAGOutcome.NO_MATCH,
                reason=f"No candidates met the threshold ({threshold})."
            )
        elif len(matches) == 1:
            return RAGResult(
                outcome=RAGOutcome.SINGLE_HIGH_CONFIDENCE_MATCH,
                match=matches[0],
                reason="Exactly one high confidence match found."
            )
        else:
            return RAGResult(
                outcome=RAGOutcome.AMBIGUOUS_MATCH,
                candidates=matches,
                reason=(
                    f"Ambiguous: {len(matches)} competing matches breached "
                    f"the threshold."
                )
            )
