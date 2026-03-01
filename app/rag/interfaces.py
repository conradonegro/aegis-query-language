from typing import Protocol

from app.rag.models import CategoricalValue, RAGResult


class VectorStoreProtocol(Protocol):
    """
    Interface for indexing and searching categorical values.
    """

    def index_value(self, value: CategoricalValue) -> None:
        """
        Indexes a categorical value into the store.
        """
        ...

    def search(
        self, query: str, tenant_id: str, limit: int = 5, threshold: float = 0.85
    ) -> RAGResult:
        """
        Searches the store for a semantic match against the query, isolated by tenant.
        Returns a strict RAGResult encapsulating the evaluated outcome.
        """
        ...
