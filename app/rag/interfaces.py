from typing import Protocol

from app.rag.models import CategoricalValue, RAGResult


class VectorStoreProtocol(Protocol):
    """
    Interface for indexing and searching categorical values.
    """

    def index_value(self, value: CategoricalValue) -> None:
        """Indexes a categorical value into the store."""
        ...

    def search(
        self,
        query: str,
        tenant_id: str,
        limit: int = 5,
        threshold: float = 0.85,
    ) -> RAGResult:
        """
        Searches the store for a semantic match against the query, isolated
        by tenant. Returns a strict RAGResult encapsulating the evaluated
        outcome.
        """
        ...

    def clear(
        self,
        tenant_id: str,
        artifact_version: str | None = None,
    ) -> None:
        """
        Remove indexed values for a tenant.

        If artifact_version is given, only entries matching that version are
        removed. Otherwise all entries for the tenant are cleared.
        """
        ...

    def set_artifact_version(self, v: str) -> None:
        """Record the artifact version that was used to build this index."""
        ...

    @property
    def index_ready(self) -> bool:
        """True once an artifact version has been recorded (index is built)."""
        ...

    @property
    def indexed_artifact_version(self) -> str | None:
        """The artifact version the index was built from, or None if not built."""
        ...
