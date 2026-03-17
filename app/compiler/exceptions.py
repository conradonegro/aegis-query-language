"""Domain exceptions for the compiler pipeline."""


class UnknownSourceDatabaseError(Exception):
    """Raised when source_database is explicitly set but matches no tables."""

    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(f"Unknown source_database: '{name}'")


class AmbiguousSourceDatabaseError(Exception):
    """Raised when auto-detection finds multiple equally plausible databases."""

    def __init__(self, candidates: list[str]) -> None:
        self.candidates = candidates
        super().__init__(
            f"Query matches multiple databases: {candidates}. "
            "Retry with source_database set to one of the candidates."
        )
