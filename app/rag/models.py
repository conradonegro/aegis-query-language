from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class RAGOutcome(StrEnum):
    """Explicit modeled outcomes for a RAG lookup."""
    NO_MATCH = "NO_MATCH"
    SINGLE_HIGH_CONFIDENCE_MATCH = "SINGLE_HIGH_CONFIDENCE_MATCH"
    AMBIGUOUS_MATCH = "AMBIGUOUS_MATCH"


class CategoricalValue(BaseModel):
    """A distinct categorical value indexed in the vector store."""

    model_config = ConfigDict(frozen=True)

    value: str = Field(..., description="The distinct string value, e.g. 'Nvidia'")
    abstract_column: str = Field(
        ..., description="The abstract column alias it belongs to."
    )
    tenant_id: str = Field(
        ..., description="The tenant ID this value is isolated to."
    )
    artifact_version: str = Field(
        default="",
        description="The artifact version this value was indexed from.",
    )


class ValueMatch(BaseModel):
    """A matched categorical value with its similarity score."""
    model_config = ConfigDict(frozen=True)

    categorical_value: CategoricalValue
    similarity_score: float = Field(
        ..., description="The confidence score of the match, typically 0.0 to 1.0"
    )


class RAGResult(BaseModel):
    """The explicit outcome of a RAG vector store search."""
    model_config = ConfigDict(frozen=True)

    outcome: RAGOutcome
    match: ValueMatch | None = Field(
        default=None,
        description="The winning match if outcome is SINGLE_HIGH_CONFIDENCE_MATCH.",
    )
    candidates: list[ValueMatch] | None = Field(
        default=None,
        description="The competing candidates if outcome was AMBIGUOUS_MATCH",
    )
    reason: str = Field(..., description="The explanation for the outcome.")
