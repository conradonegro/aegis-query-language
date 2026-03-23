from typing import Literal

from pydantic import BaseModel


class SafetyClassification(BaseModel):
    allowed_in_where: bool = False
    allowed_in_select: bool = False
    allowed_in_group_by: bool = False
    aggregation_allowed: bool = False
    join_participation_allowed: bool = False

class AbstractColumnDef(BaseModel):
    alias: str
    description: str
    data_type: str = "text"
    safety: SafetyClassification
    physical_target: str
    rag_enabled: bool = False
    rag_cardinality_hint: Literal["low", "medium", "high"] | None = None
    sample_values: list[str] = []

class AbstractTableDef(BaseModel):
    alias: str
    description: str
    columns: list[AbstractColumnDef]
    physical_target: str
    source_database: str | None = None

class AbstractRelationshipDef(BaseModel):
    source_table: str
    source_column: str
    target_table: str
    target_column: str

class RegistrySchema(BaseModel):
    """Immutable snapshot of an active schema.
    The ONLY cross-context schema artifact.
    """
    version: str
    tables: list[AbstractTableDef]
    relationships: list[AbstractRelationshipDef]

class RegistryEntry(BaseModel):
    """Internal Steward mapping from abstract to physical. MUST remain internal."""
    abstract_alias: str
    physical_table: str
    physical_column: str
    description: str | None = None

class SchemaChangeProposal(BaseModel):
    physical_table: str
    physical_column: str
    intended_meaning: str
    sensitivity: str
    tenant_scope: str
