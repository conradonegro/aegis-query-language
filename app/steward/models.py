from pydantic import BaseModel


class SafetyClassification(BaseModel):
    allowed_in_where: bool = False
    allowed_in_select: bool = False
    allowed_in_group_by: bool = False
    aggregation_allowed: bool = False
    join_participation_allowed: bool = False

class AbstractIdentifierDef(BaseModel):
    """Safe, abstract representation of a schema element.
    Includes mapped target for translation but NEVER leaked to LLM.
    """
    alias: str
    description: str
    safety: SafetyClassification
    physical_target: str

class RegistrySchema(BaseModel):
    """Immutable snapshot of an active schema.
    The ONLY cross-context schema artifact.
    """
    version: str
    identifiers: list[AbstractIdentifierDef]

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
