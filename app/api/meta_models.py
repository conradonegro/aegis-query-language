import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    ForeignKeyConstraint,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass

class MetadataVersion(Base):
    """
    Tracks the immutable state payload for a set of schemas.
    The Runtime Aegis Engine boots EXCLUSIVELY off the latest 'active' artifact.
    """
    __tablename__ = "metadata_versions"
    __table_args__ = {"schema": "aegis_meta"}

    version_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    registry_hash: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Enum("draft", "pending_review", "active", "archived", name="version_status", schema="aegis_meta"), default="draft")
    created_by: Mapped[str] = mapped_column(Text, default="system")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    approved_by: Mapped[str | None] = mapped_column(Text)
    approved_at: Mapped[datetime | None] = mapped_column(DateTime)
    change_reason: Mapped[str | None] = mapped_column(Text)
    
    # Relationships
    tables = relationship("MetadataTable", back_populates="version", cascade="all, delete-orphan")
    columns = relationship("MetadataColumn", back_populates="version", cascade="all, delete-orphan")
    edges = relationship("MetadataRelationship", back_populates="version", cascade="all, delete-orphan")


class MetadataTable(Base):
    __tablename__ = "metadata_tables"
    table_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    version_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("aegis_meta.metadata_versions.version_id"))
    real_name: Mapped[str] = mapped_column(Text, nullable=False)
    alias: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    tenant_id: Mapped[str | None] = mapped_column(Text)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("version_id", "alias", name="uq_table_alias"),
        UniqueConstraint("version_id", "real_name", name="uq_table_real_name"),
        UniqueConstraint("version_id", "table_id", name="uq_table_composite_id"),
        {"schema": "aegis_meta"}
    )

    version = relationship("MetadataVersion", back_populates="tables")
    columns = relationship("MetadataColumn", back_populates="table", cascade="all, delete-orphan", overlaps="columns")


class MetadataColumn(Base):
    __tablename__ = "metadata_columns"
    column_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    version_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("aegis_meta.metadata_versions.version_id"))
    table_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))

    real_name: Mapped[str] = mapped_column(Text, nullable=False)
    alias: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    
    data_type: Mapped[str] = mapped_column(Text, nullable=False)
    is_nullable: Mapped[bool] = mapped_column(Boolean, default=True)
    is_primary_key: Mapped[bool] = mapped_column(Boolean, default=False)
    is_unique: Mapped[bool] = mapped_column(Boolean, default=False)
    is_sensitive: Mapped[bool] = mapped_column(Boolean, default=False)
    
    allowed_in_select: Mapped[bool] = mapped_column(Boolean, default=False)
    allowed_in_filter: Mapped[bool] = mapped_column(Boolean, default=False)
    allowed_in_join: Mapped[bool] = mapped_column(Boolean, default=False)
    safety_classification: Mapped[dict[str, Any] | None] = mapped_column(JSONB)

    __table_args__ = (
        UniqueConstraint("version_id", "table_id", "alias", name="uq_col_alias"),
        UniqueConstraint("version_id", "table_id", "real_name", name="uq_col_real_name"),
        UniqueConstraint("version_id", "column_id", name="uq_col_composite_id"),
        ForeignKeyConstraint(
            ["version_id", "table_id"],
            ["aegis_meta.metadata_tables.version_id", "aegis_meta.metadata_tables.table_id"]
        ),
        {"schema": "aegis_meta"}
    )

    version = relationship("MetadataVersion", back_populates="columns", overlaps="columns")
    table = relationship("MetadataTable", back_populates="columns", overlaps="columns,version")


class MetadataRelationship(Base):
    __tablename__ = "metadata_relationships"
    relationship_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    version_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("aegis_meta.metadata_versions.version_id"))
    
    source_table_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    source_column_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    target_table_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    target_column_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))

    relationship_type: Mapped[str] = mapped_column(Enum("fk", "logical", "denormalized", name="rel_type", schema="aegis_meta"))
    cardinality: Mapped[str] = mapped_column(Enum("1:1", "1:n", "n:1", "n:m", name="cardinality_type", schema="aegis_meta", default="1:n"))
    
    bidirectional: Mapped[bool] = mapped_column(Boolean, default=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True)

    __table_args__ = (
        ForeignKeyConstraint(
            ["version_id", "source_column_id"],
            ["aegis_meta.metadata_columns.version_id", "aegis_meta.metadata_columns.column_id"]
        ),
        ForeignKeyConstraint(
            ["version_id", "target_column_id"],
            ["aegis_meta.metadata_columns.version_id", "aegis_meta.metadata_columns.column_id"]
        ),
        {"schema": "aegis_meta"}
    )

    version = relationship("MetadataVersion", back_populates="edges")


class MetadataAudit(Base):
    """
    WORM compliant audit logging natively handled by postgres.
    """
    __tablename__ = "metadata_audit"
    __table_args__ = {"schema": "aegis_meta"}

    audit_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    version_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    actor: Mapped[str] = mapped_column(Text, nullable=False)
    action: Mapped[str] = mapped_column(Enum("create", "update", "approve", "deploy", "revoke", name="audit_action", schema="aegis_meta"))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    timestamp: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class CompiledRegistryArtifact(Base):
    """
    The strictly immutable JSON payload resulting from a compilation pipeline.
    This is what `steward.py` parses securely into runtime instances.
    """
    __tablename__ = "compiled_registry_artifacts"
    __table_args__ = {"schema": "aegis_meta"}

    artifact_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    version_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("aegis_meta.metadata_versions.version_id"), unique=True)
    artifact_blob: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    artifact_hash: Mapped[str] = mapped_column(Text, nullable=False)
    compiled_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    compiler_version: Mapped[str] = mapped_column(Text)
    signature: Mapped[str | None] = mapped_column(Text)
