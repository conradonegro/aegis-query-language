import os
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import (  # noqa: E402
    Boolean,
    DateTime,
    Enum,
    ForeignKey,
    ForeignKeyConstraint,
    Integer,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID  # noqa: E402
from sqlalchemy.orm import (  # noqa: E402
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
)

# SQLite (used in tests) doesn't support schemas. Conditionally strip it.
_SCHEMA: str | None = None if os.getenv("TESTING") == "true" else "aegis_meta"



class Base(DeclarativeBase):
    pass

class MetadataVersion(Base):
    """
    Tracks the immutable state payload for a set of schemas.
    The Runtime Aegis Engine boots EXCLUSIVELY off the latest 'active' artifact.
    """
    __tablename__ = "metadata_versions"
    __table_args__ = {"schema": "aegis_meta"}

    version_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    registry_hash: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str] = mapped_column(
        Enum(
            "draft",
            "pending_review",
            "active",
            "archived",
            name="version_status",
            schema="aegis_meta",
        ),
        default="draft",
    )
    created_by: Mapped[str] = mapped_column(Text, default="system")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )
    approved_by: Mapped[str | None] = mapped_column(Text)
    approved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    change_reason: Mapped[str | None] = mapped_column(Text)

    # Relationships
    tables = relationship(
        "MetadataTable", back_populates="version", cascade="all, delete-orphan"
    )
    columns = relationship(
        "MetadataColumn", back_populates="version", cascade="all, delete-orphan"
    )
    edges = relationship(
        "MetadataRelationship",
        back_populates="version",
        cascade="all, delete-orphan",
    )


class MetadataTable(Base):
    __tablename__ = "metadata_tables"
    table_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    version_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("aegis_meta.metadata_versions.version_id")
    )
    real_name: Mapped[str] = mapped_column(Text, nullable=False)
    alias: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    tenant_id: Mapped[str | None] = mapped_column(Text)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )

    __table_args__ = (
        UniqueConstraint("version_id", "alias", name="uq_table_alias"),
        UniqueConstraint("version_id", "real_name", name="uq_table_real_name"),
        UniqueConstraint("version_id", "table_id", name="uq_table_composite_id"),
        {"schema": "aegis_meta"},
    )

    version = relationship("MetadataVersion", back_populates="tables")
    columns = relationship(
        "MetadataColumn",
        back_populates="table",
        cascade="all, delete-orphan",
        overlaps="columns",
    )


class MetadataColumn(Base):
    __tablename__ = "metadata_columns"
    column_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    version_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("aegis_meta.metadata_versions.version_id")
    )
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

    # RAG indexing fields
    rag_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    rag_cardinality_hint: Mapped[str | None] = mapped_column(
        Enum(
            "low",
            "medium",
            "high",
            name="rag_cardinality",
            schema="aegis_meta",
        ),
        nullable=True,
    )
    rag_limit: Mapped[int | None] = mapped_column(Integer, nullable=True)

    __table_args__ = (
        UniqueConstraint("version_id", "table_id", "alias", name="uq_col_alias"),
        UniqueConstraint(
            "version_id", "table_id", "real_name", name="uq_col_real_name"
        ),
        UniqueConstraint("version_id", "column_id", name="uq_col_composite_id"),
        ForeignKeyConstraint(
            ["version_id", "table_id"],
            [
                "aegis_meta.metadata_tables.version_id",
                "aegis_meta.metadata_tables.table_id",
            ]
        ),
        {"schema": "aegis_meta"},
    )

    version = relationship(
        "MetadataVersion", back_populates="columns", overlaps="columns"
    )
    table = relationship(
        "MetadataTable", back_populates="columns", overlaps="columns,version"
    )
    values = relationship(
        "MetadataColumnValue",
        back_populates="column",
        cascade="all, delete-orphan",
    )


class MetadataColumnValue(Base):
    """Curated categorical values that get indexed into the RAG store."""

    __tablename__ = "metadata_column_values"
    __table_args__ = (
        UniqueConstraint(
            "version_id", "column_id", "value", name="uq_col_value"
        ),
        ForeignKeyConstraint(
            ["version_id"],
            ["aegis_meta.metadata_versions.version_id"],
        ),
        ForeignKeyConstraint(
            ["version_id", "column_id"],
            [
                "aegis_meta.metadata_columns.version_id",
                "aegis_meta.metadata_columns.column_id",
            ],
        ),
        {"schema": "aegis_meta"},
    )

    value_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    column_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    version_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), nullable=False
    )
    value: Mapped[str] = mapped_column(Text, nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )

    column = relationship(
        "MetadataColumn",
        back_populates="values",
        foreign_keys="[MetadataColumnValue.version_id, MetadataColumnValue.column_id]",
    )


class MetadataRelationship(Base):
    __tablename__ = "metadata_relationships"
    relationship_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    version_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("aegis_meta.metadata_versions.version_id")
    )

    source_table_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    source_column_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    target_table_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    target_column_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))

    relationship_type: Mapped[str] = mapped_column(
        Enum("fk", "logical", "denormalized", name="rel_type", schema="aegis_meta")
    )
    cardinality: Mapped[str] = mapped_column(
        Enum(
            "1:1",
            "1:n",
            "n:1",
            "n:m",
            name="cardinality_type",
            schema="aegis_meta",
            default="1:n",
        )
    )

    bidirectional: Mapped[bool] = mapped_column(Boolean, default=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True)

    __table_args__ = (
        ForeignKeyConstraint(
            ["version_id", "source_column_id"],
            [
                "aegis_meta.metadata_columns.version_id",
                "aegis_meta.metadata_columns.column_id",
            ]
        ),
        ForeignKeyConstraint(
            ["version_id", "target_column_id"],
            [
                "aegis_meta.metadata_columns.version_id",
                "aegis_meta.metadata_columns.column_id",
            ]
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

    audit_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    version_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    actor: Mapped[str] = mapped_column(Text, nullable=False)
    action: Mapped[str] = mapped_column(
        Enum(
            "create",
            "update",
            "approve",
            "deploy",
            "revoke",
            name="audit_action",
            schema="aegis_meta",
        )
    )
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )

    # Cryptographic WORM Chaining
    previous_hash: Mapped[str | None] = mapped_column(Text)
    row_hash: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    hash_algorithm: Mapped[str] = mapped_column(Text, default="sha256-v1")
    key_id: Mapped[str | None] = mapped_column(Text)

    # API key traceability — which credential triggered this admin action
    credential_id: Mapped[str | None] = mapped_column(Text, nullable=True)


class CompiledRegistryArtifact(Base):
    """
    The strictly immutable JSON payload resulting from a compilation pipeline.
    This is what `steward.py` parses securely into runtime instances.
    """
    __tablename__ = "compiled_registry_artifacts"
    __table_args__ = {"schema": "aegis_meta"}

    artifact_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    version_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("aegis_meta.metadata_versions.version_id"), unique=True
    )
    artifact_blob: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    artifact_hash: Mapped[str] = mapped_column(Text, nullable=False)
    compiled_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )
    compiler_version: Mapped[str] = mapped_column(Text)

    # Cryptographic HMAC Verification
    signature: Mapped[str | None] = mapped_column(Text)
    signature_algo: Mapped[str] = mapped_column(Text, default="hmac-sha256-v1")
    signature_key_id: Mapped[str | None] = mapped_column(Text)


class TenantCredential(Base):
    """
    Stores hashed API keys for tenant authentication.
    scope='query' permits query endpoints only;
    scope='admin' permits query and metadata endpoints.
    Raw keys are never stored — only the HMAC-SHA256 digest.
    """

    __tablename__ = "tenant_credentials"
    __table_args__ = {"schema": _SCHEMA} if _SCHEMA else {}

    credential_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    tenant_id: Mapped[str] = mapped_column(Text, nullable=False)
    user_id: Mapped[str] = mapped_column(Text, nullable=False)
    key_hash: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    scope: Mapped[str] = mapped_column(
        Enum("query", "admin", name="credential_scope", schema=_SCHEMA),
        nullable=False,
    )
    description: Mapped[str | None] = mapped_column(Text)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )


class ChatSession(Base):
    """
    Tracks a unified multi-turn conversation context across model executions.
    """
    __tablename__ = "chat_sessions"
    __table_args__ = {"schema": _SCHEMA} if _SCHEMA else {}

    session_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    tenant_id: Mapped[str] = mapped_column(
        Text, nullable=False, default="default_tenant"
    )
    user_id: Mapped[str] = mapped_column(
        Text, nullable=False, default="api_user"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )

    messages = relationship(
        "ChatMessage",
        back_populates="session",
        cascade="all, delete-orphan",
        order_by="ChatMessage.sequence_number",
    )


class ChatMessage(Base):
    """
    Atomic ordered log of interaction within a particular session.
    """
    __tablename__ = "chat_messages"

    __table_args__ = (
        UniqueConstraint("session_id", "sequence_number", name="uq_session_sequence"),
        {"schema": _SCHEMA} if _SCHEMA else {}
    )

    message_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    session_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(
            f"{_SCHEMA}.chat_sessions.session_id"
            if _SCHEMA
            else "chat_sessions.session_id"
        )
    )
    sequence_number: Mapped[int] = mapped_column(nullable=False)

    role: Mapped[str] = mapped_column(
        Enum("user", "assistant", "system", name="chat_role", schema=_SCHEMA),
        nullable=False
    )
    content: Mapped[str] = mapped_column(Text, nullable=False)

    # Provider Context
    # e.g., 'ollama:llama3', 'openai:gpt-4o'
    provider_id: Mapped[str | None] = mapped_column(Text)
    prompt_tokens: Mapped[int | None] = mapped_column()
    completion_tokens: Mapped[int | None] = mapped_column()
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )

    session = relationship("ChatSession", back_populates="messages")
