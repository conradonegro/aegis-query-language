"""Initial consolidated schema

Revision ID: 0001_initial_schema
Revises:
Create Date: 2026-03-18

Consolidated baseline migration. Replaces 7 iterative migrations.
Key additions vs the old history:
  - tenant_id TEXT NOT NULL on metadata_versions and compiled_registry_artifacts
  - Composite FK (version_id, tenant_id) from compiled_registry_artifacts →
    metadata_versions (with uq_version_tenant on the target side)
  - All timestamps as TIMESTAMPTZ
  - ChatSession.tenant_id has no default (must be supplied by application layer)
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_initial_schema"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # --- metadata_audit ---
    op.create_table(
        "metadata_audit",
        sa.Column("audit_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("version_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("actor", sa.Text(), nullable=False),
        sa.Column(
            "action",
            sa.Enum(
                "create", "update", "approve", "deploy", "revoke",
                name="audit_action", schema="aegis_meta",
            ),
            nullable=False,
        ),
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "timestamp",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column("previous_hash", sa.Text(), nullable=True),
        sa.Column("row_hash", sa.Text(), nullable=False, unique=True),
        sa.Column("hash_algorithm", sa.Text(), nullable=False),
        sa.Column("key_id", sa.Text(), nullable=True),
        sa.Column("credential_id", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("audit_id"),
        schema="aegis_meta",
    )

    # --- metadata_versions (with tenant_id NOT NULL) ---
    op.create_table(
        "metadata_versions",
        sa.Column("version_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", sa.Text(), nullable=False),
        sa.Column("registry_hash", sa.Text(), nullable=True),
        sa.Column(
            "status",
            sa.Enum(
                "draft", "pending_review", "active", "archived",
                name="version_status", schema="aegis_meta",
            ),
            nullable=False,
        ),
        sa.Column("created_by", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("approved_by", sa.Text(), nullable=True),
        sa.Column("approved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("change_reason", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("version_id"),
        schema="aegis_meta",
    )
    # Required by the composite FK from compiled_registry_artifacts
    op.create_unique_constraint(
        "uq_version_tenant",
        "metadata_versions",
        ["version_id", "tenant_id"],
        schema="aegis_meta",
    )

    # --- compiled_registry_artifacts (with tenant_id NOT NULL) ---
    op.create_table(
        "compiled_registry_artifacts",
        sa.Column("artifact_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("version_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", sa.Text(), nullable=False),
        sa.Column(
            "artifact_blob",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("artifact_hash", sa.Text(), nullable=False),
        sa.Column("compiled_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("compiler_version", sa.Text(), nullable=False),
        sa.Column("signature", sa.Text(), nullable=True),
        sa.Column("signature_algo", sa.Text(), nullable=False),
        sa.Column("signature_key_id", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["version_id"],
            ["aegis_meta.metadata_versions.version_id"],
        ),
        sa.PrimaryKeyConstraint("artifact_id"),
        sa.UniqueConstraint("version_id"),
        schema="aegis_meta",
    )
    # Composite FK enforces tenant_id consistency at DB layer
    op.create_foreign_key(
        "fk_artifact_version_tenant",
        "compiled_registry_artifacts",
        "metadata_versions",
        ["version_id", "tenant_id"],
        ["version_id", "tenant_id"],
        source_schema="aegis_meta",
        referent_schema="aegis_meta",
    )

    # --- metadata_tables ---
    op.create_table(
        "metadata_tables",
        sa.Column("table_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("version_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("real_name", sa.Text(), nullable=False),
        sa.Column("alias", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("tenant_id", sa.Text(), nullable=True),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("source_database", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["version_id"], ["aegis_meta.metadata_versions.version_id"],
        ),
        sa.PrimaryKeyConstraint("table_id"),
        sa.UniqueConstraint("version_id", "alias", name="uq_table_alias"),
        sa.UniqueConstraint("version_id", "real_name", name="uq_table_real_name"),
        sa.UniqueConstraint("version_id", "table_id", name="uq_table_composite_id"),
        schema="aegis_meta",
    )

    # --- metadata_columns ---
    op.create_table(
        "metadata_columns",
        sa.Column("column_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("version_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("table_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("real_name", sa.Text(), nullable=False),
        sa.Column("alias", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("data_type", sa.Text(), nullable=False),
        sa.Column("is_nullable", sa.Boolean(), nullable=False),
        sa.Column("is_primary_key", sa.Boolean(), nullable=False),
        sa.Column("is_unique", sa.Boolean(), nullable=False),
        sa.Column("is_sensitive", sa.Boolean(), nullable=False),
        sa.Column("allowed_in_select", sa.Boolean(), nullable=False),
        sa.Column("allowed_in_filter", sa.Boolean(), nullable=False),
        sa.Column("allowed_in_join", sa.Boolean(), nullable=False),
        sa.Column(
            "safety_classification",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("rag_enabled", sa.Boolean(), nullable=False),
        sa.Column(
            "rag_cardinality_hint",
            sa.Enum(
                "low", "medium", "high",
                name="rag_cardinality", schema="aegis_meta",
            ),
            nullable=True,
        ),
        sa.Column("rag_limit", sa.Integer(), nullable=True),
        sa.Column("rag_sample_strategy", sa.Text(), nullable=True),
        sa.Column("rag_order_by_column", sa.Text(), nullable=True),
        sa.Column("rag_order_direction", sa.Text(), nullable=True),
        sa.Column(
            "refresh_on_compile",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column(
            "sample_values",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "sample_values_exhaustive",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.ForeignKeyConstraint(
            ["version_id", "table_id"],
            [
                "aegis_meta.metadata_tables.version_id",
                "aegis_meta.metadata_tables.table_id",
            ],
        ),
        sa.ForeignKeyConstraint(
            ["version_id"], ["aegis_meta.metadata_versions.version_id"],
        ),
        sa.PrimaryKeyConstraint("column_id"),
        sa.UniqueConstraint("version_id", "column_id", name="uq_col_composite_id"),
        sa.UniqueConstraint("version_id", "table_id", "alias", name="uq_col_alias"),
        sa.UniqueConstraint(
            "version_id", "table_id", "real_name", name="uq_col_real_name"
        ),
        schema="aegis_meta",
    )

    # --- metadata_column_values ---
    op.create_table(
        "metadata_column_values",
        sa.Column("value_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("column_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("version_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("value", sa.Text(), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["version_id"], ["aegis_meta.metadata_versions.version_id"],
        ),
        sa.ForeignKeyConstraint(
            ["version_id", "column_id"],
            [
                "aegis_meta.metadata_columns.version_id",
                "aegis_meta.metadata_columns.column_id",
            ],
        ),
        sa.PrimaryKeyConstraint("value_id"),
        sa.UniqueConstraint("version_id", "column_id", "value", name="uq_col_value"),
        schema="aegis_meta",
    )

    # --- metadata_relationships ---
    op.create_table(
        "metadata_relationships",
        sa.Column("relationship_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("version_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_table_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_column_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("target_table_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("target_column_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "relationship_type",
            sa.Enum(
                "fk", "logical", "denormalized",
                name="rel_type", schema="aegis_meta",
            ),
            nullable=False,
        ),
        sa.Column(
            "cardinality",
            sa.Enum(
                "1:1", "1:n", "n:1", "n:m",
                name="cardinality_type", schema="aegis_meta",
            ),
            nullable=False,
        ),
        sa.Column("bidirectional", sa.Boolean(), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.ForeignKeyConstraint(
            ["version_id", "source_column_id"],
            [
                "aegis_meta.metadata_columns.version_id",
                "aegis_meta.metadata_columns.column_id",
            ],
        ),
        sa.ForeignKeyConstraint(
            ["version_id", "target_column_id"],
            [
                "aegis_meta.metadata_columns.version_id",
                "aegis_meta.metadata_columns.column_id",
            ],
        ),
        sa.ForeignKeyConstraint(
            ["version_id"], ["aegis_meta.metadata_versions.version_id"],
        ),
        sa.PrimaryKeyConstraint("relationship_id"),
        schema="aegis_meta",
    )

    # --- tenant_credentials ---
    op.create_table(
        "tenant_credentials",
        sa.Column("credential_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", sa.Text(), nullable=False),
        sa.Column("user_id", sa.Text(), nullable=False),
        sa.Column("key_hash", sa.Text(), nullable=False, unique=True),
        sa.Column(
            "scope",
            sa.Enum(
                "query", "admin",
                name="credential_scope", schema="aegis_meta",
            ),
            nullable=False,
        ),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("credential_id"),
        schema="aegis_meta",
    )

    # --- chat_sessions ---
    op.create_table(
        "chat_sessions",
        sa.Column("session_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", sa.Text(), nullable=False),
        sa.Column("user_id", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("session_id"),
        schema="aegis_meta",
    )

    # --- chat_messages ---
    op.create_table(
        "chat_messages",
        sa.Column("message_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("session_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("sequence_number", sa.Integer(), nullable=False),
        sa.Column(
            "role",
            sa.Enum(
                "user", "assistant", "system",
                name="chat_role", schema="aegis_meta",
            ),
            nullable=False,
        ),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("provider_id", sa.Text(), nullable=True),
        sa.Column("prompt_tokens", sa.Integer(), nullable=True),
        sa.Column("completion_tokens", sa.Integer(), nullable=True),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["session_id"], ["aegis_meta.chat_sessions.session_id"],
        ),
        sa.PrimaryKeyConstraint("message_id"),
        sa.UniqueConstraint(
            "session_id", "sequence_number", name="uq_session_sequence"
        ),
        schema="aegis_meta",
    )

    # Grant privileges for application roles
    op.execute("GRANT USAGE ON SCHEMA aegis_meta TO user_aegis_runtime")
    op.execute("GRANT USAGE ON SCHEMA aegis_meta TO user_aegis_registry_runtime")
    op.execute("GRANT USAGE ON SCHEMA aegis_meta TO user_aegis_steward")
    op.execute("GRANT USAGE ON SCHEMA aegis_meta TO user_aegis_registry_admin")

    op.execute(
        "GRANT SELECT ON aegis_meta.metadata_versions TO user_aegis_registry_runtime"
    )
    op.execute(
        "GRANT SELECT ON aegis_meta.compiled_registry_artifacts"
        " TO user_aegis_registry_runtime"
    )
    op.execute(
        "GRANT SELECT ON aegis_meta.metadata_column_values"
        " TO user_aegis_registry_runtime"
    )
    op.execute(
        "GRANT SELECT, INSERT ON aegis_meta.chat_sessions TO user_aegis_runtime"
    )
    op.execute(
        "GRANT SELECT, INSERT ON aegis_meta.chat_messages TO user_aegis_runtime"
    )
    op.execute(
        "GRANT SELECT ON aegis_meta.tenant_credentials TO user_aegis_runtime"
    )
    op.execute(
        "GRANT SELECT ON aegis_meta.tenant_credentials"
        " TO user_aegis_registry_runtime"
    )

    op.execute(
        "GRANT SELECT, INSERT, UPDATE ON aegis_meta.metadata_versions"
        " TO user_aegis_steward"
    )
    op.execute(
        "GRANT SELECT, INSERT, UPDATE ON aegis_meta.metadata_tables"
        " TO user_aegis_steward"
    )
    op.execute(
        "GRANT SELECT, INSERT, UPDATE ON aegis_meta.metadata_columns"
        " TO user_aegis_steward"
    )
    op.execute(
        "GRANT SELECT, INSERT, UPDATE ON aegis_meta.metadata_column_values"
        " TO user_aegis_steward"
    )
    op.execute(
        "GRANT SELECT, INSERT, UPDATE ON aegis_meta.metadata_relationships"
        " TO user_aegis_steward"
    )
    op.execute(
        "GRANT SELECT, INSERT ON aegis_meta.metadata_audit TO user_aegis_steward"
    )

    op.execute(
        "GRANT SELECT, INSERT, UPDATE ON aegis_meta.metadata_versions"
        " TO user_aegis_registry_admin"
    )
    op.execute(
        "GRANT SELECT, INSERT, DELETE ON aegis_meta.compiled_registry_artifacts"
        " TO user_aegis_registry_admin"
    )
    op.execute(
        "GRANT SELECT ON aegis_meta.metadata_tables TO user_aegis_registry_admin"
    )
    op.execute(
        "GRANT SELECT ON aegis_meta.metadata_columns TO user_aegis_registry_admin"
    )
    op.execute(
        "GRANT SELECT ON aegis_meta.metadata_column_values"
        " TO user_aegis_registry_admin"
    )
    op.execute(
        "GRANT SELECT, INSERT ON aegis_meta.metadata_audit TO user_aegis_registry_admin"
    )
    op.execute(
        "GRANT SELECT, INSERT, UPDATE ON aegis_meta.tenant_credentials"
        " TO user_aegis_registry_admin"
    )

    # Enforce at most one active version per tenant
    op.execute("""
        CREATE UNIQUE INDEX uq_one_active_version_per_tenant
            ON aegis_meta.metadata_versions (tenant_id)
            WHERE (status = 'active')
    """)

    # Prevent WORM audit chain forks under concurrent admin writes.
    # Two concurrent transactions could otherwise read the same chain tip
    # and commit different rows pointing at the same previous_hash, branching
    # the audit history. The partial unique index rejects the second writer;
    # the application retries against the new tip (see app/api/compiler.py
    # and app/api/router.py). The index is partial so the genesis row
    # (previous_hash = '') is exempt from uniqueness.
    op.execute("""
        CREATE UNIQUE INDEX uq_audit_previous_hash_nonempty
            ON aegis_meta.metadata_audit (previous_hash)
            WHERE previous_hash != ''
    """)

    # WORM audit trigger: prevent UPDATE and DELETE on metadata_audit
    op.execute("""
        CREATE OR REPLACE FUNCTION aegis_meta.prevent_audit_mutation()
        RETURNS TRIGGER LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'WORM violation: audit records are immutable';
        END;
        $$
    """)
    op.execute("""
        CREATE TRIGGER trg_audit_worm
        BEFORE UPDATE OR DELETE ON aegis_meta.metadata_audit
        FOR EACH ROW EXECUTE FUNCTION aegis_meta.prevent_audit_mutation()
    """)


def downgrade() -> None:
    op.execute(
        "DROP INDEX IF EXISTS aegis_meta.uq_audit_previous_hash_nonempty"
    )
    op.execute(
        "DROP INDEX IF EXISTS aegis_meta.uq_one_active_version_per_tenant"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS trg_audit_worm ON aegis_meta.metadata_audit"
    )
    op.execute(
        "DROP FUNCTION IF EXISTS aegis_meta.prevent_audit_mutation()"
    )
    op.drop_table("chat_messages", schema="aegis_meta")
    op.drop_table("chat_sessions", schema="aegis_meta")
    op.drop_table("tenant_credentials", schema="aegis_meta")
    op.drop_table("metadata_relationships", schema="aegis_meta")
    op.drop_table("metadata_column_values", schema="aegis_meta")
    op.drop_table("metadata_columns", schema="aegis_meta")
    op.drop_table("metadata_tables", schema="aegis_meta")
    op.drop_constraint(
        "fk_artifact_version_tenant",
        "compiled_registry_artifacts",
        schema="aegis_meta",
        type_="foreignkey",
    )
    op.drop_table("compiled_registry_artifacts", schema="aegis_meta")
    op.drop_constraint(
        "uq_version_tenant",
        "metadata_versions",
        schema="aegis_meta",
        type_="unique",
    )
    op.drop_table("metadata_versions", schema="aegis_meta")
    op.drop_table("metadata_audit", schema="aegis_meta")
    op.execute("DROP TYPE IF EXISTS aegis_meta.chat_role")
    op.execute("DROP TYPE IF EXISTS aegis_meta.credential_scope")
    op.execute("DROP TYPE IF EXISTS aegis_meta.rag_cardinality")
    op.execute("DROP TYPE IF EXISTS aegis_meta.cardinality_type")
    op.execute("DROP TYPE IF EXISTS aegis_meta.rel_type")
    op.execute("DROP TYPE IF EXISTS aegis_meta.audit_action")
    op.execute("DROP TYPE IF EXISTS aegis_meta.version_status")
