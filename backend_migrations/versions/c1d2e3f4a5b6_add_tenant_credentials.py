"""Add tenant_credentials table for API key authentication, and
credential_id audit column on metadata_audit.

Revision ID: c1d2e3f4a5b6
Revises: b1c2d3e4f5a6
Create Date: 2026-03-15
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "c1d2e3f4a5b6"
down_revision: str | Sequence[str] | None = "b1c2d3e4f5a6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # 1. Create the tenant_credentials table.
    # sa.Enum with default create_type=True will issue CREATE TYPE automatically
    # before creating the table — the standard Alembic pattern.
    op.create_table(
        "tenant_credentials",
        sa.Column(
            "credential_id",
            sa.dialects.postgresql.UUID(as_uuid=True),
            primary_key=True,
        ),
        sa.Column("tenant_id", sa.Text(), nullable=False),
        sa.Column("user_id", sa.Text(), nullable=False),
        sa.Column("key_hash", sa.Text(), nullable=False),
        sa.Column(
            "scope",
            sa.Enum(
                "query",
                "admin",
                name="credential_scope",
                schema="aegis_meta",
            ),
            nullable=False,
        ),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column(
            "is_active",
            sa.Boolean(),
            nullable=False,
            server_default="true",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        schema="aegis_meta",
    )

    # 3. UNIQUE index on key_hash for fast O(1) auth lookups
    op.create_index(
        "uq_tenant_credentials_key_hash",
        "tenant_credentials",
        ["key_hash"],
        unique=True,
        schema="aegis_meta",
    )

    # 4. Grants — registry_runtime needs SELECT for auth lookups;
    #    registry_admin needs SELECT/INSERT/UPDATE for key management
    op.execute(
        "GRANT SELECT ON aegis_meta.tenant_credentials"
        " TO user_aegis_registry_runtime"
    )
    op.execute(
        "GRANT SELECT, INSERT, UPDATE ON aegis_meta.tenant_credentials"
        " TO user_aegis_registry_admin"
    )

    # 5. Add credential_id to metadata_audit so every admin action is
    #    traceable to the specific API key that triggered it.
    #    nullable=True for backward compatibility with existing rows.
    op.add_column(
        "metadata_audit",
        sa.Column("credential_id", sa.Text(), nullable=True),
        schema="aegis_meta",
    )


def downgrade() -> None:
    op.drop_column("metadata_audit", "credential_id", schema="aegis_meta")
    op.drop_index(
        "uq_tenant_credentials_key_hash",
        table_name="tenant_credentials",
        schema="aegis_meta",
    )
    op.drop_table("tenant_credentials", schema="aegis_meta")
    op.execute("DROP TYPE IF EXISTS aegis_meta.credential_scope")
