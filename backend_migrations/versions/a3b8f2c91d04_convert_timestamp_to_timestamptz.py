"""Convert TIMESTAMP columns to TIMESTAMPTZ

Revision ID: a3b8f2c91d04
Revises: 1f31f87352cc
Create Date: 2026-03-13

All datetime columns that store UTC values must be TIMESTAMPTZ so that
asyncpg can bind timezone-aware datetime objects without raising
'can't subtract offset-naive and offset-aware datetimes'.

The previous migration stored TIMESTAMP WITHOUT TIME ZONE but the ORM
defaults produce datetime.now(UTC) (timezone-aware) since BUG-008 was
fixed. This migration aligns the schema with the application model.

Existing rows are safe: PostgreSQL converts a bare TIMESTAMP to
TIMESTAMPTZ by treating the stored value as UTC (AT TIME ZONE 'UTC'),
which is exactly what the application has always written.
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a3b8f2c91d04"
down_revision: str | Sequence[str] | None = "1f31f87352cc"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # aegis_meta schema tables
    op.alter_column(
        "metadata_versions",
        "created_at",
        type_=sa.DateTime(timezone=True),
        schema="aegis_meta",
    )
    op.alter_column(
        "metadata_versions",
        "approved_at",
        type_=sa.DateTime(timezone=True),
        schema="aegis_meta",
    )
    op.alter_column(
        "metadata_tables",
        "created_at",
        type_=sa.DateTime(timezone=True),
        schema="aegis_meta",
    )
    op.alter_column(
        "metadata_audit",
        "timestamp",
        type_=sa.DateTime(timezone=True),
        schema="aegis_meta",
    )
    op.alter_column(
        "compiled_registry_artifacts",
        "compiled_at",
        type_=sa.DateTime(timezone=True),
        schema="aegis_meta",
    )
    # Public schema tables
    op.alter_column("chat_sessions", "created_at", type_=sa.DateTime(timezone=True))
    op.alter_column("chat_messages", "timestamp", type_=sa.DateTime(timezone=True))


def downgrade() -> None:
    op.alter_column(
        "metadata_versions",
        "created_at",
        type_=sa.DateTime(timezone=False),
        schema="aegis_meta",
    )
    op.alter_column(
        "metadata_versions",
        "approved_at",
        type_=sa.DateTime(timezone=False),
        schema="aegis_meta",
    )
    op.alter_column(
        "metadata_tables",
        "created_at",
        type_=sa.DateTime(timezone=False),
        schema="aegis_meta",
    )
    op.alter_column(
        "metadata_audit",
        "timestamp",
        type_=sa.DateTime(timezone=False),
        schema="aegis_meta",
    )
    op.alter_column(
        "compiled_registry_artifacts",
        "compiled_at",
        type_=sa.DateTime(timezone=False),
        schema="aegis_meta",
    )
    op.alter_column(
        "chat_sessions", "created_at", type_=sa.DateTime(timezone=False)
    )
    op.alter_column(
        "chat_messages", "timestamp", type_=sa.DateTime(timezone=False)
    )
