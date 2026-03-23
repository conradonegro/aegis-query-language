"""Add sample_values column to metadata_columns

Revision ID: 0002_add_sample_values
Revises: 0001_initial_schema
Create Date: 2026-03-21
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002_add_sample_values"
down_revision: str | Sequence[str] | None = "0001_initial_schema"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "metadata_columns",
        sa.Column("sample_values", postgresql.JSONB(), nullable=True),
        schema="aegis_meta",
    )


def downgrade() -> None:
    op.drop_column("metadata_columns", "sample_values", schema="aegis_meta")
