"""Add source_database column to metadata_tables.

Revision ID: d4e5f6a7b8c9
Revises: c1d2e3f4a5b6
Create Date: 2026-03-17

Adds a nullable TEXT column `source_database` to `aegis_meta.metadata_tables`.
This field identifies which logical database a table belongs to (e.g. "financial",
"formula_1" for the BIRD benchmark). NULL means "not assigned", which is valid for
any non-BIRD or general-purpose schema — no backfill is required.

An index is created for future queries that filter tables by source database.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "d4e5f6a7b8c9"
down_revision: str | Sequence[str] | None = "c1d2e3f4a5b6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "metadata_tables",
        sa.Column("source_database", sa.Text(), nullable=True),
        schema="aegis_meta",
    )
    op.create_index(
        "ix_metadata_tables_source_database",
        "metadata_tables",
        ["source_database"],
        schema="aegis_meta",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_metadata_tables_source_database",
        table_name="metadata_tables",
        schema="aegis_meta",
    )
    op.drop_column("metadata_tables", "source_database", schema="aegis_meta")
