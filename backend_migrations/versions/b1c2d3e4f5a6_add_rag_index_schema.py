"""Add RAG index schema: rag_enabled/cardinality_hint/rag_limit to
metadata_columns, and metadata_column_values table.

Revision ID: b1c2d3e4f5a6
Revises: 1f31f87352cc
Create Date: 2026-03-13
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "b1c2d3e4f5a6"
down_revision: str | Sequence[str] | None = "1f31f87352cc"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        "CREATE TYPE aegis_meta.rag_cardinality AS ENUM ('low', 'medium', 'high')"
    )
    op.add_column(
        "metadata_columns",
        sa.Column(
            "rag_enabled",
            sa.Boolean(),
            nullable=False,
            server_default="false",
        ),
        schema="aegis_meta",
    )
    op.add_column(
        "metadata_columns",
        sa.Column(
            "rag_cardinality_hint",
            sa.Enum(
                "low",
                "medium",
                "high",
                name="rag_cardinality",
                schema="aegis_meta",
            ),
            nullable=True,
        ),
        schema="aegis_meta",
    )
    op.add_column(
        "metadata_columns",
        sa.Column("rag_limit", sa.Integer(), nullable=True),
        schema="aegis_meta",
    )
    op.create_table(
        "metadata_column_values",
        sa.Column(
            "value_id",
            sa.dialects.postgresql.UUID(as_uuid=True),
            primary_key=True,
        ),
        sa.Column(
            "column_id",
            sa.dialects.postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "version_id",
            sa.dialects.postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("value", sa.Text(), nullable=False),
        sa.Column(
            "active",
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
        sa.ForeignKeyConstraint(
            ["version_id"],
            ["aegis_meta.metadata_versions.version_id"],
        ),
        sa.ForeignKeyConstraint(
            ["version_id", "column_id"],
            [
                "aegis_meta.metadata_columns.version_id",
                "aegis_meta.metadata_columns.column_id",
            ],
        ),
        sa.UniqueConstraint(
            "version_id", "column_id", "value", name="uq_col_value"
        ),
        schema="aegis_meta",
    )
    op.create_index(
        "ix_col_values_column_active",
        "metadata_column_values",
        ["column_id", "active"],
        schema="aegis_meta",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_col_values_column_active",
        table_name="metadata_column_values",
        schema="aegis_meta",
    )
    op.drop_table("metadata_column_values", schema="aegis_meta")
    op.drop_column("metadata_columns", "rag_limit", schema="aegis_meta")
    op.drop_column(
        "metadata_columns", "rag_cardinality_hint", schema="aegis_meta"
    )
    op.drop_column("metadata_columns", "rag_enabled", schema="aegis_meta")
    op.execute("DROP TYPE IF EXISTS aegis_meta.rag_cardinality")
