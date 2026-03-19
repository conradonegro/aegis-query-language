"""Enforce at most one active version per tenant

Revision ID: 0002_one_active_version_per_tenant
Revises: 0001_initial_schema
Create Date: 2026-03-19

Adds a partial unique index on metadata_versions(tenant_id) WHERE status = 'active'.
This makes the DB the authoritative arbiter of the one-active-per-tenant invariant,
complementing the application-level atomic swap in update_version_status.

PostgreSQL partial unique indexes are not supported by SQLite (used in tests).
The application-level pre-check and IntegrityError handler cover the SQLite path.
"""
from collections.abc import Sequence

from alembic import op

revision: str = "0002_one_active_version_per_tenant"
down_revision: str | Sequence[str] | None = "0001_initial_schema"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("""
        CREATE UNIQUE INDEX uq_one_active_version_per_tenant
            ON aegis_meta.metadata_versions (tenant_id)
            WHERE (status = 'active')
    """)


def downgrade() -> None:
    op.execute(
        "DROP INDEX IF EXISTS aegis_meta.uq_one_active_version_per_tenant"
    )
