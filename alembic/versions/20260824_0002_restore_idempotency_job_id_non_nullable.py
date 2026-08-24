"""restore idempotency_record.job_id to non-nullable

Revision ID: 20260824_0002
Revises: 20260823_0001
Create Date: 2026-08-24
"""

from collections.abc import Sequence

from alembic import op

revision: str = "20260824_0002"
down_revision: str | None = "20260823_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # First, ensure no NULLs exist to avoid constraint violation
    # In a real scenario we'd delete or fix them.
    op.execute('DELETE FROM idempotency_record WHERE job_id IS NULL')
    op.alter_column("idempotency_record", "job_id", nullable=False)


def downgrade() -> None:
    op.alter_column("idempotency_record", "job_id", nullable=True)
