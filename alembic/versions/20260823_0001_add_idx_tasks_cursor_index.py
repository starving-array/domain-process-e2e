"""add idx_tasks_cursor index

Revision ID: 20260823_0001
Revises: 20260822_0001
Create Date: 2026-08-23
"""

from collections.abc import Sequence

from alembic import op

revision: str = "20260823_0001"
down_revision: str | None = "20260822_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index("idx_tasks_cursor", "task", ["job_id", "created_at", "id"])


def downgrade() -> None:
    op.drop_index("idx_tasks_cursor", table_name="task")