"""initial schema

Revision ID: 20260822_0001
Revises:
Create Date: 2026-08-22
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "20260822_0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

task_status_enum = postgresql.ENUM(
    "PENDING",
    "PROCESSING",
    "COMPLETED",
    "FAILED",
    name="task_status",
    create_type=False,
)
task_type_enum = postgresql.ENUM(
    "USER_REQUEST",
    "REFRESH",
    name="task_type",
    create_type=False,
)


def upgrade() -> None:
    bind = op.get_bind()
    task_status_enum.create(bind, checkfirst=True)
    task_type_enum.create(bind, checkfirst=True)

    op.create_table(
        "job",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("status", task_status_enum, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "domain",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("normalized_domain", sa.String(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.Column("deactivated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "idx_domain_normalized",
        "domain",
        ["normalized_domain"],
        unique=True,
    )

    op.create_table(
        "task",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("domain_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("type", task_type_enum, nullable=False),
        sa.Column("status", task_status_enum, nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_payload", postgresql.JSONB(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["job_id"], ["job.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["domain_id"], ["domain.id"], ondelete="RESTRICT"),
    )
    op.create_index("idx_tasks_claim", "task", ["status", "next_attempt_at", "type"])
    op.create_index("idx_tasks_lease", "task", ["status", "lease_expires_at"])
    op.create_index("idx_tasks_job_id", "task", ["job_id"])

    op.create_table(
        "domain_detail",
        sa.Column("domain_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("ip_addresses", postgresql.JSONB(), nullable=False),
        sa.Column("dns_records", postgresql.JSONB(), nullable=False),
        sa.Column("http_status", sa.Integer(), nullable=True),
        sa.Column("page_title", sa.String(), nullable=True),
        sa.Column("response_time", sa.Integer(), nullable=True),
        sa.Column("response_headers", postgresql.JSONB(), nullable=False),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("next_refresh_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["domain_id"], ["domain.id"], ondelete="RESTRICT"),
    )
    op.create_index("idx_domain_detail_refresh", "domain_detail", ["next_refresh_at"])

    op.create_table(
        "idempotency_record",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("client_id", sa.String(), nullable=False),
        sa.Column("idempotency_key", sa.String(), nullable=False),
        sa.Column("request_hash", sa.String(), nullable=False),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["job_id"], ["job.id"], ondelete="RESTRICT"),
        sa.UniqueConstraint(
            "client_id",
            "idempotency_key",
            name="uq_idempotency_record_client_id_idempotency_key",
        ),
    )


def downgrade() -> None:
    op.drop_table("idempotency_record")
    op.drop_index("idx_domain_detail_refresh", table_name="domain_detail")
    op.drop_table("domain_detail")
    op.drop_index("idx_tasks_job_id", table_name="task")
    op.drop_index("idx_tasks_lease", table_name="task")
    op.drop_index("idx_tasks_claim", table_name="task")
    op.drop_table("task")
    op.drop_index("idx_domain_normalized", table_name="domain")
    op.drop_table("domain")
    op.drop_table("job")

    bind = op.get_bind()
    task_type_enum.drop(bind, checkfirst=True)
    task_status_enum.drop(bind, checkfirst=True)
