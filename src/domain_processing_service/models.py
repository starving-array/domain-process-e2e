from __future__ import annotations

import uuid
from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import Boolean, DateTime, Enum, ForeignKey, Index, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class TaskStatus(StrEnum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class TaskType(StrEnum):
    USER_REQUEST = "USER_REQUEST"
    REFRESH = "REFRESH"


task_status_type = Enum(TaskStatus, name="task_status")
task_type_type = Enum(TaskType, name="task_type")


class Job(Base):
    __tablename__ = "job"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    status: Mapped[TaskStatus] = mapped_column(task_status_type, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    tasks: Mapped[list[Task]] = relationship(back_populates="job")
    idempotency_record: Mapped[IdempotencyRecord | None] = relationship(back_populates="job")


class Domain(Base):
    __tablename__ = "domain"
    __table_args__ = (
        Index("idx_domain_normalized", "normalized_domain", unique=True),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    normalized_domain: Mapped[str] = mapped_column(String, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False)
    deactivated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    tasks: Mapped[list[Task]] = relationship(back_populates="domain")
    detail: Mapped[DomainDetail | None] = relationship(back_populates="domain")


class Task(Base):
    __tablename__ = "task"
    __table_args__ = (
        Index("idx_tasks_claim", "status", "next_attempt_at", "type"),
        Index("idx_tasks_lease", "status", "lease_expires_at"),
        Index("idx_tasks_job_id", "job_id"),
        Index("idx_tasks_cursor", "job_id", "created_at", "id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    job_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("job.id", ondelete="RESTRICT"),
        nullable=False,
    )
    domain_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("domain.id", ondelete="RESTRICT"),
        nullable=False,
    )
    type: Mapped[TaskType] = mapped_column(task_type_type, nullable=False)
    status: Mapped[TaskStatus] = mapped_column(task_status_type, nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False)
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    error_payload: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    job: Mapped[Job | None] = relationship(back_populates="tasks")
    domain: Mapped[Domain] = relationship(back_populates="tasks")


class DomainDetail(Base):
    __tablename__ = "domain_detail"
    __table_args__ = (Index("idx_domain_detail_refresh", "next_refresh_at"),)

    domain_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("domain.id", ondelete="RESTRICT"),
        primary_key=True,
    )
    ip_addresses: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    dns_records: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    http_status: Mapped[int | None] = mapped_column(Integer, nullable=False)
    page_title: Mapped[str | None] = mapped_column(String, nullable=False)
    response_time: Mapped[int | None] = mapped_column(Integer, nullable=False)
    response_headers: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    next_refresh_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)

    domain: Mapped[Domain] = relationship(back_populates="detail")


class IdempotencyRecord(Base):
    __tablename__ = "idempotency_record"
    __table_args__ = (
        UniqueConstraint(
            "client_id",
            "idempotency_key",
            name="uq_idempotency_record_client_id_idempotency_key",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    client_id: Mapped[str] = mapped_column(String, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String, nullable=False)
    request_hash: Mapped[str] = mapped_column(String, nullable=False)
    job_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("job.id", ondelete="RESTRICT"),
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    job: Mapped[Job] = relationship(back_populates="idempotency_record")


metadata = Base.metadata
