"""Job repository for data access."""

import uuid
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from domain_processing_service.models import Job, TaskStatus


class JobRepository:
    """Repository for Job persistence operations."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(self, job: Job) -> Job:
        """Insert a new Job."""
        self._session.add(job)
        await self._session.flush()
        return job

    async def get(self, job_id: uuid.UUID) -> Job | None:
        """Retrieve a Job by ID."""
        stmt = select(Job).where(Job.id == job_id)
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def update_status(self, job_id: uuid.UUID, status: TaskStatus) -> Job | None:
        """Update Job status and updated_at timestamp."""
        job = await self.get(job_id)
        if job is None:
            return None
        job.status = status
        job.updated_at = datetime.now(UTC)
        await self._session.flush()
        return job

    async def exists(self, job_id: uuid.UUID) -> bool:
        """Check if a Job exists."""
        stmt = select(Job.id).where(Job.id == job_id)
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none() is not None