"""Integration tests for repositories."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from domain_processing_service.models import (
    Domain,
    DomainDetail,
    IdempotencyRecord,
    Job,
    Task,
    TaskStatus,
    TaskType,
)
from domain_processing_service.repositories import (
    DomainDetailRepository,
    DomainRepository,
    IdempotencyRecordRepository,
    JobRepository,
    TaskRepository,
)


class TestJobRepository:
    """Tests for JobRepository."""

    async def test_create_and_get_job(self, async_db_session: AsyncSession) -> None:
        repo = JobRepository(async_db_session)
        now = datetime.now(UTC)
        job = Job(
            id=uuid.uuid4(),
            status=TaskStatus.PENDING,
            created_at=now,
            updated_at=now,
        )

        created = await repo.create(job)
        assert created.id == job.id
        assert created.status == TaskStatus.PENDING

        fetched = await repo.get(job.id)
        assert fetched is not None
        assert fetched.id == job.id
        assert fetched.status == TaskStatus.PENDING

    async def test_get_nonexistent_job(self, async_db_session: AsyncSession) -> None:
        repo = JobRepository(async_db_session)
        result = await repo.get(uuid.uuid4())
        assert result is None

    async def test_update_job_status(self, async_db_session: AsyncSession) -> None:
        repo = JobRepository(async_db_session)
        now = datetime.now(UTC)
        job = Job(
            id=uuid.uuid4(),
            status=TaskStatus.PENDING,
            created_at=now,
            updated_at=now,
        )
        await repo.create(job)

        updated = await repo.update_status(job.id, TaskStatus.PROCESSING)
        assert updated is not None
        assert updated.status == TaskStatus.PROCESSING
        assert updated.updated_at >= now

    async def test_update_nonexistent_job(self, async_db_session: AsyncSession) -> None:
        repo = JobRepository(async_db_session)
        result = await repo.update_status(uuid.uuid4(), TaskStatus.PROCESSING)
        assert result is None

    async def test_job_exists(self, async_db_session: AsyncSession) -> None:
        repo = JobRepository(async_db_session)
        now = datetime.now(UTC)
        job = Job(
            id=uuid.uuid4(),
            status=TaskStatus.PENDING,
            created_at=now,
            updated_at=now,
        )
        await repo.create(job)

        exists = await repo.exists(job.id)
        assert exists is True

        exists = await repo.exists(uuid.uuid4())
        assert exists is False


class TestTaskRepository:
    """Tests for TaskRepository."""

    async def test_create_and_get_task(
        self,
        async_db_session: AsyncSession,
        sample_domain: Domain,
        sample_job: Job,
    ) -> None:
        repo = TaskRepository(async_db_session)
        now = datetime.now(UTC)
        task = Task(
            id=uuid.uuid4(),
            job_id=sample_job.id,
            domain_id=sample_domain.id,
            type=TaskType.USER_REQUEST,
            status=TaskStatus.PENDING,
            attempts=0,
            next_attempt_at=now,
            created_at=now,
            updated_at=now,
        )

        created = await repo.create(task)
        assert created.id == task.id
        assert created.status == TaskStatus.PENDING

        fetched = await repo.get(task.id)
        assert fetched is not None
        assert fetched.id == task.id

    async def test_create_batch_tasks(
        self,
        async_db_session: AsyncSession,
        sample_domain: Domain,
        sample_job: Job,
    ) -> None:
        repo = TaskRepository(async_db_session)
        now = datetime.now(UTC)
        tasks = [
            Task(
                id=uuid.uuid4(),
                job_id=sample_job.id,
                domain_id=sample_domain.id,
                type=TaskType.USER_REQUEST,
                status=TaskStatus.PENDING,
                attempts=0,
                next_attempt_at=now,
                created_at=now,
                updated_at=now,
            )
            for _ in range(3)
        ]

        created = await repo.create_batch(tasks)
        assert len(created) == 3

        fetched = await repo.get_by_job_id(sample_job.id)
        assert len(fetched) == 3

    async def test_get_tasks_by_job_id(
        self,
        async_db_session: AsyncSession,
        sample_domain: Domain,
        sample_job: Job,
    ) -> None:
        repo = TaskRepository(async_db_session)
        now = datetime.now(UTC)

        task1 = Task(
            id=uuid.uuid4(),
            job_id=sample_job.id,
            domain_id=sample_domain.id,
            type=TaskType.USER_REQUEST,
            status=TaskStatus.PENDING,
            attempts=0,
            next_attempt_at=now,
            created_at=now,
            updated_at=now,
        )
        task2 = Task(
            id=uuid.uuid4(),
            job_id=sample_job.id,
            domain_id=sample_domain.id,
            type=TaskType.USER_REQUEST,
            status=TaskStatus.PENDING,
            attempts=0,
            next_attempt_at=now,
            created_at=now + timedelta(seconds=1),
            updated_at=now + timedelta(seconds=1),
        )
        await repo.create(task1)
        await repo.create(task2)

        tasks = await repo.get_by_job_id(sample_job.id)
        assert len(tasks) == 2
        # Should be ordered by created_at
        assert tasks[0].id == task1.id
        assert tasks[1].id == task2.id

    async def test_update_task_status(
        self,
        async_db_session: AsyncSession,
        sample_domain: Domain,
        sample_job: Job,
    ) -> None:
        repo = TaskRepository(async_db_session)
        now = datetime.now(UTC)
        task = Task(
            id=uuid.uuid4(),
            job_id=sample_job.id,
            domain_id=sample_domain.id,
            type=TaskType.USER_REQUEST,
            status=TaskStatus.PENDING,
            attempts=0,
            next_attempt_at=now,
            created_at=now,
            updated_at=now,
        )
        await repo.create(task)

        lease_expires = now + timedelta(seconds=120)
        updated = await repo.update_status(
            task.id,
            TaskStatus.PROCESSING,
            lease_expires_at=lease_expires,
            attempts=1,
        )
        assert updated is not None
        assert updated.status == TaskStatus.PROCESSING
        assert updated.lease_expires_at == lease_expires
        assert updated.attempts == 1
        assert updated.updated_at >= now

    async def test_update_task_with_error_payload(
        self,
        async_db_session: AsyncSession,
        sample_domain: Domain,
        sample_job: Job,
    ) -> None:
        repo = TaskRepository(async_db_session)
        now = datetime.now(UTC)
        task = Task(
            id=uuid.uuid4(),
            job_id=sample_job.id,
            domain_id=sample_domain.id,
            type=TaskType.USER_REQUEST,
            status=TaskStatus.PENDING,
            attempts=0,
            next_attempt_at=now,
            created_at=now,
            updated_at=now,
        )
        await repo.create(task)

        error_payload = {"code": "DNS_RESOLUTION_FAILED", "message": "NXDOMAIN"}
        updated = await repo.update_status(
            task.id,
            TaskStatus.FAILED,
            error_payload=error_payload,
        )
        assert updated is not None
        assert updated.status == TaskStatus.FAILED
        assert updated.error_payload == error_payload


class TestDomainRepository:
    """Tests for DomainRepository."""

    async def test_create_and_get_domain(self, async_db_session: AsyncSession) -> None:
        repo = DomainRepository(async_db_session)
        now = datetime.now(UTC)
        domain = Domain(
            id=uuid.uuid4(),
            normalized_domain="example.com",
            is_active=True,
            created_at=now,
            updated_at=now,
        )

        created = await repo.create(domain)
        assert created.normalized_domain == "example.com"
        assert created.is_active is True

        fetched = await repo.get(domain.id)
        assert fetched is not None
        assert fetched.normalized_domain == "example.com"

    async def test_get_by_normalized_domain(
        self,
        async_db_session: AsyncSession,
        sample_domain: Domain,
    ) -> None:
        repo = DomainRepository(async_db_session)
        fetched = await repo.get_by_normalized_domain("example.com")
        assert fetched is not None
        assert fetched.id == sample_domain.id

    async def test_get_by_normalized_domain_not_found(self, async_db_session: AsyncSession) -> None:
        repo = DomainRepository(async_db_session)
        result = await repo.get_by_normalized_domain("nonexistent.com")
        assert result is None

    async def test_get_or_create_existing(
        self,
        async_db_session: AsyncSession,
        sample_domain: Domain,
    ) -> None:
        repo = DomainRepository(async_db_session)
        domain = await repo.get_or_create("example.com")
        assert domain.id == sample_domain.id

    async def test_get_or_create_new(self, async_db_session: AsyncSession) -> None:
        repo = DomainRepository(async_db_session)
        domain = await repo.get_or_create("newdomain.com")
        assert domain.normalized_domain == "newdomain.com"
        assert domain.is_active is True

    async def test_update_domain(
        self,
        async_db_session: AsyncSession,
        sample_domain: Domain,
    ) -> None:
        repo = DomainRepository(async_db_session)
        deactivated_at = datetime.now(UTC)

        updated = await repo.update(
            sample_domain.id,
            is_active=False,
            deactivated_at=deactivated_at,
        )
        assert updated is not None
        assert updated.is_active is False
        assert updated.deactivated_at == deactivated_at

    async def test_duplicate_normalized_domain_rejected(
        self, async_db_session: AsyncSession
    ) -> None:
        repo = DomainRepository(async_db_session)
        now = datetime.now(UTC)

        domain1 = Domain(
            id=uuid.uuid4(),
            normalized_domain="duplicate.com",
            is_active=True,
            created_at=now,
            updated_at=now,
        )
        await repo.create(domain1)

        domain2 = Domain(
            id=uuid.uuid4(),
            normalized_domain="duplicate.com",
            is_active=True,
            created_at=now,
            updated_at=now,
        )
        async_db_session.add(domain2)
        with pytest.raises(IntegrityError):
            await async_db_session.flush()


class TestDomainDetailRepository:
    """Tests for DomainDetailRepository."""

    async def test_create_and_get_domain_detail(
        self,
        async_db_session: AsyncSession,
        sample_domain: Domain,
    ) -> None:
        repo = DomainDetailRepository(async_db_session)
        now = datetime.now(UTC)
        detail = DomainDetail(
            domain_id=sample_domain.id,
            ip_addresses=["93.184.216.34"],
            dns_records={"A": ["93.184.216.34"]},
            http_status=200,
            page_title="Example Domain",
            response_time=120,
            response_headers={"content-type": "text/html"},
            fetched_at=now,
            next_refresh_at=now + timedelta(days=14),
            version=1,
        )

        created = await repo.create(detail)
        assert created.domain_id == sample_domain.id
        assert created.version == 1

        fetched = await repo.get(sample_domain.id)
        assert fetched is not None
        assert fetched.ip_addresses == ["93.184.216.34"]
        assert fetched.page_title == "Example Domain"

    async def test_upsert_domain_detail(
        self,
        async_db_session: AsyncSession,
        sample_domain: Domain,
    ) -> None:
        repo = DomainDetailRepository(async_db_session)
        now = datetime.now(UTC)

        # Initial insert
        detail = DomainDetail(
            domain_id=sample_domain.id,
            ip_addresses=["1.1.1.1"],
            dns_records={"A": ["1.1.1.1"]},
            http_status=200,
            page_title="Original",
            response_time=100,
            response_headers={},
            fetched_at=now,
            next_refresh_at=now + timedelta(days=14),
            version=1,
        )
        await repo.upsert(detail)

        # Update via upsert
        now2 = datetime.now(UTC)
        detail2 = DomainDetail(
            domain_id=sample_domain.id,
            ip_addresses=["2.2.2.2"],
            dns_records={"A": ["2.2.2.2"]},
            http_status=200,
            page_title="Updated",
            response_time=150,
            response_headers={},
            fetched_at=now2,
            next_refresh_at=now2 + timedelta(days=14),
            version=2,
        )
        updated = await repo.upsert(detail2)
        assert updated is not None
        assert updated.ip_addresses == ["2.2.2.2"]
        assert updated.page_title == "Updated"
        assert updated.version == 2

    async def test_get_stale_domains(
        self,
        async_db_session: AsyncSession,
        sample_domain: Domain,
    ) -> None:
        repo = DomainDetailRepository(async_db_session)
        now = datetime.now(UTC)

        # Create a stale domain detail
        detail = DomainDetail(
            domain_id=sample_domain.id,
            ip_addresses=["1.1.1.1"],
            dns_records={"A": ["1.1.1.1"]},
            http_status=200,
            page_title="Stale",
            response_time=100,
            response_headers={},
            fetched_at=now - timedelta(days=15),
            next_refresh_at=now - timedelta(days=1),
            version=1,
        )
        await repo.create(detail)

        # Create a fresh domain detail
        fresh_domain = Domain(
            id=uuid.uuid4(),
            normalized_domain="fresh.com",
            is_active=True,
            created_at=now,
            updated_at=now,
        )
        async_db_session.add(fresh_domain)
        await async_db_session.flush()

        fresh_detail = DomainDetail(
            domain_id=fresh_domain.id,
            ip_addresses=["2.2.2.2"],
            dns_records={"A": ["2.2.2.2"]},
            http_status=200,
            page_title="Fresh",
            response_time=100,
            response_headers={},
            fetched_at=now,
            next_refresh_at=now + timedelta(days=14),
            version=1,
        )
        await repo.create(fresh_detail)

        stale = await repo.get_stale_domains(limit=10, now=now)
        assert len(stale) == 1
        assert stale[0].domain_id == sample_domain.id


class TestIdempotencyRecordRepository:
    """Tests for IdempotencyRecordRepository."""

    async def test_create_and_get_idempotency_record(
        self,
        async_db_session: AsyncSession,
        sample_job: Job,
    ) -> None:
        repo = IdempotencyRecordRepository(async_db_session)
        now = datetime.now(UTC)
        record = IdempotencyRecord(
            id=uuid.uuid4(),
            client_id="client1",
            idempotency_key="key1",
            request_hash="hash1",
            job_id=sample_job.id,
            created_at=now,
        )

        created = await repo.create(record)
        assert created.client_id == "client1"
        assert created.idempotency_key == "key1"

        fetched = await repo.get("client1", "key1")
        assert fetched is not None
        assert fetched.job_id == sample_job.id

    async def test_get_nonexistent_record(self, async_db_session: AsyncSession) -> None:
        repo = IdempotencyRecordRepository(async_db_session)
        result = await repo.get("client1", "nonexistent")
        assert result is None

    async def test_upsert_new_record(
        self,
        async_db_session: AsyncSession,
        sample_job: Job,
    ) -> None:
        repo = IdempotencyRecordRepository(async_db_session)
        now = datetime.now(UTC)
        record = IdempotencyRecord(
            id=uuid.uuid4(),
            client_id="client1",
            idempotency_key="newkey",
            request_hash="hash1",
            job_id=sample_job.id,
            created_at=now,
        )

        result, created = await repo.upsert(record)
        assert created is True
        assert result.id == record.id

    async def test_upsert_existing_record(
        self,
        async_db_session: AsyncSession,
        sample_job: Job,
    ) -> None:
        repo = IdempotencyRecordRepository(async_db_session)
        now = datetime.now(UTC)

        # Create initial record
        record1 = IdempotencyRecord(
            id=uuid.uuid4(),
            client_id="client1",
            idempotency_key="key1",
            request_hash="hash1",
            job_id=sample_job.id,
            created_at=now,
        )
        await repo.create(record1)

        # Try to upsert with same key but different hash
        record2 = IdempotencyRecord(
            id=uuid.uuid4(),
            client_id="client1",
            idempotency_key="key1",
            request_hash="hash2",
            job_id=uuid.uuid4(),
            created_at=now,
        )
        result, created = await repo.upsert(record2)
        assert created is False
        assert result.id == record1.id
        assert result.request_hash == "hash1"
        assert result.job_id == sample_job.id

    async def test_duplicate_idempotency_key_rejected_on_create(
        self,
        async_db_session: AsyncSession,
        sample_job: Job,
    ) -> None:
        repo = IdempotencyRecordRepository(async_db_session)
        now = datetime.now(UTC)

        record1 = IdempotencyRecord(
            id=uuid.uuid4(),
            client_id="client1",
            idempotency_key="key1",
            request_hash="hash1",
            job_id=sample_job.id,
            created_at=now,
        )
        await repo.create(record1)

        record2 = IdempotencyRecord(
            id=uuid.uuid4(),
            client_id="client1",
            idempotency_key="key1",
            request_hash="hash2",
            job_id=uuid.uuid4(),
            created_at=now,
        )
        async_db_session.add(record2)
        with pytest.raises(IntegrityError):
            await async_db_session.flush()