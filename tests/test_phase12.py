"""Phase 12: Idempotency, OCC, and Soft Deactivation Tests."""

import json
import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest
import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from domain_processing_service.config import AppSettings
from domain_processing_service.models import Domain, DomainDetail, Task, TaskStatus, TaskType
from domain_processing_service.repositories import DomainDetailRepository, TaskRepository, DomainRepository, IdempotencyRecordRepository


class TestIdempotency:
    """Tests for Idempotency-Key functionality."""

    async def test_first_request_with_idempotency_key_creates_job(
        self, async_client: httpx.AsyncClient
    ) -> None:
        """First request with Idempotency-Key should create a job and return 202."""
        response = await async_client.post(
            "/jobs",
            json={"domains": ["example.com"]},
            headers={"Idempotency-Key": "test-key-123", "X-Client-ID": "client-1"},
        )
        assert response.status_code == 202
        data = response.json()
        assert "jobId" in data
        assert data["status"] == "PENDING"

    async def test_repeated_request_same_key_same_payload_returns_200(
        self, async_client: httpx.AsyncClient
    ) -> None:
        """Repeated request with same key and payload should return 200 with existing job."""
        # First request
        response1 = await async_client.post(
            "/jobs",
            json={"domains": ["example.com"]},
            headers={"Idempotency-Key": "test-key-456", "X-Client-ID": "client-1"},
        )
        assert response1.status_code == 202
        job_id = response1.json()["jobId"]

        # Second request with same key and payload
        response2 = await async_client.post(
            "/jobs",
            json={"domains": ["example.com"]},
            headers={"Idempotency-Key": "test-key-456", "X-Client-ID": "client-1"},
        )
        assert response2.status_code == 200
        assert response2.json()["jobId"] == job_id

    async def test_repeated_request_same_key_different_payload_returns_409(
        self, async_client: httpx.AsyncClient
    ) -> None:
        """Repeated request with same key but different payload should return 409."""
        # First request
        response1 = await async_client.post(
            "/jobs",
            json={"domains": ["example.com"]},
            headers={"Idempotency-Key": "test-key-789", "X-Client-ID": "client-1"},
        )
        assert response1.status_code == 202

        # Second request with same key but different payload
        response2 = await async_client.post(
            "/jobs",
            json={"domains": ["different.com"]},
            headers={"Idempotency-Key": "test-key-789", "X-Client-ID": "client-1"},
        )
        assert response2.status_code == 409
        assert "conflict" in response2.json()["detail"].lower()

    async def test_different_client_ids_with_same_key_are_independent(
        self, async_client: httpx.AsyncClient
    ) -> None:
        """Same Idempotency-Key with different client IDs should be independent."""
        # Client 1
        response1 = await async_client.post(
            "/jobs",
            json={"domains": ["example.com"]},
            headers={"Idempotency-Key": "shared-key", "X-Client-ID": "client-1"},
        )
        assert response1.status_code == 202
        job_id_1 = response1.json()["jobId"]

        # Client 2 with same key
        response2 = await async_client.post(
            "/jobs",
            json={"domains": ["example.com"]},
            headers={"Idempotency-Key": "shared-key", "X-Client-ID": "client-2"},
        )
        assert response2.status_code == 202
        job_id_2 = response2.json()["jobId"]

        assert job_id_1 != job_id_2

    async def test_request_without_idempotency_key_works_normally(
        self, async_client: httpx.AsyncClient
    ) -> None:
        """Request without Idempotency-Key should work normally (202)."""
        response = await async_client.post(
            "/jobs",
            json={"domains": ["example.com"]},
        )
        assert response.status_code == 202

    async def test_idempotency_with_multiple_domains(
        self, async_client: httpx.AsyncClient
    ) -> None:
        """Idempotency should work with multiple domains in the payload."""
        payload = {"domains": ["example.com", "google.com", "github.com"]}
        
        response1 = await async_client.post(
            "/jobs",
            json=payload,
            headers={"Idempotency-Key": "multi-domain-key", "X-Client-ID": "client-1"},
        )
        assert response1.status_code == 202
        job_id = response1.json()["jobId"]

        response2 = await async_client.post(
            "/jobs",
            json=payload,
            headers={"Idempotency-Key": "multi-domain-key", "X-Client-ID": "client-1"},
        )
        assert response2.status_code == 200
        assert response2.json()["jobId"] == job_id


class TestOCC:
    """Tests for Optimistic Concurrency Control on DomainDetail."""

    @pytest.fixture()
    async def domain_with_detail(
        self, async_db_session: AsyncSession
    ) -> tuple[Domain, DomainDetail]:
        """Create a domain with an existing DomainDetail."""
        now = datetime.now(UTC)
        domain = Domain(
            id=uuid.uuid4(),
            normalized_domain="occ-test.example.com",
            is_active=True,
            created_at=now,
            updated_at=now,
        )
        async_db_session.add(domain)
        await async_db_session.flush()

        detail = DomainDetail(
            domain_id=domain.id,
            ip_addresses=["93.184.216.34"],
            dns_records={"A": ["93.184.216.34"]},
            http_status=200,
            page_title="Original Title",
            response_time=100,
            response_headers={},
            fetched_at=now - timedelta(hours=1),
            next_refresh_at=now + timedelta(days=1),
            version=5,
        )
        async_db_session.add(detail)
        await async_db_session.commit()
        await async_db_session.refresh(domain)
        await async_db_session.refresh(detail)
        return domain, detail

    async def test_occ_update_with_correct_version_succeeds(
        self, async_db_session: AsyncSession, domain_with_detail: tuple[Domain, DomainDetail]
    ) -> None:
        """Update with correct expected version should succeed and increment version."""
        domain, detail = domain_with_detail
        repo = DomainDetailRepository(async_db_session)

        new_detail = DomainDetail(
            domain_id=domain.id,
            ip_addresses=["93.184.216.34"],
            dns_records={"A": ["93.184.216.34"]},
            http_status=200,
            page_title="Updated Title",
            response_time=150,
            response_headers={},
            fetched_at=datetime.now(UTC),
            next_refresh_at=datetime.now(UTC) + timedelta(days=1),
            version=6,  # Expected version + 1
        )

        updated = await repo.upsert_with_occ(new_detail, expected_version=5)
        assert updated.version == 6
        assert updated.page_title == "Updated Title"

    async def test_occ_update_with_stale_version_fails(
        self, async_db_session: AsyncSession, domain_with_detail: tuple[Domain, DomainDetail]
    ) -> None:
        """Update with stale expected version should raise RuntimeError."""
        domain, detail = domain_with_detail
        repo = DomainDetailRepository(async_db_session)

        new_detail = DomainDetail(
            domain_id=domain.id,
            ip_addresses=["93.184.216.34"],
            dns_records={"A": ["93.184.216.34"]},
            http_status=200,
            page_title="Stale Update",
            response_time=150,
            response_headers={},
            fetched_at=datetime.now(UTC),
            next_refresh_at=datetime.now(UTC) + timedelta(days=1),
            version=7,  # This will be ignored, expected_version=4 is stale
        )

        with pytest.raises(RuntimeError, match="OCC conflict"):
            await repo.upsert_with_occ(new_detail, expected_version=4)

    async def test_occ_insert_new_record_succeeds(
        self, async_db_session: AsyncSession
    ) -> None:
        """Inserting new DomainDetail (no existing record) should succeed with version=1."""
        now = datetime.now(UTC)
        domain = Domain(
            id=uuid.uuid4(),
            normalized_domain="new-occ.example.com",
            is_active=True,
            created_at=now,
            updated_at=now,
        )
        async_db_session.add(domain)
        await async_db_session.commit()

        repo = DomainDetailRepository(async_db_session)
        new_detail = DomainDetail(
            domain_id=domain.id,
            ip_addresses=["1.2.3.4"],
            dns_records={"A": ["1.2.3.4"]},
            http_status=200,
            page_title="First Version",
            response_time=50,
            response_headers={},
            fetched_at=now,
            next_refresh_at=now + timedelta(days=1),
            version=1,  # Will be ignored for insert
        )

        created = await repo.upsert_with_occ(new_detail, expected_version=0)
        assert created.version == 1
        assert created.page_title == "First Version"

    async def test_occ_concurrent_updates_one_succeeds_one_fails(
        self, async_db_session: AsyncSession, domain_with_detail: tuple[Domain, DomainDetail]
    ) -> None:
        """Simulate two concurrent updates - one should succeed, one should fail."""
        domain, detail = domain_with_detail
        repo = DomainDetailRepository(async_db_session)

        # First update succeeds
        update1 = DomainDetail(
            domain_id=domain.id,
            ip_addresses=["93.184.216.34"],
            dns_records={"A": ["93.184.216.34"]},
            http_status=200,
            page_title="Update 1",
            response_time=100,
            response_headers={},
            fetched_at=datetime.now(UTC),
            next_refresh_at=datetime.now(UTC) + timedelta(days=1),
            version=6,
        )
        await repo.upsert_with_occ(update1, expected_version=5)

        # Second update with same old expected_version should fail
        update2 = DomainDetail(
            domain_id=domain.id,
            ip_addresses=["93.184.216.34"],
            dns_records={"A": ["93.184.216.34"]},
            http_status=200,
            page_title="Update 2 (stale)",
            response_time=100,
            response_headers={},
            fetched_at=datetime.now(UTC),
            next_refresh_at=datetime.now(UTC) + timedelta(days=1),
            version=6,
        )
        with pytest.raises(RuntimeError, match="OCC conflict"):
            await repo.upsert_with_occ(update2, expected_version=5)


class TestSoftDeactivation:
    """Tests for Soft Deactivation functionality."""

    @pytest.fixture()
    async def active_domain_with_detail(
        self, async_db_session: AsyncSession
    ) -> Domain:
        """Create an active domain with DomainDetail."""
        now = datetime.now(UTC)
        domain = Domain(
            id=uuid.uuid4(),
            normalized_domain="deactivate-test.example.com",
            is_active=True,
            created_at=now,
            updated_at=now,
        )
        async_db_session.add(domain)
        await async_db_session.flush()

        detail = DomainDetail(
            domain_id=domain.id,
            ip_addresses=["93.184.216.34"],
            dns_records={"A": ["93.184.216.34"]},
            http_status=200,
            page_title="Test Domain",
            response_time=100,
            response_headers={},
            fetched_at=now,
            next_refresh_at=now + timedelta(days=1),
            version=1,
        )
        async_db_session.add(detail)
        await async_db_session.commit()
        await async_db_session.refresh(domain)
        return domain

    async def test_domain_can_be_deactivated(
        self, async_db_session: AsyncSession, active_domain_with_detail: Domain
    ) -> None:
        """Domain can be soft deactivated."""
        domain = active_domain_with_detail
        repo = DomainRepository(async_db_session)

        assert domain.is_active is True
        assert domain.deactivated_at is None

        updated = await repo.update(domain.id, is_active=False, deactivated_at=datetime.now(UTC))
        assert updated is not None
        assert updated.is_active is False
        assert updated.deactivated_at is not None

    async def test_inactive_domain_skipped_by_refresh_scheduler(
        self, async_db_session: AsyncSession, active_domain_with_detail: Domain
    ) -> None:
        """Inactive domains should not be selected by refresh scheduler."""
        domain = active_domain_with_detail
        
        # Deactivate the domain
        domain_repo = DomainRepository(async_db_session)
        await domain_repo.update(domain.id, is_active=False, deactivated_at=datetime.now(UTC))
        await async_db_session.commit()

        # Refresh scheduler should not find it
        detail_repo = DomainDetailRepository(async_db_session)
        domains_needing_refresh = await detail_repo.get_domains_needing_refresh(limit=10)
        
        domain_ids = [d[0] for d in domains_needing_refresh]
        assert domain.id not in domain_ids

    async def test_reactivation_clears_domain_detail(
        self, async_db_session: AsyncSession, active_domain_with_detail: Domain
    ) -> None:
        """Reactivating a domain should clear its DomainDetail."""
        domain = active_domain_with_detail
        
        # Deactivate first
        domain_repo = DomainRepository(async_db_session)
        await domain_repo.update(domain.id, is_active=False, deactivated_at=datetime.now(UTC))
        await async_db_session.commit()

        # Verify DomainDetail exists before reactivation
        detail_repo = DomainDetailRepository(async_db_session)
        detail_before = await detail_repo.get(domain.id)
        assert detail_before is not None

        # Reactivate via get_or_create_with_reactivation
        reactivated_domain, was_reactivated = await domain_repo.get_or_create_with_reactivation(domain.normalized_domain)
        assert was_reactivated is True
        assert reactivated_domain.is_active is True

        # Clear DomainDetail (as done in API)
        await domain_repo.clear_domain_detail(domain.id)
        await async_db_session.commit()

        # DomainDetail should be gone
        detail_after = await detail_repo.get(domain.id)
        assert detail_after is None

    async def test_reactivated_domain_creates_new_tasks(
        self, async_client: httpx.AsyncClient, async_db_session: AsyncSession, active_domain_with_detail: Domain
    ) -> None:
        """Submitting a previously deactivated domain should create new tasks with fresh validation."""
        domain = active_domain_with_detail
        
        # Deactivate the domain
        domain_repo = DomainRepository(async_db_session)
        await domain_repo.update(domain.id, is_active=False, deactivated_at=datetime.now(UTC))
        await async_db_session.commit()

        # Submit the same domain again (should reactivate)
        response = await async_client.post(
            "/jobs",
            json={"domains": [domain.normalized_domain]},
        )
        assert response.status_code == 202
        job_id = response.json()["jobId"]

        # Verify tasks were created for the reactivated domain
        task_repo = TaskRepository(async_db_session)
        tasks = await task_repo.get_by_job_id(uuid.UUID(job_id))
        assert len(tasks) == 1
        assert tasks[0].domain_id == domain.id
        assert tasks[0].status == TaskStatus.PENDING


class TestIdempotencyRepository:
    """Tests for IdempotencyRecordRepository."""

    async def test_upsert_new_record(
        self, async_db_session: AsyncSession
    ) -> None:
        """Upsert should create new record when key doesn't exist."""
        from domain_processing_service.repositories import IdempotencyRecordRepository, JobRepository
        from domain_processing_service.models import IdempotencyRecord, Job, TaskStatus

        # First create a job
        job_repo = JobRepository(async_db_session)
        now = datetime.now(UTC)
        job = Job(
            id=uuid.uuid4(),
            status=TaskStatus.PENDING,
            created_at=now,
            updated_at=now,
        )
        await job_repo.create(job)
        await async_db_session.commit()

        repo = IdempotencyRecordRepository(async_db_session)
        record = IdempotencyRecord(
            id=uuid.uuid4(),
            client_id="client-1",
            idempotency_key="test-key",
            request_hash="abc123",
            job_id=job.id,
            created_at=now,
        )

        created, is_new = await repo.upsert(record)
        assert is_new is True
        assert created.id == record.id

    async def test_upsert_existing_record(
        self, async_db_session: AsyncSession
    ) -> None:
        """Upsert should return existing record when key exists."""
        from domain_processing_service.repositories import IdempotencyRecordRepository, JobRepository
        from domain_processing_service.models import IdempotencyRecord, Job, TaskStatus

        # First create a job
        job_repo = JobRepository(async_db_session)
        now = datetime.now(UTC)
        job = Job(
            id=uuid.uuid4(),
            status=TaskStatus.PENDING,
            created_at=now,
            updated_at=now,
        )
        await job_repo.create(job)
        await async_db_session.commit()

        repo = IdempotencyRecordRepository(async_db_session)
        record = IdempotencyRecord(
            id=uuid.uuid4(),
            client_id="client-1",
            idempotency_key="test-key",
            request_hash="abc123",
            job_id=job.id,
            created_at=now,
        )

        # First upsert
        created, is_new = await repo.upsert(record)
        assert is_new is True

        # Second upsert with same key
        record2 = IdempotencyRecord(
            id=uuid.uuid4(),
            client_id="client-1",
            idempotency_key="test-key",
            request_hash="abc123",
            job_id=uuid.uuid4(),  # Different job_id
            created_at=now,
        )

        existing, is_new = await repo.upsert(record2)
        assert is_new is False
        assert existing.id == record.id
        assert existing.job_id == job.id  # Original job_id preserved


class TestIntegration:
    """Integration tests for Phase 12 features working together."""

    async def test_idempotency_with_deactivated_domain_reactivation(
        self, async_client: httpx.AsyncClient, async_db_session: AsyncSession
    ) -> None:
        """Idempotency should work correctly with domain reactivation."""
        # First, create a domain and deactivate it
        domain_repo = DomainRepository(async_db_session)
        now = datetime.now(UTC)
        domain = Domain(
            id=uuid.uuid4(),
            normalized_domain="integration-test.example.com",
            is_active=False,
            deactivated_at=now - timedelta(days=1),
            created_at=now - timedelta(days=2),
            updated_at=now - timedelta(days=1),
        )
        async_db_session.add(domain)
        await async_db_session.commit()

        # Submit with idempotency key
        payload = {"domains": [domain.normalized_domain]}
        
        response1 = await async_client.post(
            "/jobs",
            json=payload,
            headers={"Idempotency-Key": "int-key-1", "X-Client-ID": "client-1"},
        )
        assert response1.status_code == 202
        job_id_1 = response1.json()["jobId"]

        # Second request with same idempotency key should return same job
        response2 = await async_client.post(
            "/jobs",
            json=payload,
            headers={"Idempotency-Key": "int-key-1", "X-Client-ID": "client-1"},
        )
        assert response2.status_code == 200
        assert response2.json()["jobId"] == job_id_1

        # Verify domain was reactivated
        # Use a fresh session to avoid stale cache from expire_on_commit=False
        from sqlalchemy.ext.asyncio import async_sessionmaker
        fresh_session_maker = async_sessionmaker(
            bind=async_db_session.bind, class_=AsyncSession, expire_on_commit=False
        )
        async with fresh_session_maker() as fresh_session:
            fresh_domain_repo = DomainRepository(fresh_session)
            updated_domain = await fresh_domain_repo.get_by_normalized_domain(domain.normalized_domain)
            assert updated_domain is not None
            assert updated_domain.is_active is True

    async def test_occ_conflict_does_not_corrupt_domain_detail(
        self, async_db_session: AsyncSession
    ) -> None:
        """OCC conflict should not leave DomainDetail in inconsistent state."""
        from domain_processing_service.repositories import DomainDetailRepository

        # Create domain with detail
        now = datetime.now(UTC)
        domain = Domain(
            id=uuid.uuid4(),
            normalized_domain="occ-integration.example.com",
            is_active=True,
            created_at=now,
            updated_at=now,
        )
        async_db_session.add(domain)
        await async_db_session.flush()

        detail = DomainDetail(
            domain_id=domain.id,
            ip_addresses=["1.2.3.4"],
            dns_records={"A": ["1.2.3.4"]},
            http_status=200,
            page_title="Original",
            response_time=100,
            response_headers={},
            fetched_at=now,
            next_refresh_at=now + timedelta(days=1),
            version=1,
        )
        async_db_session.add(detail)
        await async_db_session.commit()

        repo = DomainDetailRepository(async_db_session)

        # Attempt two concurrent updates with same stale version
        update1 = DomainDetail(
            domain_id=domain.id,
            ip_addresses=["1.2.3.4"],
            dns_records={"A": ["1.2.3.4"]},
            http_status=200,
            page_title="Update 1",
            response_time=100,
            response_headers={},
            fetched_at=datetime.now(UTC),
            next_refresh_at=datetime.now(UTC) + timedelta(days=1),
            version=2,
        )

        update2 = DomainDetail(
            domain_id=domain.id,
            ip_addresses=["1.2.3.4"],
            dns_records={"A": ["1.2.3.4"]},
            http_status=200,
            page_title="Update 2",
            response_time=100,
            response_headers={},
            fetched_at=datetime.now(UTC),
            next_refresh_at=datetime.now(UTC) + timedelta(days=1),
            version=2,
        )

        # First update succeeds
        await repo.upsert_with_occ(update1, expected_version=1)

        # Second update fails
        with pytest.raises(RuntimeError, match="OCC conflict"):
            await repo.upsert_with_occ(update2, expected_version=1)

        # Verify DomainDetail has the first update's data (not corrupted)
        final = await repo.get(domain.id)
        assert final.page_title == "Update 1"
        assert final.version == 2