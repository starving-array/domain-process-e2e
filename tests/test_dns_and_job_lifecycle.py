"""Tests for DNS configuration/classification and Job lifecycle status transitions."""

import asyncio
import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import aiodns
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from domain_processing_service.config import AppSettings
from domain_processing_service.dns import DnsResolver, classify_dns_error
from domain_processing_service.domain_processor import DomainProcessor
from domain_processing_service.models import Domain, Job, Task, TaskStatus, TaskType


# ============================================================================
# DNS Tests
# ============================================================================

def test_dns_nameservers_config_parsing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify AppSettings properly parses DOMAIN_PROCESSING_DNS_NAMESERVERS."""
    monkeypatch.setenv(
        "DOMAIN_PROCESSING_DNS_NAMESERVERS",
        "8.8.8.8, 1.1.1.1, 8.8.4.4",
    )
    settings = AppSettings()
    assert settings.dns_nameservers == ["8.8.8.8", "1.1.1.1", "8.8.4.4"]


def test_dns_error_classification() -> None:
    """Verify classification of DNS error types."""
    # Timeout
    assert classify_dns_error(TimeoutError("DNS timed out")) == "retryable"
    assert (
        classify_dns_error(aiodns.error.DNSError(aiodns.error.ARES_ETIMEOUT, "Timeout"))
        == "retryable"
    )

    # Server failure / Connection refused
    assert (
        classify_dns_error(
            aiodns.error.DNSError(aiodns.error.ARES_ESERVFAIL, "Server failure")
        )
        == "retryable"
    )
    assert (
        classify_dns_error(
            aiodns.error.DNSError(
                aiodns.error.ARES_ECONNREFUSED, "Could not contact DNS servers"
            )
        )
        == "retryable"
    )

    # Permanent failures
    assert (
        classify_dns_error(
            aiodns.error.DNSError(aiodns.error.ARES_ENOTFOUND, "Domain not found")
        )
        == "permanent"
    )
    assert (
        classify_dns_error(
            aiodns.error.DNSError(aiodns.error.ARES_ENODATA, "No data")
        )
        == "permanent"
    )

    # Unknown
    assert classify_dns_error(ValueError("Custom error")) == "unknown"


@pytest.mark.asyncio
async def test_dns_resolver_uses_configured_nameservers() -> None:
    """Verify DnsResolver correctly configures aiodns with settings nameservers."""
    settings = AppSettings(dns_nameservers=["8.8.8.8", "1.1.1.1"])
    resolver = DnsResolver(settings)

    assert resolver._resolver.nameservers == ["8.8.8.8", "1.1.1.1"]


@pytest.mark.asyncio
async def test_dns_resolver_resolution_success() -> None:
    """Verify DnsResolver resolves addresses when mocked."""
    settings = AppSettings(dns_nameservers=["8.8.8.8"])
    resolver = DnsResolver(settings)

    with patch.object(resolver._resolver, "query", new_callable=AsyncMock) as mock_query:
        class MockRecord:
            def __init__(self, host):
                self.host = host

        mock_query.side_effect = lambda domain, qtype: (
            [MockRecord("93.184.216.34")] if qtype == "A"
            else [MockRecord("2606:2800:220:1:248:1893:25c8:1946")] if qtype == "AAAA"
            else []
        )

        result = await resolver.resolve("example.com")
        assert result.is_success
        assert "93.184.216.34" in result.ips_v4
        assert "2606:2800:220:1:248:1893:25c8:1946" in result.ips_v6


# ============================================================================
# Job Lifecycle Status Tests
# ============================================================================

async def _create_test_job_with_tasks(
    session: AsyncSession, task_count: int
) -> tuple[Job, list[Task]]:
    """Helper to create a Job with N pending tasks."""
    now = datetime.now(UTC)
    job = Job(
        id=uuid.uuid4(),
        status=TaskStatus.PENDING,
        created_at=now,
        updated_at=now,
    )
    session.add(job)
    await session.flush()

    tasks: list[Task] = []
    for i in range(task_count):
        domain = Domain(
            id=uuid.uuid4(),
            normalized_domain=f"domain-{i}-{uuid.uuid4().hex[:6]}.com",
            is_active=True,
            created_at=now,
            updated_at=now,
        )
        session.add(domain)
        await session.flush()

        task = Task(
            id=uuid.uuid4(),
            job_id=job.id,
            domain_id=domain.id,
            type=TaskType.USER_REQUEST,
            status=TaskStatus.PENDING,
            attempts=0,
            next_attempt_at=now,
            created_at=now,
            updated_at=now,
        )
        session.add(task)
        tasks.append(task)

    await session.commit()
    return job, tasks


@pytest.mark.asyncio
async def test_job_becomes_completed_when_all_tasks_complete(
    async_engine, test_settings, async_client
) -> None:
    """A. When all tasks complete, Job becomes COMPLETED and GET /jobs/{id} returns COMPLETED."""
    session_maker = async_sessionmaker(
        bind=async_engine, expire_on_commit=False, class_=AsyncSession
    )
    async with session_maker() as session:
        job, tasks = await _create_test_job_with_tasks(session, 2)

    processor = DomainProcessor(
        session_maker=session_maker,
        settings=test_settings,
    )

    # Complete task 1 -> Job should transition to PROCESSING
    await processor._write_complete_task(str(tasks[0].id), TaskStatus.COMPLETED)

    async with session_maker() as session:
        j = (await session.execute(select(Job).where(Job.id == job.id))).scalar_one()
        assert j.status == TaskStatus.PROCESSING

    resp1 = await async_client.get(f"/jobs/{job.id}")
    assert resp1.status_code == 200
    assert resp1.json()["status"] == "PROCESSING"
    assert resp1.json()["summary"]["completed"] == 1
    assert resp1.json()["summary"]["pending"] == 1

    # Complete task 2 -> All tasks are terminal, Job should become COMPLETED
    await processor._write_complete_task(str(tasks[1].id), TaskStatus.COMPLETED)

    async with session_maker() as session:
        j = (await session.execute(select(Job).where(Job.id == job.id))).scalar_one()
        assert j.status == TaskStatus.COMPLETED

    resp2 = await async_client.get(f"/jobs/{job.id}")
    assert resp2.status_code == 200
    assert resp2.json()["status"] == "COMPLETED"
    assert resp2.json()["summary"]["completed"] == 2
    assert resp2.json()["summary"]["pending"] == 0


@pytest.mark.asyncio
async def test_job_becomes_completed_when_all_tasks_fail(
    async_engine, test_settings, async_client
) -> None:
    """B. When all tasks fail, Job becomes COMPLETED and GET /jobs/{id} returns COMPLETED."""
    session_maker = async_sessionmaker(
        bind=async_engine, expire_on_commit=False, class_=AsyncSession
    )
    async with session_maker() as session:
        job, tasks = await _create_test_job_with_tasks(session, 2)

    processor = DomainProcessor(
        session_maker=session_maker,
        settings=test_settings,
    )

    # Fail task 1
    await processor._write_fail_task(str(tasks[0].id), "Permanent error", "permanent")

    async with session_maker() as session:
        j = (await session.execute(select(Job).where(Job.id == job.id))).scalar_one()
        assert j.status == TaskStatus.PROCESSING

    # Fail task 2
    await processor._write_fail_task(str(tasks[1].id), "Permanent error", "permanent")

    async with session_maker() as session:
        j = (await session.execute(select(Job).where(Job.id == job.id))).scalar_one()
        assert j.status == TaskStatus.COMPLETED

    resp = await async_client.get(f"/jobs/{job.id}")
    assert resp.status_code == 200
    assert resp.json()["status"] == "COMPLETED"
    assert resp.json()["summary"]["failed"] == 2
    assert resp.json()["summary"]["completed"] == 0


@pytest.mark.asyncio
async def test_job_becomes_completed_with_mixed_outcomes(
    async_engine, test_settings, async_client
) -> None:
    """C. Mixed completed + failed: Job becomes COMPLETED once every task is terminal."""
    session_maker = async_sessionmaker(
        bind=async_engine, expire_on_commit=False, class_=AsyncSession
    )
    async with session_maker() as session:
        job, tasks = await _create_test_job_with_tasks(session, 3)

    processor = DomainProcessor(
        session_maker=session_maker,
        settings=test_settings,
    )

    # Complete task 1
    await processor._write_complete_task(str(tasks[0].id), TaskStatus.COMPLETED)
    # Fail task 2
    await processor._write_fail_task(str(tasks[1].id), "Error", "permanent")

    # Task 3 is still PENDING -> Job must not be COMPLETED
    async with session_maker() as session:
        j = (await session.execute(select(Job).where(Job.id == job.id))).scalar_one()
        assert j.status == TaskStatus.PROCESSING

    resp1 = await async_client.get(f"/jobs/{job.id}")
    assert resp1.status_code == 200
    assert resp1.json()["status"] == "PROCESSING"

    # Complete task 3
    await processor._write_complete_task(str(tasks[2].id), TaskStatus.COMPLETED)

    # All 3 tasks now terminal -> Job must be COMPLETED
    async with session_maker() as session:
        j = (await session.execute(select(Job).where(Job.id == job.id))).scalar_one()
        assert j.status == TaskStatus.COMPLETED

    resp2 = await async_client.get(f"/jobs/{job.id}")
    assert resp2.status_code == 200
    assert resp2.json()["status"] == "COMPLETED"
    assert resp2.json()["summary"]["completed"] == 2
    assert resp2.json()["summary"]["failed"] == 1


@pytest.mark.asyncio
async def test_job_remains_processing_when_tasks_pending_or_processing(
    async_engine, test_settings, async_client
) -> None:
    """D & E. Some tasks still pending or processing: Job must not become COMPLETED."""
    session_maker = async_sessionmaker(
        bind=async_engine, expire_on_commit=False, class_=AsyncSession
    )
    async with session_maker() as session:
        job, tasks = await _create_test_job_with_tasks(session, 2)
        # Set task 2 to PROCESSING
        tasks[1].status = TaskStatus.PROCESSING
        await session.commit()

    processor = DomainProcessor(
        session_maker=session_maker,
        settings=test_settings,
    )

    # Complete task 1
    await processor._write_complete_task(str(tasks[0].id), TaskStatus.COMPLETED)

    # Task 2 is still PROCESSING
    async with session_maker() as session:
        j = (await session.execute(select(Job).where(Job.id == job.id))).scalar_one()
        assert j.status == TaskStatus.PROCESSING

    resp = await async_client.get(f"/jobs/{job.id}")
    assert resp.status_code == 200
    assert resp.json()["status"] == "PROCESSING"
    assert resp.json()["summary"]["completed"] == 1
    assert resp.json()["summary"]["processing"] == 1


@pytest.mark.asyncio
async def test_concurrent_final_task_completion(
    async_engine, test_settings, async_client
) -> None:
    """F. Concurrent final-task completion: Verify race-safe final Job status."""
    session_maker = async_sessionmaker(
        bind=async_engine, expire_on_commit=False, class_=AsyncSession
    )
    async with session_maker() as session:
        job, tasks = await _create_test_job_with_tasks(session, 4)

    processor = DomainProcessor(
        session_maker=session_maker,
        settings=test_settings,
    )

    # Run all 4 task terminal updates concurrently
    await asyncio.gather(
        processor._write_complete_task(str(tasks[0].id), TaskStatus.COMPLETED),
        processor._write_fail_task(str(tasks[1].id), "Error 1", "permanent"),
        processor._write_complete_task(str(tasks[2].id), TaskStatus.COMPLETED),
        processor._write_fail_task(str(tasks[3].id), "Error 2", "permanent"),
    )

    # Verify final persisted Job status in database
    async with session_maker() as session:
        j = (await session.execute(select(Job).where(Job.id == job.id))).scalar_one()
        assert j.status == TaskStatus.COMPLETED

    # Verify GET API response
    resp = await async_client.get(f"/jobs/{job.id}")
    assert resp.status_code == 200
    assert resp.json()["status"] == "COMPLETED"
    assert resp.json()["summary"]["total"] == 4
    assert resp.json()["summary"]["completed"] == 2
    assert resp.json()["summary"]["failed"] == 2
    assert resp.json()["summary"]["pending"] == 0
    assert resp.json()["summary"]["processing"] == 0


@pytest.mark.asyncio
async def test_get_job_is_strictly_read_only(
    async_client, async_db_session: AsyncSession
) -> None:
    """G. GET /jobs/{id} is strictly read-only and does not mutate Job state."""
    job, tasks = await _create_test_job_with_tasks(async_db_session, 2)
    job_updated_at_before = job.updated_at

    response = await async_client.get(f"/jobs/{job.id}")
    assert response.status_code == 200
    data = response.json()

    assert data["jobId"] == str(job.id)
    assert data["status"] == "PENDING"

    # Verify Job row in DB was not mutated
    await async_db_session.refresh(job)
    assert job.status == TaskStatus.PENDING
    assert job.updated_at == job_updated_at_before


# ============================================================================
# HTTP Probe Success / Redirect Observation & Worker Session Tests
# ============================================================================

@pytest.mark.parametrize("status_code", [200, 301, 302, 307, 308])
@pytest.mark.asyncio
async def test_http_success_and_redirects_persist_domain_detail_and_complete_task(
    async_engine, test_settings, status_code: int
) -> None:
    """Requirement 5A, 5B, 5C, 5D: HTTP 2xx and 3xx responses follow the normal success path."""
    from domain_processing_service.http_client import HttpResult
    from domain_processing_service.models import DomainDetail
    from domain_processing_service.dns import DnsResult

    session_maker = async_sessionmaker(
        bind=async_engine, expire_on_commit=False, class_=AsyncSession
    )
    async with session_maker() as session:
        job, tasks = await _create_test_job_with_tasks(session, 1)
        task = tasks[0]

    processor = DomainProcessor(
        session_maker=session_maker,
        settings=test_settings,
    )

    # Mock DNS and HTTP
    mock_dns = DnsResult(
        ips_v4=["93.184.216.34"],
        ips_v6=[],
        cname=None,
    )
    mock_http = HttpResult(
        status_code=status_code,
        headers={"location": "https://example.com/"} if status_code >= 300 else {},
        body=b"<html><head><title>Example</title></head></html>",
        response_time_ms=120,
        page_title="Example" if status_code == 200 else None,
        error=None,
    )

    with patch.object(processor, "_resolve_dns", new_callable=AsyncMock, return_value=mock_dns), \
         patch.object(processor._http_client, "probe", new_callable=AsyncMock, return_value=mock_http):
        
        result = await processor.process_task(task)
        assert result.status == TaskStatus.COMPLETED

    # Verify task is COMPLETED and DomainDetail is persisted in DB
    async with session_maker() as session:
        t = (await session.execute(select(Task).where(Task.id == task.id))).scalar_one()
        assert t.status == TaskStatus.COMPLETED

        dd = (await session.execute(select(DomainDetail).where(DomainDetail.domain_id == task.domain_id))).scalar_one_or_none()
        assert dd is not None
        assert dd.http_status == status_code
        assert "93.184.216.34" in dd.ip_addresses

        j = (await session.execute(select(Job).where(Job.id == job.id))).scalar_one()
        assert j.status == TaskStatus.COMPLETED


@pytest.mark.parametrize("status_code,expected_category", [
    (404, "permanent"),
    (403, "permanent"),
    (500, "retryable"),
    (503, "retryable"),
])
@pytest.mark.asyncio
async def test_http_client_and_server_error_matrix(
    async_engine, test_settings, status_code: int, expected_category: str
) -> None:
    """Requirement 5E: 4xx/5xx errors remain failures according to failure matrix."""
    from domain_processing_service.http_client import HttpResult
    from domain_processing_service.dns import DnsResult

    session_maker = async_sessionmaker(
        bind=async_engine, expire_on_commit=False, class_=AsyncSession
    )
    async with session_maker() as session:
        job, tasks = await _create_test_job_with_tasks(session, 1)
        task = tasks[0]

    processor = DomainProcessor(
        session_maker=session_maker,
        settings=test_settings,
    )

    mock_dns = DnsResult(
        ips_v4=["93.184.216.34"],
        ips_v6=[],
        cname=None,
    )
    mock_http = HttpResult(
        status_code=status_code,
        headers={},
        body=b"",
        response_time_ms=50,
        page_title=None,
        error="Server error" if status_code >= 500 else None,
    )

    with patch.object(processor, "_resolve_dns", new_callable=AsyncMock, return_value=mock_dns), \
         patch.object(processor._http_client, "probe", new_callable=AsyncMock, return_value=mock_http):
        
        result = await processor.process_task(task)
        if expected_category == "permanent":
            assert result.status == TaskStatus.FAILED
            assert result.error_category == "permanent"
        else:
            assert result.status == TaskStatus.PENDING
            assert result.error_category == "retryable"


@pytest.mark.asyncio
async def test_worker_phase9_handler_and_legacy_handler_session_lifecycle(
    async_engine, test_settings
) -> None:
    """Requirement 5F & 5G: Phase 9 handler has no outer worker session; legacy handler gets session."""
    from domain_processing_service.worker.worker import Worker, WorkerConfig, create_phase9_task_handler

    session_maker = async_sessionmaker(
        bind=async_engine, expire_on_commit=False, class_=AsyncSession
    )
    async with session_maker() as session:
        job, tasks = await _create_test_job_with_tasks(session, 2)

    # 1. Phase 9 handler test
    received_sessions = []
    p9_handler = create_phase9_task_handler(test_settings)
    p9_handler._attach_session_maker(session_maker)

    original_p9_call = p9_handler
    async def tracking_p9_handler(task, session):
        received_sessions.append(session)
        # Mock quick success for the task
        async with session_maker() as s:
            t = (await s.execute(select(Task).where(Task.id == task.id))).scalar_one()
            t.status = TaskStatus.COMPLETED
            await s.commit()

    tracking_p9_handler._attach_session_maker = p9_handler._attach_session_maker

    worker_p9 = Worker(
        WorkerConfig(
            worker_id="w-p9",
            task_handler=tracking_p9_handler,
            session_maker=session_maker,
            settings=test_settings,
        )
    )
    await worker_p9._process_task(tasks[0])
    assert received_sessions == [None], "Phase 9 handler must receive session=None (self-managed)"

    # 2. Legacy/mock handler test
    legacy_received_sessions = []
    async def legacy_mock_handler(task: Task, session: AsyncSession) -> None:
        legacy_received_sessions.append(session)
        assert session is not None
        # Mock in-session update
        t = await session.get(Task, task.id)
        if t:
            t.status = TaskStatus.COMPLETED

    worker_legacy = Worker(
        WorkerConfig(
            worker_id="w-legacy",
            task_handler=legacy_mock_handler,
            session_maker=session_maker,
            settings=test_settings,
        )
    )
    await worker_legacy._process_task(tasks[1])
    assert len(legacy_received_sessions) == 1
    assert legacy_received_sessions[0] is not None, "Legacy handler must receive an active session"
