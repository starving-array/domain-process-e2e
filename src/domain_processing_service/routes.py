"""API routes for the domain processing service."""

import hashlib
import json
import logging
import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from domain_processing_service.config import AppSettings
from domain_processing_service.database import SqlAlchemyDatabase
from domain_processing_service.dtos import (
    CreateJobRequest,
    CreateJobResponse,
    Cursor,
    ErrorDTO,
    JobResponse,
    JobSummaryDTO,
    TaskResultDTO,
)
from domain_processing_service.logging import log_event
from domain_processing_service.models import (
    Domain,
    DomainDetail,
    IdempotencyRecord,
    Job,
    Task,
    TaskStatus,
    TaskType,
)
from domain_processing_service.normalization import (
    NormalizedDomain,
    deduplicate_domains,
    normalize_domain,
)
from domain_processing_service.repositories import (
    DomainRepository,
    IdempotencyRecordRepository,
    JobRepository,
    TaskRepository,
)

router = APIRouter()

logger = logging.getLogger(__name__)

IDEMPOTENCY_KEY_HEADER = "Idempotency-Key"
CLIENT_ID_HEADER = "X-Client-ID"


def get_session(request: Request) -> AsyncSession:
    """Get the database session from the app state."""
    database: SqlAlchemyDatabase = request.app.state.database
    return database.session_maker()


async def get_async_session(request: Request) -> AsyncGenerator[AsyncSession, None]:
    """Get an async database session."""
    database: SqlAlchemyDatabase = request.app.state.database
    async_session_maker = database.session_maker
    async with async_session_maker() as session:
        yield session


async def get_settings(request: Request) -> AppSettings:
    """Get application settings."""
    return request.app.state.settings  # type: ignore[no-any-return]


def _compute_request_hash(payload: CreateJobRequest) -> str:
    """Compute SHA256 hash of normalized request payload for idempotency."""
    normalized = [normalize_domain(d).value for d in payload.domains]
    normalized.sort()
    canonical = json.dumps({"domains": normalized}, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(canonical.encode()).hexdigest()


@router.post(
    "/jobs",
    response_model=CreateJobResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit domains for asynchronous processing",
    description=(
        "Accepts a list of domains, normalizes them, and creates a Job "
        "with Tasks for asynchronous processing. Supports Idempotency-Key "
        "header for safe retries."
    ),
)
async def create_job(
    request: Request,
    payload: CreateJobRequest,
    settings: Annotated[AppSettings, Depends(get_settings)],
    session: Annotated[AsyncSession, Depends(get_async_session)],
    idempotency_key: Annotated[str | None, Header(alias=IDEMPOTENCY_KEY_HEADER)] = None,
    client_id: Annotated[str | None, Header(alias=CLIENT_ID_HEADER)] = None,
) -> CreateJobResponse | JSONResponse:
    """Create a new Job with Tasks for the given domains."""
    request_id = request.state.request_id

    # Check if shutdown is in progress
    shutdown_coordinator = getattr(request.app.state, "shutdown_coordinator", None)
    if shutdown_coordinator is not None and shutdown_coordinator.is_shutting_down:
        log_event(
            logger,
            "request.failed",
            level=logging.WARNING,
            request_id=request_id,
            reason="shutdown_in_progress",
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service is shutting down",
        )

    # Determine client_id for idempotency scoping
    # Use provided client_id header, fall back to request_id for anonymous clients
    effective_client_id = client_id or request_id

    # Validate request
    if not payload.domains:
        log_event(
            logger,
            "request.failed",
            level=logging.WARNING,
            request_id=request_id,
            reason="empty_domains_list",
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="domains list cannot be empty",
        )

    if len(payload.domains) > settings.max_domains_per_request:
        log_event(
            logger,
            "request.failed",
            level=logging.WARNING,
            request_id=request_id,
            reason="domain_count_exceeded",
            provided=len(payload.domains),
            maximum=settings.max_domains_per_request,
        )
        raise HTTPException(
            status_code=status.HTTP_413_CONTENT_TOO_LARGE,
            detail=f"maximum {settings.max_domains_per_request} domains per request",
        )

    log_event(
        logger,
        "request.validated",
        request_id=request_id,
        domain_count=len(payload.domains),
        max_domains=settings.max_domains_per_request,
    )

    # Normalize and validate domains
    log_event(
        logger,
        "domain.normalization.started",
        request_id=request_id,
        input_count=len(payload.domains),
    )

    normalized_results: list[NormalizedDomain] = [
        normalize_domain(domain) for domain in payload.domains
    ]

    valid_domains: list[NormalizedDomain] = []
    invalid_domains: list[NormalizedDomain] = []

    for result in normalized_results:
        if result.is_valid:
            valid_domains.append(result)
            log_event(
                logger,
                "domain.normalized",
                request_id=request_id,
                original=result.value,
                normalized=result.value,
            )
        else:
            invalid_domains.append(result)
            log_event(
                logger,
                "domain.validation.failed",
                request_id=request_id,
                original=result.value,
                normalized=result.value,
                error=result.error,
            )

    # Deduplicate valid domains
    if valid_domains:
        valid_raw_domains = [d.value for d in valid_domains]
        deduplicated_values = deduplicate_domains(valid_raw_domains)
        valid_domain_map = {d.value: d for d in valid_domains}
        final_valid_domains = [
            valid_domain_map[v] for v in deduplicated_values if v in valid_domain_map
        ]
    else:
        final_valid_domains = []

    log_event(
        logger,
        "domain.deduplicated",
        request_id=request_id,
        before=len(valid_domains),
        after=len(final_valid_domains),
    )

    now = datetime.now(UTC)

    # Create repositories first
    job_repo = JobRepository(session)
    task_repo = TaskRepository(session)
    domain_repo = DomainRepository(session)
    idempotency_repo = IdempotencyRecordRepository(session)

    job_id_to_use = None
    is_idempotent_replay = False

    try:
        if idempotency_key:
            request_hash = _compute_request_hash(payload)
            
            try:
                async with session.begin_nested():
                    savepoint_job_id = uuid.uuid4()
                    job = Job(
                        id=savepoint_job_id,
                        status=TaskStatus.PENDING,
                        created_at=now,
                        updated_at=now,
                    )
                    await job_repo.create(job)
                    
                    record = IdempotencyRecord(
                        id=uuid.uuid4(),
                        client_id=effective_client_id,
                        idempotency_key=idempotency_key,
                        request_hash=request_hash,
                        job_id=savepoint_job_id,
                        created_at=now,
                    )
                    await idempotency_repo.create(record)
                    job_id_to_use = savepoint_job_id
            except Exception:
                existing_record = await idempotency_repo.get(
                    effective_client_id, idempotency_key
                )
                if existing_record is not None:
                    if existing_record.request_hash == request_hash:
                        is_idempotent_replay = True
                        job_id_to_use = existing_record.job_id
                    if existing_record.request_hash != request_hash:
                        raise HTTPException(
                            status_code=status.HTTP_409_CONFLICT,
                            detail="Idempotency key already used with different payload (conflict)",
                        )
                else:
                    raise
        else:
            job_id_to_use = uuid.uuid4()
            job = Job(id=job_id_to_use, status=TaskStatus.PENDING, created_at=now, updated_at=now)
            await job_repo.create(job)
            
        if not is_idempotent_replay:
            # Create tasks only if it's a new job
            for norm_domain in final_valid_domains:
                domain, was_reactivated = await domain_repo.get_or_create_with_reactivation(
                    norm_domain.value
                )
                
                if was_reactivated:
                    log_event(
                        logger,
                        "domain.reactivated",
                        level=logging.INFO,
                        request_id=request_id,
                        domain_id=str(domain.id),
                        domain=domain.normalized_domain,
                    )
                    await domain_repo.clear_domain_detail(domain.id)
                
                task = Task(
                    id=uuid.uuid4(),
                    job_id=job_id_to_use,
                    domain_id=domain.id,
                    type=TaskType.USER_REQUEST,
                    status=TaskStatus.PENDING,
                    attempts=0,
                    next_attempt_at=now,
                    created_at=now,
                    updated_at=now,
                )
                await task_repo.create(task)

            for norm_domain in invalid_domains:
                domain_value = norm_domain.value if norm_domain.value else "invalid"
                domain = await domain_repo.get_or_create(domain_value)

                error_payload = {
                    "code": "VALIDATION_ERROR",
                    "message": norm_domain.error or "Domain validation failed",
                    "retryable": False,
                }

                task = Task(
                    id=uuid.uuid4(),
                    job_id=job_id_to_use,
                    domain_id=domain.id,
                    type=TaskType.USER_REQUEST,
                    status=TaskStatus.FAILED,
                    attempts=0,
                    next_attempt_at=now,
                    error_payload=error_payload,
                    created_at=now,
                    updated_at=now,
                )
                await task_repo.create(task)

        await session.commit()

    except HTTPException:
        # Don't rollback for HTTP exceptions like 409, we let FastAPI handle it
        # Actually session.commit() wasn't reached so the transaction will be rolled back by the session context manager
        # Wait, the HTTP exceptions raised inside try block will be caught here. We should rollback.
        await session.rollback()
        raise
    except Exception as e:
        await session.rollback()
        log_event(
            logger,
            "request.failed",
            level=logging.ERROR,
            request_id=request_id,
            reason="transaction_rolled_back",
            error=str(e),
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="failed to create job",
        ) from e

    if is_idempotent_replay:
        log_event(
            logger,
            "request.completed",
            request_id=request_id,
            job_id=str(job_id_to_use),
            status="returned_existing",
        )
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"jobId": str(job_id_to_use), "status": TaskStatus.PENDING.value},
        )
        
    log_event(
        logger,
        "job.creation.committed",
        request_id=request_id,
        job_id=str(job_id_to_use),
        total_tasks=len(final_valid_domains) + len(invalid_domains),
        pending_tasks=len(final_valid_domains),
        failed_tasks=len(invalid_domains),
    )
    log_event(
        logger,
        "request.completed",
        request_id=request_id,
        job_id=str(job_id_to_use),
        status="created",
    )

    assert job_id_to_use is not None
    return CreateJobResponse(jobId=job_id_to_use, status=TaskStatus.PENDING.value)


@router.get(
    "/jobs/{job_id}",
    response_model=JobResponse,
    status_code=status.HTTP_200_OK,
    summary="Retrieve job status and paginated results",
    description=(
        "Returns the Job status, summary counts, and paginated Task results "
        "for the given Job ID."
    ),
)
async def get_job(
    request: Request,
    job_id: uuid.UUID,
    settings: Annotated[AppSettings, Depends(get_settings)],
    session: Annotated[AsyncSession, Depends(get_async_session)],
    limit: int | None = None,
    cursor: str | None = None,
) -> JobResponse:
    request_id = request.state.request_id

    log_event(
        logger,
        "request.received",
        request_id=request_id,
        job_id=str(job_id),
        limit=limit,
        has_cursor=cursor is not None,
    )

    job_repo = JobRepository(session)
    task_repo = TaskRepository(session)

    job = await job_repo.get(job_id)
    if job is None:
        log_event(
            logger,
            "request.failed",
            level=logging.WARNING,
            request_id=request_id,
            reason="job_not_found",
            job_id=str(job_id),
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="job not found",
        )

    page_limit = limit if limit is not None else settings.default_page_size
    if page_limit <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="limit must be positive",
        )
    if page_limit > settings.max_page_size:
        page_limit = settings.max_page_size

    cursor_created_at: datetime | None = None
    cursor_task_id: uuid.UUID | None = None
    if cursor is not None:
        try:
            decoded_cursor = Cursor.decode(cursor)
            cursor_created_at = decoded_cursor.created_at
            cursor_task_id = decoded_cursor.task_id
        except ValueError as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="invalid cursor format",
            ) from e

    tasks = await task_repo.get_by_job_id_paginated(
        job_id=job_id,
        limit=page_limit + 1,
        cursor_created_at=cursor_created_at,
        cursor_task_id=cursor_task_id,
    )

    summary_counts = await task_repo.get_summary_by_job_id(job_id)

    has_next_page = len(tasks) > page_limit
    if has_next_page:
        tasks = tasks[:page_limit]

    next_cursor: str | None = None
    if has_next_page and tasks:
        last_task = tasks[-1]
        next_cursor = Cursor(
            created_at=last_task.created_at,
            task_id=last_task.id,
        ).encode()

    task_domain_ids = [task.domain_id for task in tasks]
    domains_map: dict[uuid.UUID, Any] = {}
    domain_details_map: dict[uuid.UUID, Any] = {}

    if task_domain_ids:
        from sqlalchemy import select

        domain_stmt = select(Domain).where(Domain.id.in_(task_domain_ids))
        domain_result = await session.execute(domain_stmt)
        domains_map = {d.id: d for d in domain_result.scalars().all()}

        domain_detail_stmt = select(DomainDetail).where(
            DomainDetail.domain_id.in_(task_domain_ids)
        )
        domain_detail_result = await session.execute(domain_detail_stmt)
        domain_details_map = {d.domain_id: d for d in domain_detail_result.scalars().all()}

    task_results: list[TaskResultDTO] = []
    for task in tasks:
        domain = domains_map.get(task.domain_id)
        domain_detail = domain_details_map.get(task.domain_id)

        domain_name = domain.normalized_domain if domain else "unknown"

        result_data: dict[str, Any] | None = None
        error_data: ErrorDTO | None = None

        if task.status == TaskStatus.COMPLETED and domain_detail:
            result_data = {
                "ip_addresses": domain_detail.ip_addresses,
                "dns_records": domain_detail.dns_records,
                "http_status": domain_detail.http_status,
                "page_title": domain_detail.page_title,
                "response_time": domain_detail.response_time,
            }
        elif task.status == TaskStatus.FAILED and task.error_payload:
            error_data = ErrorDTO(
                code=task.error_payload.get("code", "INTERNAL_ERROR"),
                message=task.error_payload.get("message", "Task failed"),
                retryable=task.error_payload.get("retryable", False),
            )

        task_results.append(
            TaskResultDTO(
                taskId=task.id,
                domain=domain_name,
                status=task.status.value,
                result=result_data,
                error=error_data,
            )
        )

    summary = JobSummaryDTO(
        total=sum(summary_counts.values()),
        completed=summary_counts.get(TaskStatus.COMPLETED, 0),
        failed=summary_counts.get(TaskStatus.FAILED, 0),
        pending=summary_counts.get(TaskStatus.PENDING, 0),
        processing=summary_counts.get(TaskStatus.PROCESSING, 0),
    )

    log_event(
        logger,
        "request.completed",
        request_id=request_id,
        job_id=str(job_id),
        status="retrieved",
        results_count=len(task_results),
        has_next_page=has_next_page,
    )

    return JobResponse(
        jobId=job.id,
        status=job.status.value,
        summary=summary,
        results=task_results,
        nextCursor=next_cursor,
    )