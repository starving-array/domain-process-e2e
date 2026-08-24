import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Optional
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from domain_processing_service.config import AppSettings
from domain_processing_service.dns import DnsResolver, DnsResult, classify_dns_error
from domain_processing_service.domain_lock import DomainLockManager
from domain_processing_service.http_client import HttpClient, HttpResult
from domain_processing_service.ip_validator import IpValidationResult, IpValidator
from domain_processing_service.logging import log_event
from domain_processing_service.models import DomainDetail, Task, TaskStatus
from domain_processing_service.repositories import (
    DomainDetailRepository,
    DomainRepository,
    TaskRepository,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProcessingResult:
    """Result of domain processing."""
    
    status: TaskStatus
    domain_detail: DomainDetail | None = None
    error: str | None = None
    error_category: str | None = None  # "retryable", "permanent", "security"
    dns_result: DnsResult | None = None
    http_result: "HttpResult | None" = None
    ip_validation: IpValidationResult | None = None


class DomainProcessor:
    """
    Main domain processing orchestrator.
    
    Implements the full domain processing pipeline:
    1. Check DomainDetail freshness
    5. Acquire Redis domain lock
    5. Double-check freshness
    5. DNS resolution
    5. IP validation (SSRF)
    5. HTTP/HTTPS probing
    5. DomainDetail persistence
    5. Task completion
    """
    
    def __init__(
        self,
        settings: AppSettings,
        session: AsyncSession,
        domain_lock_manager: Optional["DomainLockManager"] = None,
    ) -> None:
        self._settings = settings
        self._session = session
        
        # Repositories
        self._domain_repo = DomainRepository(session)
        self._domain_detail_repo = DomainDetailRepository(session)
        self._task_repo = TaskRepository(session)
        
        # Components
        self._dns_resolver = DnsResolver(settings)
        self._ip_validator = IpValidator()
        self._http_client = HttpClient(settings)
        self._domain_lock = domain_lock_manager
        
        # Configuration
        self._freshness_seconds = settings.domain_detail_freshness_seconds
        self._max_attempts = settings.max_attempts
    
    async def close(self) -> None:
        """Clean up resources."""
        await self._http_client.close()
    
    async def process_task(self, task: Task) -> ProcessingResult:
        """
        Process a single task through the full domain processing pipeline.
        
        This is the main entry point called by the worker.
        
        Args:
            task: The task to process
            
        Returns:
            ProcessingResult with status and any error information
        """
        task_id = str(task.id)
        domain_id = task.domain_id
        
        log_event(
            logger,
            "domain_processing.started",
            level=logging.INFO,
            task_id=task_id,
            domain_id=str(domain_id),
            task_type=task.type.value,
            attempt=task.attempts + 1,
        )
        
        start_time = datetime.now(UTC)
        
        try:
            # Step 1: Get domain
            domain = await self._domain_repo.get(domain_id)
            if domain is None:
                return ProcessingResult(
                    status=TaskStatus.FAILED,
                    error="Domain not found",
                    error_category="permanent",
                )
            
            normalized_domain = domain.normalized_domain
            
            log_event(
                logger,
                "domain_processing.domain_resolved",
                level=logging.INFO,
                task_id=task_id,
                domain=normalized_domain,
            )
            
            # Step 2: Check DomainDetail freshness
            is_fresh, domain_detail = await self._check_freshness(domain_id)
            
            if is_fresh:
                log_event(
                    logger,
                    "domain_processing.fresh_detail_reused",
                    level=logging.INFO,
                    task_id=task_id,
                    domain=normalized_domain,
                )
                
                # Mark task as completed with fresh data
                await self._complete_task(task, TaskStatus.COMPLETED)
                return ProcessingResult(
                    status=TaskStatus.COMPLETED,
                    domain_detail=domain_detail,
                )
            
            log_event(
                logger,
                "domain_processing.needs_processing",
                level=logging.INFO,
                task_id=task_id,
                domain=normalized_domain,
            )
            
            # Step 3: Acquire Redis domain lock
            # We need to initialize the lock manager if not provided
            from domain_processing_service.config import get_settings
            from domain_processing_service.domain_lock import DomainLockManager

            lock_settings = get_settings()
            lock_manager = DomainLockManager(lock_settings)
            await lock_manager.connect()
            
            try:
                # Step 4: Acquire Redis domain lock
                async with lock_manager._lock.acquire(normalized_domain) as lock_ctx:
                    if not lock_ctx.is_acquired:
                        log_event(
                            logger,
                            "domain_processing.lock_contention",
                            level=logging.INFO,
                            task_id=task_id,
                            domain=normalized_domain,
                        )
                        # Lock contention - reschedule task
                        await self._reschedule_task(task, "Lock contention")
                        return ProcessingResult(
                            status=TaskStatus.PENDING,
                            error="Lock contention",
                            error_category="retryable",
                        )
                    
                    # Step 5: Double-check freshness after lock acquisition
                    is_fresh, domain_detail = await self._check_freshness(domain_id)
                    
                    if is_fresh:
                        log_event(
                            logger,
                            "domain_processing.fresh_after_lock",
                            level=logging.INFO,
                            task_id=task_id,
                            domain=normalized_domain,
                        )
                        await self._complete_task(task, TaskStatus.COMPLETED)
                        return ProcessingResult(
                            status=TaskStatus.COMPLETED,
                            domain_detail=domain_detail,
                        )
                    
                    log_event(
                        logger,
                        "domain_processing.lock_acquired",
                        level=logging.INFO,
                        task_id=task_id,
                        domain=normalized_domain,
                    )
                    
                    # Step 5: DNS Resolution
                    dns_result = await self._resolve_dns(normalized_domain, task_id)
                    
                    if not dns_result.is_success:
                        return await self._handle_dns_failure(
                            task, normalized_domain, dns_result, task_id
                        )
                    
                    log_event(
                        logger,
                        "domain_processing.dns_resolved",
                        level=logging.INFO,
                        task_id=task_id,
                        domain=normalized_domain,
                        ips_v4=len(dns_result.ips_v4),
                        ips_v6=len(dns_result.ips_v6),
                    )
                    
                    # Step 6: IP Validation (SSRF protection)
                    ip_validation = await self._validate_ips(dns_result, task_id)
                    
                    if not ip_validation.is_allowed:
                        log_event(
                            logger,
                            "domain_processing.ssrf_rejected",
                            level=logging.WARNING,
                            task_id=task_id,
                            domain=normalized_domain,
                            rejected_ip=ip_validation.ip,
                            reason=ip_validation.reason,
                        )
                        
                        await self._fail_task(task, "SSRF rejection", "security")
                        await self._maybe_deactivate_domain(
                            task.domain_id, "security", "SECURITY_REJECTION"
                        )
                        return ProcessingResult(
                            status=TaskStatus.FAILED,
                            error="SSRF rejection",
                            error_category="security",
                            ip_validation=ip_validation,
                        )
                    
                    log_event(
                        logger,
                        "domain_processing.ip_validated",
                        level=logging.INFO,
                        task_id=task_id,
                        domain=normalized_domain,
                        validated_ip=ip_validation.ip,
                    )
                    
                    # Step 7: HTTP/HTTPS Probing
                    http_result = await self._http_client.probe(
                        domain=normalized_domain,
                        validated_ip=ip_validation.ip,
                        original_domain=normalized_domain,
                    )
                    
                    if not http_result.is_success:
                        return await self._handle_http_failure(
                            task, normalized_domain, http_result, task_id
                        )
                    
                    log_event(
                        logger,
                        "domain_processing.http_completed",
                        level=logging.INFO,
                        task_id=task_id,
                        domain=normalized_domain,
                        status_code=http_result.status_code,
                        response_time_ms=http_result.response_time_ms,
                    )
                    
                    # Step 8: Persist DomainDetail
                    domain_detail = await self._persist_domain_detail(
                        domain_id=domain_id,
                        dns_result=dns_result,
                        http_result=http_result,
                        validated_ip=ip_validation.ip,
                    )
                    
                    # Step 9: Complete task
                    await self._complete_task(task, TaskStatus.COMPLETED)
                    
                    duration_ms = int((datetime.now(UTC) - start_time).total_seconds() * 1000)
                    
                    log_event(
                        logger,
                        "domain_processing.completed",
                        level=logging.INFO,
                        task_id=task_id,
                        domain=normalized_domain,
                        duration_ms=duration_ms,
                    )
                    
                    return ProcessingResult(
                        status=TaskStatus.COMPLETED,
                        domain_detail=domain_detail,
                        dns_result=dns_result,
                        http_result=http_result,
                    )
                    
            finally:
                # Note: DomainLockManager doesn't need explicit close
                pass
                
        except Exception as e:
            logger.error("Error processing task %s: %s", task_id, e, exc_info=True)
            
            # Determine if error is retryable
            error_category = self._classify_exception(e)
            
            if error_category == "retryable":
                await self._reschedule_task(task, str(e))
                return ProcessingResult(
                    status=TaskStatus.PENDING,
                    error=str(e),
                    error_category="retryable",
                )
            else:
                await self._fail_task(task, str(e), "permanent")
                return ProcessingResult(
                    status=TaskStatus.FAILED,
                    error=str(e),
                    error_category="permanent",
                )
    
    async def _check_freshness(self, domain_id: UUID) -> tuple[bool, DomainDetail | None]:
        """Check if DomainDetail is fresh enough to reuse."""
        domain_detail = await self._domain_detail_repo.get(domain_id)
        
        if domain_detail is None:
            return False, None
        
        now = datetime.now(UTC)
        age_seconds = (now - domain_detail.fetched_at).total_seconds()
        
        if age_seconds <= self._freshness_seconds:
            return True, domain_detail
        
        return False, domain_detail
    
    async def _resolve_dns(self, domain: str, task_id: str) -> DnsResult:
        """Resolve DNS for the domain."""
        dns_result = await self._dns_resolver.resolve(domain)
        
        # Classify any DNS error
        if dns_result.error:
            classification = classify_dns_error(Exception(dns_result.error))
            # Attach classification to result for error handling
            dns_result.classification = classification  # type: ignore[attr-defined]
        
        return dns_result
    
    async def _handle_dns_failure(
        self,
        task: Task,
        domain: str,
        dns_result: DnsResult,
        task_id: str,
    ) -> ProcessingResult:
        """Handle DNS resolution failure."""
        classification = getattr(dns_result, 'classification', 'unknown')
        
        if classification == "permanent":
            # NXDOMAIN or no data - permanent failure
            log_event(
                logger,
                "domain_processing.dns_permanent_failure",
                level=logging.WARNING,
                task_id=task_id,
                domain=domain,
                error=dns_result.error,
            )
            await self._fail_task(task, f"DNS resolution failed: {dns_result.error}", "permanent")
            await self._maybe_deactivate_domain(
                task.domain_id, "permanent", "DNS_PERMANENT_FAILURE"
            )
            return ProcessingResult(
                status=TaskStatus.FAILED,
                error=dns_result.error,
                error_category="permanent",
                dns_result=dns_result,
            )
        else:
            # Transient failure - retry
            log_event(
                logger,
                "domain_processing.dns_transient_failure",
                level=logging.WARNING,
                task_id=task_id,
                domain=domain,
                error=dns_result.error,
            )
            await self._reschedule_task(task, f"DNS error: {dns_result.error}")
            return ProcessingResult(
                status=TaskStatus.PENDING,
                error=dns_result.error,
                error_category="retryable",
                dns_result=dns_result,
            )
    
    async def _validate_ips(self, dns_result: DnsResult, task_id: str) -> IpValidationResult:
        """Validate resolved IPs against SSRF blocklist."""
        all_ips = dns_result.all_ips
        
        if not all_ips:
            # No IPs resolved - this is a failure
            from domain_processing_service.ip_validator import IpValidationResult
            return IpValidationResult(
                is_allowed=False,
                ip="",
                reason="No IP addresses resolved",
            )
        
        # Check each IP, return first allowed
        for ip in all_ips:
            result = self._ip_validator.validate(ip)
            if result.is_allowed:
                return result
        
        # All IPs blocked
        return IpValidationResult(
            is_allowed=False,
            ip=all_ips[0] if all_ips else "",
            reason="All resolved IPs blocked by SSRF protection",
        )
    
    async def _handle_http_failure(
        self,
        task: Task,
        domain: str,
        http_result: "HttpResult",
        task_id: str,
    ) -> ProcessingResult:
        """Handle HTTP probe failure."""
        if http_result.is_retryable_error:
            log_event(
                logger,
                "domain_processing.http_transient_failure",
                level=logging.WARNING,
                task_id=task_id,
                domain=domain,
                error=http_result.error,
                status_code=http_result.status_code,
            )
            await self._reschedule_task(task, f"HTTP error: {http_result.error}")
            return ProcessingResult(
                status=TaskStatus.PENDING,
                error=http_result.error,
                error_category="retryable",
                http_result=http_result,
            )
        elif http_result.is_permanent_failure:
            log_event(
                logger,
                "domain_processing.http_permanent_failure",
                level=logging.WARNING,
                task_id=task_id,
                domain=domain,
                status_code=http_result.status_code,
            )
            error_msg = f"HTTP {http_result.status_code}"
            if http_result.error:
                error_msg += f": {http_result.error}"
            await self._fail_task(task, error_msg, "permanent")
            await self._maybe_deactivate_domain(
                task.domain_id, "permanent", "HTTP_PERMANENT_FAILURE"
            )
            return ProcessingResult(
                status=TaskStatus.FAILED,
                error=error_msg,
                error_category="permanent",
                http_result=http_result,
            )
        else:
            # Successful HTTP response (2xx, 3xx, 4xx)
            log_event(
                logger,
                "domain_processing.http_completed",
                level=logging.INFO,
                task_id=task_id,
                domain=domain,
                status_code=http_result.status_code,
            )
            # We'll handle successful HTTP responses in the main flow
            return ProcessingResult(
                status=TaskStatus.COMPLETED,
                http_result=http_result,
            )
    
    async def _persist_domain_detail(
        self,
        domain_id: uuid.UUID,
        dns_result: DnsResult,
        http_result: HttpResult,
        validated_ip: str,
    ) -> DomainDetail:
        """Create or update DomainDetail with observation data using OCC."""
        now = datetime.now(UTC)
        next_refresh = now + timedelta(seconds=self._settings.refresh_interval_seconds)

        # Prepare DNS records
        dns_records = {}
        if dns_result.ips_v4:
            dns_records["A"] = dns_result.ips_v4
        if dns_result.ips_v6:
            dns_records["AAAA"] = dns_result.ips_v6
        if dns_result.cname:
            dns_records["CNAME"] = [dns_result.cname]

        # Prepare IP addresses list
        ip_addresses = []
        ip_addresses.extend(dns_result.ips_v4)
        ip_addresses.extend(dns_result.ips_v6)

        # Get HTTP data
        http_status = http_result.status_code
        headers = http_result.headers
        page_title = http_result.page_title
        response_time = http_result.response_time_ms

        # Determine next refresh time based on success/failure
        if http_result.is_success:
            next_refresh = datetime.now(UTC) + timedelta(seconds=86400)  # 24 hours for successful
        else:
            next_refresh = datetime.now(UTC) + timedelta(seconds=3600)  # 1 hour for failed

        # Get existing detail for version (OCC)
        existing = await self._domain_detail_repo.get(domain_id)
        expected_version = existing.version if existing else 0

        detail = DomainDetail(
            domain_id=domain_id,
            ip_addresses=ip_addresses,
            dns_records=dns_records,
            http_status=http_status,
            page_title=page_title,
            response_time=response_time,
            response_headers=headers,
            fetched_at=datetime.now(UTC),
            next_refresh_at=next_refresh,
            version=(expected_version + 1) if existing else 1,
        )

        log_event(
            logger,
            "occ.update_attempted",
            level=logging.DEBUG,
            domain_id=str(domain_id),
            expected_version=expected_version,
        )

        # Use OCC-enabled upsert
        try:
            upserted = await self._domain_detail_repo.upsert_with_occ(
                detail, expected_version
            )
            log_event(
                logger,
                "occ.update_succeeded",
                level=logging.DEBUG,
                domain_id=str(domain_id),
                new_version=upserted.version,
            )
            return upserted
        except RuntimeError as e:
            if "OCC conflict" in str(e):
                log_event(
                    logger,
                    "occ.conflict",
                    level=logging.WARNING,
                    domain_id=str(domain_id),
                    expected_version=expected_version,
                    error=str(e),
                )
                # Re-read and retry once with fresh version
                existing = await self._domain_detail_repo.get(domain_id)
                if existing is None:
                    raise
                new_expected_version = existing.version
                detail.version = new_expected_version + 1

                log_event(
                    logger,
                    "occ.update_attempted",
                    level=logging.DEBUG,
                    domain_id=str(domain_id),
                    expected_version=new_expected_version,
                    retry=True,
                )

                upserted = await self._domain_detail_repo.upsert_with_occ(
                    detail, new_expected_version
                )
                log_event(
                    logger,
                    "occ.update_succeeded",
                    level=logging.DEBUG,
                    domain_id=str(domain_id),
                    new_version=upserted.version,
                    retry=True,
                )
                return upserted
            raise
    
    async def _complete_task(self, task: Task, status: TaskStatus) -> None:
        """Mark task as completed with given status."""
        task.status = status
        task.updated_at = datetime.now(UTC)
        await self._session.flush()
    
    async def _fail_task(self, task: Task, error: str, category: str) -> None:
        """Mark task as failed with error."""
        task.status = TaskStatus.FAILED
        task.updated_at = datetime.now(UTC)
        task.error_payload = {
            "code": category.upper() + "_ERROR",
            "message": error,
            "retryable": category == "retryable",
        }
        await self._session.flush()

    def _should_soft_deactivate(self, error_category: str, error_code: str) -> bool:
        """
        Determine if a permanent failure should trigger soft deactivation.
        
        According to architecture:
        - Confirmed NXDOMAIN -> deactivate
        - SSRF/security rejection -> deactivate
        - Max retries exceeded -> deactivate
        - Single transient failures (timeout, etc.) -> do NOT deactivate
        """
        if error_category == "security":
            # SSRF and security rejections are permanent and indicate invalid domain
            return True
        if error_code in ("DNS_PERMANENT_FAILURE", "PERMANENT_ERROR"):
            # Confirmed NXDOMAIN or other permanent DNS failures
            return True
        if error_code == "MAX_ATTEMPTS_EXCEEDED":
            # Max retries exceeded
            return True
        return False

    async def _maybe_deactivate_domain(
        self,
        domain_id: uuid.UUID,
        error_category: str,
        error_code: str,
    ) -> None:
        """
        Soft deactivate a domain if the failure warrants it.
        
        Only deactivates if the domain is currently active.
        """
        if not self._should_soft_deactivate(error_category, error_code):
            return
        
        domain = await self._domain_repo.get(domain_id)
        if domain is None or not domain.is_active:
            return
        
        now = datetime.now(UTC)
        domain.is_active = False
        domain.deactivated_at = now
        domain.updated_at = now
        await self._session.flush()
        
        log_event(
            logger,
            "domain.deactivated",
            level=logging.WARNING,
            domain_id=str(domain_id),
            domain=domain.normalized_domain,
            reason=f"permanent_failure:{error_code}",
        )
    
    async def _reschedule_task(self, task: Task, error: str) -> None:
        """Reschedule task for retry with backoff."""
        task.status = TaskStatus.PENDING
        task.attempts += 1
        task.updated_at = datetime.now(UTC)
        task.error_payload = {
            "code": "TRANSIENT_ERROR",
            "message": error,
            "retryable": True,
        }
        
        # Exponential backoff with jitter
        base_delay = 60  # 1 minute base
        delay = min(base_delay * (2 ** (task.attempts - 1)), 3600)
        import random
        jitter = random.randint(0, 60)
        task.next_attempt_at = datetime.now(UTC) + timedelta(seconds=delay + jitter)
        
        await self._session.flush()
    
    def _classify_exception(self, e: Exception) -> str:
        """Classify exception as retryable or permanent."""
        if isinstance(e, TimeoutError):
            return "retryable"
        
        # Check for HTTP client errors
        import httpx
        if isinstance(e, httpx.TimeoutException):
            return "retryable"
        if isinstance(e, httpx.ConnectError):
            return "retryable"
        if isinstance(e, httpx.TooManyRedirects):
            return "permanent"  # Redirect loops are permanent
        
        # Database errors
        from sqlalchemy.exc import DisconnectionError, OperationalError
        if isinstance(e, (OperationalError, DisconnectionError)):
            return "retryable"
        
        # Redis errors
        import redis
        if isinstance(e, redis.RedisError):
            return "retryable"
        
        # Default to permanent for unknown errors
        return "permanent"