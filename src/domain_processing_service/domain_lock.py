"""Redis domain locking for Phase 9 domain processing coordination."""

import logging
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

import redis.asyncio as redis

from domain_processing_service.config import AppSettings
from domain_processing_service.logging import log_event

if TYPE_CHECKING:
    from redis.asyncio import Redis

    from domain_processing_service.models import DomainDetail

logger = logging.getLogger(__name__)


class DomainLock:
    """Redis-based distributed lock for domain processing coordination."""
    
    def __init__(self, settings: AppSettings) -> None:
        self._settings = settings
        self._client: Redis | None = None
        self._lock_ttl_seconds = 60  # Lock TTL - must exceed max processing time
    
    async def connect(self) -> None:
        """Initialize Redis connection."""
        if self._client is None:
            client = redis.Redis(
                host=self._settings.redis_host
                if hasattr(self._settings, "redis_host")
                else "localhost",
                port=self._settings.redis_port
                if hasattr(self._settings, "redis_port")
                else 6379,
                password=self._settings.redis_password
                if hasattr(self._settings, "redis_password")
                else None,
                db=self._settings.redis_db if hasattr(self._settings, "redis_db") else 0,
                decode_responses=True,
                socket_timeout=5.0,
                socket_connect_timeout=5.0,
            )
            self._client = client
            # Test connection
            await client.ping()
            logger.info("Redis connection established for domain locking")
    
    async def close(self) -> None:
        """Close Redis connection."""
        if self._client is not None:
            await self._client.close()
            self._client = None
    
    @asynccontextmanager
    async def acquire(self, normalized_domain: str) -> AsyncIterator["LockContext"]:
        """
        Acquire a domain lock.
        """
        if self._client is None:
            await self.connect()
        
        client = self._client
        assert client is not None
        token = str(uuid.uuid4())
        lock_key = f"lock:domain:{normalized_domain}"
        
        acquired = False
        error_msg = None
        try:
            acquired_result = await client.set(
                lock_key,
                token,
                nx=True,
                px=self._lock_ttl_seconds * 1000,
            )
            acquired = bool(acquired_result)
        except Exception as e:
            log_event(
                logger,
                "domain_lock.failed",
                level=logging.ERROR,
                domain=normalized_domain,
                error=str(e),
                error_type=type(e).__name__,
            )
            error_msg = str(e)
            
        try:
            if error_msg is not None:
                yield LockContext(
                    lock=self,
                    domain=normalized_domain,
                    token=None,
                    acquired=False,
                    error=error_msg,
                )
            elif acquired:
                log_event(
                    logger,
                    "domain_lock.acquired",
                    level=logging.DEBUG,
                    domain=normalized_domain,
                    token=token,
                )
                yield LockContext(
                    lock=self,
                    domain=normalized_domain,
                    token=token,
                    acquired=True,
                )
            else:
                log_event(
                    logger,
                    "domain_lock.failed",
                    level=logging.DEBUG,
                    domain=normalized_domain,
                    reason="already_held",
                )
                yield LockContext(
                    lock=self,
                    domain=normalized_domain,
                    token=None,
                    acquired=False,
                )
        finally:
            if acquired:
                await self.release(normalized_domain, token)

    async def release(self, normalized_domain: str, token: str) -> bool:
        """
        Release a domain lock.
        
        Uses Lua script to verify token ownership before deletion.
        
        Args:
            normalized_domain: The domain that was locked
            token: The ownership token returned when lock was acquired
            
        Returns:
            True if lock was released, False if not owned or already expired
        """
        if self._client is None:
            return False
        
        # Lua script to atomically check token and delete
        lua_script = """
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("DEL", KEYS[1])
        else
            return 0
        end
        """
        
        try:
            result = await self._client.eval(
                lua_script, 1, f"lock:domain:{normalized_domain}", token
            )
            if result:
                log_event(
                    logger,
                    "domain_lock.released",
                    level=logging.DEBUG,
                    domain=normalized_domain,
                    token=token,
                )
                return True
            else:
                log_event(
                    logger,
                    "domain_lock.release_failed",
                    level=logging.WARNING,
                    domain=normalized_domain,
                    reason="token_mismatch_or_expired",
                )
                return False
        except Exception as e:
            log_event(
                logger,
                "domain_lock.failed",
                level=logging.ERROR,
                domain=normalized_domain,
                error=str(e),
                error_type=type(e).__name__,
            )
            return False


@dataclass(frozen=True)
class LockContext:
    """Context manager for domain lock."""
    
    lock: "DomainLock"
    domain: str
    token: str | None
    acquired: bool
    error: str | None = None
    
    @property
    def is_acquired(self) -> bool:
        return self.acquired and self.token is not None
    
    async def __aenter__(self) -> "LockContext":
        return self
    
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object | None,
    ) -> None:
        if self.acquired and self.token:
            await self.lock.release(self.domain, self.token)
    
    def __enter__(self) -> "LockContext":
        raise TypeError("Use async with for DomainLock")
    
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object | None,
    ) -> None:
        raise TypeError("Use async with for DomainLock")


class DomainLockManager:
    """
    High-level domain lock manager that integrates with the worker pipeline.
    
    Handles the full lock acquisition, double-check, and release flow.
    """
    
    def __init__(self, settings: AppSettings) -> None:
        self._settings = settings
        self._lock = DomainLock(settings)
    
    async def connect(self) -> None:
        """Initialize Redis connection."""
        await self._lock.connect()
    
    async def close(self) -> None:
        """Close Redis connection."""
        await self._lock.close()
    
    @asynccontextmanager
    async def process_domain(
        self,
        normalized_domain: str,
        freshness_check: Callable[[str], Awaitable[tuple[bool, Optional["DomainDetail"]]]],
        process_func: Callable[[str], Awaitable[object]],
    ) -> AsyncIterator[tuple[bool, object]]:
        """
        Full domain processing with lock, double-check, and processing.
        
        This implements the double-check locking pattern:
        1. Acquire Redis lock for the domain
        2. Double-check DomainDetail freshness after lock acquisition
        3. If still stale, execute process_func
        4. Release lock
        
        Args:
            normalized_domain: The normalized domain to process
            freshness_check: Async function that returns (is_fresh, domain_detail)
            process_func: Async function to perform actual domain processing
            
        Yields:
            Tuple of (lock_acquired: bool, result: Any)
        """
        async with self._lock.acquire(normalized_domain) as lock_ctx:
            if not lock_ctx.is_acquired:
                logger.debug(
                    "Could not acquire lock for %s, yielding",
                    normalized_domain
                )
                yield (False, None)
                return
            
            # Double-check: re-check freshness after acquiring lock
            is_fresh, domain_detail = await freshness_check(normalized_domain)
            
            if is_fresh:
                logger.info(
                    "Domain %s became fresh after lock acquisition, skipping processing",
                    normalized_domain
                )
                yield (True, None)
                return
            
            # Process the domain
            try:
                result = await process_func(normalized_domain)
                yield (True, result)
            except Exception as e:
                logger.error("Error processing domain %s: %s", normalized_domain, e)
                raise