"""DNS resolution utilities for domain processing."""

import asyncio
import logging
from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

import aiodns

from domain_processing_service.config import AppSettings

if TYPE_CHECKING:
    from aiodns import DNSResolver as AiodnsResolver

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DnsResult:
    """Result of DNS resolution."""
    
    ips_v4: list[str]
    ips_v6: list[str]
    cname: str | None
    error: str | None = None
    
    @property
    def is_success(self) -> bool:
        return self.error is None and bool(self.ips_v4 or self.ips_v6)
    
    @property
    def all_ips(self) -> list[str]:
        return self.ips_v4 + self.ips_v6


class DnsResolver:
    """DNS resolver with configurable timeouts."""
    
    def __init__(self, settings: AppSettings) -> None:
        self._settings = settings
        self._resolver: AiodnsResolver = aiodns.DNSResolver()
        object.__setattr__(self._resolver, "timeout", settings.dns_timeout_seconds)
        object.__setattr__(self._resolver, "tries", 1)
    
    async def resolve(self, domain: str) -> DnsResult:
        """
        Resolve a domain to its IP addresses.
        
        Performs A, AAAA, and CNAME lookups.
        
        Args:
            domain: The domain to resolve (already normalized)
            
        Returns:
            DnsResult with resolved IPs and any error
        """
        # Perform A, AAAA, and CNAME lookups concurrently
        a_task = self._resolve_a(domain)
        aaaa_task = self._resolve_aaaa(domain)
        cname_task = self._resolve_cname(domain)
        
        results: tuple[
            list[str] | BaseException,
            list[str] | BaseException,
            str | BaseException | None,
        ] = await asyncio.gather(
            a_task, aaaa_task, cname_task, return_exceptions=True
        )
        
        ips_v4_result, ips_v6_result, cname_result = results
        
        # Handle exceptions from individual lookups
        ips_v4: list[str] = []
        if isinstance(ips_v4_result, Exception):
            logger.warning("A record lookup failed for %s: %s", domain, ips_v4_result)
        else:
            ips_v4 = cast(list[str], ips_v4_result)
        
        ips_v6: list[str] = []
        if isinstance(ips_v6_result, Exception):
            logger.warning("AAAA record lookup failed for %s: %s", domain, ips_v6_result)
        else:
            ips_v6 = cast(list[str], ips_v6_result)
        
        cname: str | None = None
        if isinstance(cname_result, Exception):
            logger.warning("CNAME lookup failed for %s: %s", domain, cname_result)
        else:
            cname = cast(str | None, cname_result)
        
        return DnsResult(
            ips_v4=ips_v4,
            ips_v6=ips_v6,
            cname=cname,
        )
    
    async def _resolve_a(self, domain: str) -> list[str]:
        """Resolve A records (IPv4)."""
        try:
            result = await self._resolver.query(domain, "A")
            return [r.host for r in result]
        except aiodns.error.DNSError as e:
            if e.args[0] == aiodns.error.ARES_ENOTFOUND:
                return []  # NXDOMAIN
            if e.args[0] == aiodns.error.ARES_ENODATA:
                return []  # No A records
            raise
    
    async def _resolve_aaaa(self, domain: str) -> list[str]:
        """Resolve AAAA records (IPv6)."""
        try:
            result = await self._resolver.query(domain, "AAAA")
            return [r.host for r in result]
        except aiodns.error.DNSError as e:
            if e.args[0] == aiodns.error.ARES_ENOTFOUND:
                return []  # NXDOMAIN
            if e.args[0] == aiodns.error.ARES_ENODATA:
                return []  # No AAAA records
            raise
    
    async def _resolve_cname(self, domain: str) -> str | None:
        """Resolve CNAME record."""
        try:
            result = await self._resolver.query(domain, "CNAME")
            # CNAME query returns an iterable of results
            for r in cast("Iterable[Any]", result):
                return cast(str, r.host)
            return None
        except aiodns.error.DNSError:
            return None


class DnsClassificationError(Exception):
    """Exception for DNS classification failures."""
    pass


def classify_dns_error(error: Exception) -> str:
    """
    Classify a DNS error as retryable or permanent.
    
    Returns:
        "retryable" for transient errors (timeout, SERVFAIL)
        "permanent" for permanent errors (NXDOMAIN, no data)
        "unknown" for other errors
    """
    if isinstance(error, TimeoutError):
        return "retryable"
    
    if isinstance(error, aiodns.error.DNSError):
        error_code = error.args[0] if error.args else None
        if error_code in (
            aiodns.error.ARES_ETIMEOUT,
            aiodns.error.ARES_ESERVFAIL,
        ):
            return "retryable"
        if error_code in (
            aiodns.error.ARES_ENOTFOUND,  # NXDOMAIN
            aiodns.error.ARES_ENODATA,    # No records
        ):
            return "permanent"
    
    return "unknown"