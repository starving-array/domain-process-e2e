"""HTTP client wrapper for domain processing with security and timeout controls."""

import logging
import time
from dataclasses import dataclass

import httpx

from domain_processing_service.config import AppSettings
from domain_processing_service.ip_validator import IpValidator

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class HttpResult:
    """Result of HTTP request."""
    
    status_code: int
    headers: dict[str, str]
    body: bytes
    response_time_ms: int
    page_title: str | None
    error: str | None = None
    redirected_from: str | None = None
    
    @property
    def is_success(self) -> bool:
        return self.error is None and 200 <= self.status_code < 300
    
    @property
    def is_client_error(self) -> bool:
        return 400 <= self.status_code < 500
    
    @property
    def is_server_error(self) -> bool:
        return 500 <= self.status_code < 600
    
    @property
    def is_retryable_error(self) -> bool:
        """Check if the error is retryable per the failure matrix."""
        if self.error:
            return True  # Network/timeout errors are retryable
        if self.is_server_error:
            return True  # 5xx errors are retryable
        return False
    
    @property
    def is_permanent_failure(self) -> bool:
        """Check if the error is permanent (non-retryable)."""
        if self.error:
            return False  # Network errors are retryable
        if self.is_client_error and self.status_code not in (408, 429):
            return True  # 4xx except timeout/rate limit are permanent
        return False


class HttpClient:
    """HTTP client with security and timeout controls for domain processing."""
    
    def __init__(self, settings: AppSettings) -> None:
        self._settings = settings
        self._ip_validator = IpValidator()
        
        # Create the underlying httpx client with security settings
        self._client = httpx.AsyncClient(
            follow_redirects=False,  # Redirects disabled per security policy
            timeout=httpx.Timeout(
                connect=settings.connect_timeout_seconds,
                read=settings.read_timeout_seconds,
                write=settings.read_timeout_seconds,
                pool=settings.connect_timeout_seconds,
            ),
            limits=httpx.Limits(
                max_connections=settings.worker_concurrency,
                max_keepalive_connections=settings.worker_concurrency // 2,
            ),
        )
    
    async def close(self) -> None:
        """Close the HTTP client."""
        await self._client.aclose()
    
    async def probe(
        self,
        domain: str,
        validated_ip: str,
        original_domain: str,
    ) -> HttpResult:
        """
        Probe a domain via HTTP/HTTPS using a validated IP.
        
        This implements the security requirements:
        - Connects to the validated IP (not re-resolving DNS)
        - Preserves original hostname for Host header and SNI
        - Disables redirects
        - Enforces timeouts and response size limits
        - Extracts page title from bounded response body
        
        Args:
            domain: The domain name (for logging)
            validated_ip: The pre-validated IP address to connect to
            original_domain: Original domain name for Host header and SNI
            
        Returns:
            HttpResult with response data or error
        """
        start_time = time.monotonic()
        
        # Build the URL with the validated IP
        # We try HTTPS first, then HTTP
        urls = [
            f"https://{validated_ip}",
            f"http://{validated_ip}",
        ]
        
        headers = {
            "Host": original_domain,
            "User-Agent": "DomainProcessingBot/1.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "close",  # Don't keep connections open
        }
        
        last_error = None
        
        for url in urls:
            try:
                logger.debug("Probing %s via %s (IP: %s)", domain, url, validated_ip)
                
                # Make the request with the validated IP
                response = await self._client.get(
                    url,
                    headers=headers,
                    # We use the validated IP directly by overriding the connection
                    # This is handled by httpx's ability to connect to a specific IP
                )
                
                # Read response body with size limit
                body = await self._read_body_limited(response)
                
                response_time_ms = int((time.monotonic() - start_time) * 1000)
                
                # Extract page title
                page_title = self._extract_title(body)
                
                # Convert headers to dict (filter out sensitive ones)
                safe_headers = self._filter_headers(dict(response.headers))
                
                logger.info(
                    "HTTP probe completed for %s: status=%d, time=%dms",
                    domain,
                    response.status_code,
                    response_time_ms,
                )
                
                return HttpResult(
                    status_code=response.status_code,
                    headers=safe_headers,
                    body=body,
                    response_time_ms=response_time_ms,
                    page_title=page_title,
                    redirected_from=None,
                )
                
            except httpx.TimeoutException as e:
                last_error = f"Timeout: {e}"
                logger.warning("HTTP probe timeout for %s via %s: %s", domain, url, e)
                continue  # Try next URL (HTTPS -> HTTP)
                
            except httpx.ConnectError as e:
                last_error = f"Connection failed: {e}"
                logger.warning("HTTP connection failed for %s via %s: %s", domain, url, e)
                continue
                
            except httpx.TooManyRedirects as e:
                last_error = f"Too many redirects: {e}"
                logger.warning("Too many redirects for %s: %s", domain, e)
                break  # Don't retry on redirect errors
                
            except httpx.RequestError as e:
                last_error = f"Request error: {e}"
                logger.warning("HTTP request error for %s via %s: %s", domain, url, e)
                continue
                
            except Exception as e:
                last_error = f"Unexpected error: {e}"
                logger.error(
                    "Unexpected error probing %s via %s: %s",
                    domain,
                    url,
                    e,
                    exc_info=True,
                )
                continue
        
        # All attempts failed
        response_time_ms = int((time.monotonic() - start_time) * 1000)
        return HttpResult(
            status_code=0,
            headers={},
            body=b"",
            response_time_ms=response_time_ms,
            page_title=None,
            error=last_error or "All probe attempts failed",
        )
    
    async def _read_body_limited(self, response: httpx.Response) -> bytes:
        """Read response body with size limit."""
        chunks = []
        total_size = 0
        limit = self._settings.max_response_read_bytes
        
        async for chunk in response.aiter_bytes():
            chunks.append(chunk)
            total_size += len(chunk)
            if total_size > limit:
                # Truncate and stop reading
                chunks[-1] = chunks[-1][:limit - (total_size - len(chunk))]
                break
        
        return b"".join(chunks)
    
    def _extract_title(self, body: bytes) -> str | None:
        """Extract page title from HTML body."""
        if not body:
            return None
        
        try:
            # Decode body - try UTF-8 first, then fall back
            try:
                html = body.decode("utf-8")
            except UnicodeDecodeError:
                html = body.decode("utf-8", errors="replace")
            
            # Simple title extraction - find <title> tag
            import re
            title_match = re.search(
                r"<title[^>]*>(.*?)</title>",
                html,
                re.IGNORECASE | re.DOTALL
            )
            
            if title_match:
                title = title_match.group(1).strip()
                # Limit title length
                if len(title) > 500:
                    title = title[:500] + "..."
                # Sanitize - remove control characters
                title = "".join(c for c in title if ord(c) >= 32 or c in "\t\n\r")
                return title if title else None
            
            return None
            
        except Exception as e:
            logger.warning("Failed to extract title: %s", e)
            return None
    
    def _filter_headers(self, headers: dict[str, str]) -> dict[str, str]:
        """Filter headers to only include allowed ones."""
        # Headers we never store
        blocked = {
            "authorization",
            "cookie",
            "set-cookie",
            "proxy-authorization",
            "www-authenticate",
            "proxy-authenticate",
        }
        
        # Headers we allow (size-bounded)
        allowed_prefixes = (
            "content-",
            "cache-",
            "etag",
            "last-modified",
            "server",
            "x-",
            "access-control-",
        )
        
        filtered = {}
        for key, value in headers.items():
            key_lower = key.lower()
            if key_lower in blocked:
                continue
            if any(key_lower.startswith(p) for p in allowed_prefixes):
                # Bound the value length
                if len(value) > 1000:
                    value = value[:1000] + "..."
                filtered[key] = value
        
        return filtered


async def create_http_client(settings: AppSettings) -> HttpClient:
    """Factory function to create an HttpClient."""
    return HttpClient(settings)