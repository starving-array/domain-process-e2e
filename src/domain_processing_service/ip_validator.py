"""IP address validation for SSRF protection."""

import ipaddress
import logging
from dataclasses import dataclass

from domain_processing_service.logging import log_event

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class IpValidationResult:
    """Result of IP validation."""
    
    is_allowed: bool
    ip: str
    reason: str | None = None
    
    @property
    def is_rejected(self) -> bool:
        return not self.is_allowed


# Private and reserved IP ranges that must be blocked
# Based on RFC 1918, RFC 4193, RFC 3927, RFC 5735, RFC 6598, etc.
BLOCKED_NETWORKS = [
    # Loopback
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("::1/128"),
    
    # Private IPv4 (RFC 1918)
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    
    # Link-local (RFC 3927) - includes cloud metadata endpoints
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("fe80::/10"),
    
    # Private IPv6 (RFC 4193 - Unique Local Addresses)
    ipaddress.ip_network("fc00::/7"),
    
    # Multicast
    ipaddress.ip_network("224.0.0.0/4"),
    ipaddress.ip_network("ff00::/8"),
    
    # Unspecified
    ipaddress.ip_network("0.0.0.0/32"),
    ipaddress.ip_network("::/128"),
    
    # Reserved
    ipaddress.ip_network("240.0.0.0/4"),
    
    # Benchmarking (RFC 2544)
    ipaddress.ip_network("198.18.0.0/15"),
    ipaddress.ip_network("198.19.0.0/16"),
    
    # Carrier-grade NAT (RFC 6598)
    ipaddress.ip_network("100.64.0.0/10"),
    
    # Documentation (RFC 5737)
    ipaddress.ip_network("192.0.2.0/24"),
    ipaddress.ip_network("198.51.100.0/24"),
    ipaddress.ip_network("203.0.113.0/24"),
    ipaddress.ip_network("2001:db8::/32"),
]


class IpValidator:
    """Validates IP addresses against SSRF blocklist."""
    
    def __init__(self) -> None:
        self._blocked_networks = BLOCKED_NETWORKS
    
    def validate(self, ip_str: str) -> IpValidationResult:
        """
        Validate an IP address against the SSRF blocklist.
        
        Args:
            ip_str: IP address string to validate
            
        Returns:
            IpValidationResult with validation result and reason if rejected
        """
        try:
            ip = ipaddress.ip_address(ip_str)
        except ValueError as e:
            log_event(
                logger,
                "ip_validator.rejected",
                level=logging.WARNING,
                ip=ip_str,
                reason=f"Invalid IP address format: {e}",
            )
            return IpValidationResult(
                is_allowed=False,
                ip=ip_str,
                reason=f"Invalid IP address format: {e}",
            )
        
        # Check against blocked networks
        for network in self._blocked_networks:
            if ip in network:
                log_event(
                    logger,
                    "ip_validator.rejected",
                    level=logging.WARNING,
                    ip=ip_str,
                    network=str(network),
                    reason=f"IP blocked: matches restricted network {network}",
                )
                return IpValidationResult(
                    is_allowed=False,
                    ip=ip_str,
                    reason=f"IP blocked: matches restricted network {network}",
                )
        
        # Check for IPv4-mapped IPv6 addresses
        if ip.version == 6 and ip.ipv4_mapped:
            mapped = ip.ipv4_mapped
            for network in self._blocked_networks:
                if network.version == 4 and mapped in network:
                    log_event(
                        logger,
                        "ip_validator.rejected",
                        level=logging.WARNING,
                        ip=ip_str,
                        mapped_ipv4=str(mapped),
                        network=str(network),
                        reason=f"IPv4-mapped address blocked: {network}",
                    )
                    return IpValidationResult(
                        is_allowed=False,
                        ip=ip_str,
                        reason=f"IPv4-mapped address blocked: {network}",
                    )
        
        return IpValidationResult(
            is_allowed=True,
            ip=ip_str,
        )
    
    def validate_all(self, ips: list[str]) -> list[IpValidationResult]:
        """Validate multiple IP addresses."""
        return [self.validate(ip) for ip in ips]
    
    def get_first_allowed(self, ips: list[str]) -> str | None:
        """
        Get the first allowed IP from a list.
        
        Returns the first allowed IP, or None if all are blocked.
        """
        for ip in ips:
            result = self.validate(ip)
            if result.is_allowed:
                return ip
        return None


# Convenience function for quick validation
def is_ip_allowed(ip_str: str) -> bool:
    """Quick check if an IP is allowed (not blocked)."""
    validator = IpValidator()
    return validator.validate(ip_str).is_allowed


def get_first_allowed_ip(ips: list[str]) -> str | None:
    """Get the first allowed IP from a list."""
    validator = IpValidator()
    return validator.get_first_allowed(ips)