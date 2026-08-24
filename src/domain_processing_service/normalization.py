"""Domain normalization and validation utilities."""

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class NormalizedDomain:
    """Result of domain normalization."""

    value: str
    is_valid: bool
    error: str | None = None


def _is_valid_domain_label(label: str) -> bool:
    """Check if a single domain label is valid."""
    if not label:
        return False
    if len(label) > 63:
        return False
    # Domain labels can contain letters, digits, and hyphens
    # but cannot start or end with hyphen
    if label.startswith("-") or label.endswith("-"):
        return False
    # Check for valid characters (ASCII letters, digits, hyphen)
    if not re.fullmatch(r"[a-z0-9-]+", label):
        return False
    return True


def _validate_domain_syntax(domain: str) -> tuple[bool, str | None]:
    """Validate domain syntax after normalization.

    Returns (is_valid, error_message).
    """
    if not domain:
        return False, "empty domain"

    if len(domain) > 253:
        return False, "domain too long"

    labels = domain.split(".")
    if len(labels) < 2:
        return False, "domain must have at least two labels"

    for label in labels:
        if not _is_valid_domain_label(label):
            return False, f"invalid label: {label}"

    return True, None


def normalize_domain(domain: str) -> NormalizedDomain:
    """Normalize a domain according to the architecture specification.

    Normalization steps:
    1. Trim leading/trailing whitespace
    2. Remove URL scheme if present (http://, https://)
    3. Convert to lowercase
    4. Remove trailing dot
    5. Apply IDNA (Punycode) encoding for internationalized domains
    6. Validate syntax

    Args:
        domain: Raw domain string from user input

    Returns:
        NormalizedDomain with value, validity, and error if invalid
    """
    # Step 1: Trim whitespace
    normalized = domain.strip()

    if not normalized:
        return NormalizedDomain(
            value="",
            is_valid=False,
            error="empty domain after whitespace trimming",
        )

    # Step 2: Remove URL scheme if present
    # Handle http://, https://, and other schemes
    scheme_pattern = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.-]*://")
    if scheme_pattern.match(normalized):
        # Extract the host part after the scheme
        # This is a simple extraction; we don't do full URL parsing
        after_scheme = scheme_pattern.sub("", normalized)
        # Take only the host part (before any path, query, port)
        host_part = after_scheme.split("/")[0].split("?")[0].split("#")[0]
        # Remove port if present
        host_part = host_part.split(":")[0]
        normalized = host_part

    # Step 3: Convert to lowercase
    normalized = normalized.lower()

    # Step 4: Remove trailing dot
    normalized = normalized.rstrip(".")

    if not normalized:
        return NormalizedDomain(
            value="",
            is_valid=False,
            error="empty domain after normalization",
        )

    # Step 5: Apply IDNA encoding for internationalized domains
    try:
        # Encode to IDNA (Punycode) - this handles Unicode domains
        # We encode each label separately to preserve ASCII labels
        labels = normalized.split(".")
        idna_labels = []
        for label in labels:
            # Try to encode as IDNA; if it's already ASCII, this is a no-op
            try:
                idna_label = label.encode("idna").decode("ascii")
            except UnicodeError:
                # If encoding fails, keep original (will be caught by validation)
                idna_label = label
            idna_labels.append(idna_label)
        normalized = ".".join(idna_labels)
    except Exception as e:
        return NormalizedDomain(
            value=normalized,
            is_valid=False,
            error=f"IDNA encoding failed: {e}",
        )

    # Step 6: Validate syntax
    is_valid, error = _validate_domain_syntax(normalized)
    if not is_valid:
        return NormalizedDomain(
            value=normalized,
            is_valid=False,
            error=error,
        )

    return NormalizedDomain(
        value=normalized,
        is_valid=True,
        error=None,
    )


def deduplicate_domains(domains: list[str]) -> list[str]:
    """Deduplicate domains using their normalized canonical form.

    Args:
        domains: List of raw domain strings

    Returns:
        List of unique normalized domains (preserving first occurrence order)
    """
    seen = set()
    result = []
    for domain in domains:
        normalized_result = normalize_domain(domain)
        if normalized_result.is_valid:
            norm_value = normalized_result.value
        else:
            # For invalid domains, use a special marker to allow
            # deduplication of identical invalid inputs
            norm_value = f"__invalid__{domain.strip().lower()}"
        if norm_value not in seen:
            seen.add(norm_value)
            result.append(norm_value if normalized_result.is_valid else domain)
    return result