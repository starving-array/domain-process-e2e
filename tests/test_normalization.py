"""Unit tests for domain normalization and validation."""


from domain_processing_service.normalization import (
    NormalizedDomain,
    deduplicate_domains,
    normalize_domain,
)


class TestNormalizeDomain:
    """Tests for normalize_domain function."""

    def test_normal_domain(self) -> None:
        """Test normal domain normalization."""
        result = normalize_domain("example.com")
        assert isinstance(result, NormalizedDomain)
        assert result.is_valid is True
        assert result.value == "example.com"
        assert result.error is None

    def test_leading_trailing_whitespace(self) -> None:
        """Test normalization handles leading/trailing whitespace."""
        result = normalize_domain("  example.com  ")
        assert result.is_valid is True
        assert result.value == "example.com"

    def test_uppercase_input(self) -> None:
        """Test normalization converts to lowercase."""
        result = normalize_domain("Example.COM")
        assert result.is_valid is True
        assert result.value == "example.com"

    def test_trailing_dot(self) -> None:
        """Test normalization removes trailing dot."""
        result = normalize_domain("example.com.")
        assert result.is_valid is True
        assert result.value == "example.com"

    def test_whitespace_uppercase_trailing_dot_combination(self) -> None:
        """Test combination of whitespace, uppercase, and trailing dot."""
        result = normalize_domain("  EXAMPLE.COM.  ")
        assert result.is_valid is True
        assert result.value == "example.com"

    def test_idn_unicode_domain(self) -> None:
        """Test IDN/Unicode domain normalization (Punycode)."""
        # Test with a Unicode domain (e.g., münchen.de -> xn--mnchen-3ya.de)
        result = normalize_domain("münchen.de")
        assert result.is_valid is True
        assert result.value == "xn--mnchen-3ya.de"

    def test_idn_unicode_with_whitespace_and_case(self) -> None:
        """Test IDN with whitespace and mixed case."""
        result = normalize_domain("  MÜNCHEN.DE  ")
        assert result.is_valid is True
        assert result.value == "xn--mnchen-3ya.de"

    def test_scheme_https(self) -> None:
        """Test normalization removes https:// scheme."""
        result = normalize_domain("https://example.com")
        assert result.is_valid is True
        assert result.value == "example.com"

    def test_scheme_http(self) -> None:
        """Test normalization removes http:// scheme."""
        result = normalize_domain("http://example.com")
        assert result.is_valid is True
        assert result.value == "example.com"

    def test_scheme_with_path(self) -> None:
        """Test normalization removes scheme and path."""
        result = normalize_domain("https://example.com/path/to/resource")
        assert result.is_valid is True
        assert result.value == "example.com"

    def test_scheme_with_query(self) -> None:
        """Test normalization removes scheme and query string."""
        result = normalize_domain("https://example.com?query=value")
        assert result.is_valid is True
        assert result.value == "example.com"

    def test_scheme_with_fragment(self) -> None:
        """Test normalization removes scheme and fragment."""
        result = normalize_domain("https://example.com#section")
        assert result.is_valid is True
        assert result.value == "example.com"

    def test_scheme_with_port(self) -> None:
        """Test normalization removes scheme and port."""
        result = normalize_domain("https://example.com:8080")
        assert result.is_valid is True
        assert result.value == "example.com"

    def test_scheme_uppercase(self) -> None:
        """Test normalization handles uppercase scheme."""
        result = normalize_domain("HTTPS://EXAMPLE.COM")
        assert result.is_valid is True
        assert result.value == "example.com"

    def test_empty_input(self) -> None:
        """Test empty input returns invalid."""
        result = normalize_domain("")
        assert result.is_valid is False
        assert result.error is not None
        assert result.value == ""

    def test_whitespace_only(self) -> None:
        """Test whitespace-only input returns invalid."""
        result = normalize_domain("   ")
        assert result.is_valid is False
        assert result.error is not None

    def test_single_label(self) -> None:
        """Test single label domain (e.g., localhost) is invalid."""
        result = normalize_domain("localhost")
        assert result.is_valid is False
        assert result.error is not None

    def test_invalid_label_starts_with_hyphen(self) -> None:
        """Test label starting with hyphen is invalid."""
        result = normalize_domain("-example.com")
        assert result.is_valid is False
        assert result.error is not None

    def test_invalid_label_ends_with_hyphen(self) -> None:
        """Test label ending with hyphen is invalid."""
        result = normalize_domain("example-.com")
        assert result.is_valid is False
        assert result.error is not None

    def test_invalid_label_too_long(self) -> None:
        """Test label longer than 63 chars is invalid."""
        long_label = "a" * 64
        result = normalize_domain(f"{long_label}.com")
        assert result.is_valid is False
        assert result.error is not None

    def test_invalid_characters(self) -> None:
        """Test domain with invalid characters (underscore)."""
        result = normalize_domain("ex_ample.com")
        assert result.is_valid is False
        assert result.error is not None

    def test_domain_too_long(self) -> None:
        """Test domain longer than 253 chars is invalid."""
        long_domain = "a." + "b." * 200 + "com"
        result = normalize_domain(long_domain)
        assert result.is_valid is False
        assert result.error is not None

    def test_normalized_value_for_invalid_domains(self) -> None:
        """Test that invalid domains still return a normalized value."""
        result = normalize_domain("INVALID..DOMAIN")
        assert result.is_valid is False
        assert result.value is not None


class TestDeduplicateDomains:
    """Tests for deduplicate_domains function."""

    def test_duplicate_normalized_domains(self) -> None:
        """Test deduplication of domains that normalize to same value."""
        domains = ["example.com", "Example.COM", "EXAMPLE.COM."]
        result = deduplicate_domains(domains)
        assert len(result) == 1
        assert result[0] == "example.com"

    def test_whitespace_variations_deduplicated(self) -> None:
        """Test domains with different whitespace are deduplicated."""
        domains = ["example.com", "  example.com  ", "example.com  "]
        result = deduplicate_domains(domains)
        assert len(result) == 1
        assert result[0] == "example.com"

    def test_mixed_valid_invalid_deduplication(self) -> None:
        """Test deduplication handles mixed valid/invalid domains."""
        domains = ["example.com", "invalid..domain", "Example.COM"]
        result = deduplicate_domains(domains)
        # Should have 2 entries: one valid (example.com), one invalid
        assert len(result) == 2

    def test_preserves_order_of_first_occurrence(self) -> None:
        """Test deduplication preserves first occurrence order."""
        domains = ["b.com", "a.com", "B.COM", "A.COM"]
        result = deduplicate_domains(domains)
        assert result == ["b.com", "a.com"]

    def test_empty_list(self) -> None:
        """Test empty list returns empty list."""
        result = deduplicate_domains([])
        assert result == []

    def test_single_domain(self) -> None:
        """Test single domain returns as-is."""
        result = deduplicate_domains(["example.com"])
        assert result == ["example.com"]

    def test_idn_deduplication(self) -> None:
        """Test IDN domains are deduplicated correctly."""
        domains = ["münchen.de", "MÜNCHEN.DE"]
        result = deduplicate_domains(domains)
        assert len(result) == 1
        assert result[0] == "xn--mnchen-3ya.de"