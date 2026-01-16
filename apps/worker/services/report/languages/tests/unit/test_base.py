from services.report.languages.base import normalize_timestamp


class TestNormalizeTimestamp:
    def test_none_input(self):
        assert normalize_timestamp(None) is None

    def test_empty_string(self):
        assert normalize_timestamp("") is None

    def test_seconds_timestamp_unchanged(self):
        """10-digit Unix timestamps (seconds) should pass through unchanged."""
        # A recent timestamp in seconds
        assert normalize_timestamp("1768258631") == "1768258631"

    def test_milliseconds_timestamp_converted(self):
        """13-digit Unix timestamps (milliseconds) should be converted to seconds."""
        # 1768258631332 ms = 1768258631 seconds (2026-01-12 17:57:11)
        assert normalize_timestamp("1768258631332") == "1768258631"

    def test_milliseconds_timestamp_with_more_digits(self):
        """14+ digit timestamps should also be converted (future-proofing)."""
        assert normalize_timestamp("17682586313320") == "17682586313"

    def test_date_string_unchanged(self):
        """Date strings should pass through unchanged."""
        assert normalize_timestamp("2026-01-12") == "2026-01-12"
        assert normalize_timestamp("01-12-2026") == "01-12-2026"

    def test_date_with_time_unchanged(self):
        """Date/time strings should pass through unchanged."""
        assert normalize_timestamp("2026-01-12 17:57:11") == "2026-01-12 17:57:11"

    def test_mixed_content_unchanged(self):
        """Non-pure-digit strings should pass through unchanged."""
        assert normalize_timestamp("abc123") == "abc123"
        assert normalize_timestamp("12345abc") == "12345abc"
