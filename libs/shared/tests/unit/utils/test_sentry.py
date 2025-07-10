from sentry_sdk import start_span

from shared.utils.sentry import current_sentry_trace_id


def test_matching_sentry_trace_id():
    """
    Test that the current_sentry_trace_id function returns the correct trace ID
    when a span is active.
    """
    with start_span(op="test", trace_id="1234567890abcdef"):
        assert current_sentry_trace_id() == "1234567890abcdef"


def test_empty_sentry_trace_id():
    """
    Test that the current_sentry_trace_id function returns None when no span is
    active.
    """
    assert current_sentry_trace_id() is None
