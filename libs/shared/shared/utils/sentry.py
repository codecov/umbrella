from sentry_sdk import get_current_span


def current_sentry_trace_id() -> str | None:
    """
    Returns the Sentry trace ID from the current span if available.
    """
    span = get_current_span()
    if span and span.trace_id:
        return span.trace_id
    return None
