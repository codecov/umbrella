from typing import Any

from graphql import GraphQLError

BAD_QUERY_MARKER = "Cannot query field"


def drop_graphql_bad_query_events(
    event: dict[str, Any], hint: dict[str, Any]
) -> dict[str, Any] | None:
    """
    Sentry's auto-enabled Ariadne integration captures every GraphQL validation
    error as an unhandled exception. A query that asks for a field missing from
    the schema ("Cannot query field ...") is a client mistake, not a server
    failure. It is still handled, logged as a warning in
    graphql_api.views.error_formatter, and counted by the GQL error metrics,
    so the Sentry event is noise and gets dropped here. Every other event,
    including resolver errors that wrap an original exception, passes through
    unchanged.
    """
    exc_info = hint.get("exc_info") if hint else None
    if not exc_info:
        return event
    exc = exc_info[1]
    if (
        isinstance(exc, GraphQLError)
        and exc.original_error is None
        and BAD_QUERY_MARKER in (exc.message or "")
    ):
        return None
    return event
