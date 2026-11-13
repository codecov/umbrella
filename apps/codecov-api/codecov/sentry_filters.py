from typing import Any

from graphql import GraphQLError


def is_graphql_client_query_error_event(hint: dict[str, Any]) -> bool:
    """
    The Ariadne Sentry integration (auto-enabled) captures every GraphQLError
    in a response, including parse/validation errors caused by malformed
    client queries (e.g. "Cannot query field 'x' on type 'Query'"). Those are
    client errors, not server failures. They have no underlying exception and
    no execution path, which distinguishes them from resolver errors, which
    are still reported.
    """
    exc_info = hint.get("exc_info")
    if not exc_info or len(exc_info) < 2:
        return False
    exc = exc_info[1]
    return (
        isinstance(exc, GraphQLError)
        and exc.original_error is None
        and exc.path is None
    )


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    if is_graphql_client_query_error_event(hint):
        return None
    return event
