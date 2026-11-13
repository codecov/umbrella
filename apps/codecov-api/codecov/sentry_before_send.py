from typing import Any

from graphql import GraphQLError


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    """
    Drop GraphQL validation/parse errors (e.g. "Cannot query field ...").

    These are raised by GraphQL itself when a client sends an invalid query and
    have no underlying `original_error`. They are client errors, not server
    errors, so they should not be reported to Sentry. Errors raised from
    resolvers keep their `original_error` and are still reported.
    """
    exc_info = hint.get("exc_info") if hint else None
    if exc_info:
        exc = exc_info[1]
        if isinstance(exc, GraphQLError) and exc.original_error is None:
            return None
    return event
