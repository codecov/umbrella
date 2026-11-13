from typing import Any

from graphql import GraphQLError


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    """
    Drop GraphQL parse/validation errors (e.g. "Cannot query field ...").

    These are raised for malformed client queries and carry no
    ``original_error``; they are client mistakes, not server faults. They are
    still logged by ``AsyncGraphqlView.error_formatter``. GraphQL errors that
    wrap a real exception raised in a resolver are still reported.
    """
    exc_info = hint.get("exc_info")
    if exc_info:
        exc = exc_info[1]
        if isinstance(exc, GraphQLError) and exc.original_error is None:
            return None
    return event
