from typing import Any

from graphql import GraphQLError


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    """
    Drop GraphQL schema validation errors ("Cannot query field ...") that the
    auto-enabled Ariadne integration captures. These are caused by clients
    querying fields that don't exist in our schema; they are client errors
    (handled in GraphQLView.error_formatter), not server failures.
    Real resolver exceptions carry an `original_error` and are still reported.
    """
    exc_info = hint.get("exc_info")
    if exc_info:
        exc = exc_info[1]
        if (
            isinstance(exc, GraphQLError)
            and exc.original_error is None
            and str(exc.message).startswith("Cannot query field")
        ):
            return None
    return event
