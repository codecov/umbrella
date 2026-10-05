from typing import Any

from graphql import GraphQLError


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    """
    Drop client-side GraphQL errors (query syntax / validation errors such as
    missing required input fields or unknown fields). These are raised before
    any resolver runs, have no `original_error`, and are caused by malformed
    client requests rather than server faults.

    GraphQLErrors that wrap a real exception raised in a resolver are kept.
    """
    exc_info = hint.get("exc_info") if hint else None
    if exc_info:
        exc_value = exc_info[1]
        if (
            isinstance(exc_value, GraphQLError)
            and getattr(exc_value, "original_error", None) is None
        ):
            return None
    return event
