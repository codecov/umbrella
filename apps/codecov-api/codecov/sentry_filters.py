from typing import Any

from graphql import GraphQLError

# Schema validation errors produced when a client queries a field that does not
# exist. These are caused purely by client input that is rejected before any
# resolver runs, so they are harmless and not actionable server-side.
GRAPHQL_UNKNOWN_FIELD_MESSAGE = "Cannot query field"


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    exc_info = hint.get("exc_info") if hint else None
    if exc_info:
        exc = exc_info[1]
        if (
            isinstance(exc, GraphQLError)
            and exc.original_error is None
            and GRAPHQL_UNKNOWN_FIELD_MESSAGE in str(exc.message)
        ):
            return None
    return event
