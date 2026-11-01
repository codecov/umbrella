from typing import Any

from graphql import GraphQLError

UNKNOWN_FIELD_MESSAGE = "Cannot query field"


def is_unknown_field_error(error: Any) -> bool:
    """
    True for GraphQL validation errors raised because the client queried a field
    that does not exist in our schema. These are client mistakes (outdated or
    hand-written queries), never server faults: they carry no original_error.
    """
    return (
        isinstance(error, GraphQLError)
        and error.original_error is None
        and UNKNOWN_FIELD_MESSAGE in (error.message or "")
    )


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    # The auto-enabled Ariadne integration captures every GraphQL error, including
    # schema validation errors caused by invalid client queries. Those are
    # harmless client input errors (logged as warnings in graphql_api.views),
    # so drop only those; all other events are sent unchanged.
    exc_info = hint.get("exc_info") if hint else None
    if exc_info and is_unknown_field_error(exc_info[1]):
        return None
    return event
