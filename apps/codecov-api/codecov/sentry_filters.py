from typing import Any

from graphql import GraphQLError

UNKNOWN_FIELD_MESSAGE = "Cannot query field"


def is_unknown_field_query_error(error: BaseException | None) -> bool:
    """
    True when `error` is a GraphQL validation error raised because the client
    queried a field that does not exist in our schema. These come from client
    input (typos, stale clients, probing) and are not server faults: they never
    wrap an underlying exception (`original_error` is None).
    """
    return (
        isinstance(error, GraphQLError)
        and error.original_error is None
        and UNKNOWN_FIELD_MESSAGE in (error.message or "")
    )


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    """
    Drop events for GraphQL queries asking for fields that don't exist in the schema.

    Sentry's Ariadne integration captures every GraphQL validation error as
    an unhandled exception. Requests for unknown fields are harmless invalid
    client input. graphql_api.views logs them as warnings instead. Errors raised
    by resolvers (which wrap an `original_error`) are still reported.
    """
    exc_info = hint.get("exc_info") if hint else None
    if exc_info and is_unknown_field_query_error(exc_info[1]):
        return None
    return event
