from typing import Any

from graphql import GraphQLError

BAD_QUERY_MESSAGE = "Cannot query field"


def is_graphql_validation_error(exc: BaseException | None) -> bool:
    """
    A GraphQL schema validation error (e.g. querying a field that doesn't exist)
    is caused by client input, not by a server fault. graphql-core raises these
    as a bare GraphQLError with no `original_error`.
    """
    return (
        isinstance(exc, GraphQLError)
        and exc.original_error is None
        and BAD_QUERY_MESSAGE in (exc.message or "")
    )


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    """
    Drop events for harmless client-side GraphQL validation errors, which the
    Sentry Ariadne integration otherwise reports for every bad query.
    """
    exc_info = hint.get("exc_info") if hint else None
    if exc_info and len(exc_info) > 1 and is_graphql_validation_error(exc_info[1]):
        return None
    return event
