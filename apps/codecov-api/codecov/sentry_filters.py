from typing import Any

from graphql import GraphQLError

BAD_QUERY_MESSAGE = "Cannot query field"


def is_graphql_bad_query_error(exc: BaseException | None) -> bool:
    """
    A GraphQL validation error for a field that doesn't exist on the schema.
    These are caused by the client's query (typos, stale clients, scanners)
    and carry no underlying server exception, so they're not actionable.
    """
    return (
        isinstance(exc, GraphQLError)
        and exc.original_error is None
        and BAD_QUERY_MESSAGE in exc.message
    )


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    # The auto-enabled Ariadne integration reports every GraphQL validation
    # error. Drop only the "unknown field" client errors; they're logged as
    # warnings by the GraphQL view instead. All other errors are kept.
    exc_info = hint.get("exc_info") if hint else None
    if exc_info and is_graphql_bad_query_error(exc_info[1]):
        return None
    return event
