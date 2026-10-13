from typing import Any

from graphql import GraphQLError


def is_graphql_client_error(exc: BaseException | None) -> bool:
    """
    A GraphQLError without an `original_error` and without a `path` was raised by
    graphql-core while parsing/validating the incoming query (e.g. "Cannot query
    field ..."), not during resolver execution. These are client mistakes, not
    server bugs.
    """
    return (
        isinstance(exc, GraphQLError)
        and exc.original_error is None
        and exc.path is None
    )


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    exc_info = hint.get("exc_info") if hint else None
    if exc_info and is_graphql_client_error(exc_info[1]):
        return None
    return event
