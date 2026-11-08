from typing import Any

from graphql import GraphQLError


def is_graphql_client_error(exc: BaseException | None) -> bool:
    """
    A GraphQLError without an `original_error` was raised by GraphQL itself
    while parsing/validating the incoming query (e.g. "Cannot query field ..."),
    not by one of our resolvers. These are client mistakes that are already
    answered with an error response, so they are not actionable server errors.
    Resolver failures always carry their `original_error` and are kept.
    """
    return isinstance(exc, GraphQLError) and exc.original_error is None


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    exc_info = hint.get("exc_info") if hint else None
    if exc_info and len(exc_info) > 1 and is_graphql_client_error(exc_info[1]):
        return None
    return event
