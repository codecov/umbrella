from typing import Any

from graphql import GraphQLError


def is_graphql_client_error(exc: BaseException | None) -> bool:
    """
    A GraphQLError without an `original_error` was produced by GraphQL itself
    (syntax / schema validation, e.g. querying an unknown field or input field),
    not by an exception raised in our resolvers. These are client mistakes.
    """
    return isinstance(exc, GraphQLError) and exc.original_error is None


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    exc_info = hint.get("exc_info") if hint else None
    if exc_info and is_graphql_client_error(exc_info[1]):
        return None
    return event
