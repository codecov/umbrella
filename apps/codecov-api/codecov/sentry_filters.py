from typing import Any

from graphql import GraphQLError


def is_graphql_client_error(exc: BaseException | None) -> bool:
    """
    A GraphQLError without an `original_error` was raised by GraphQL itself
    (syntax/validation errors such as unknown types or fields), i.e. it is
    caused by a malformed client query rather than a server-side failure.
    """
    return isinstance(exc, GraphQLError) and exc.original_error is None


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    exc_info = hint.get("exc_info") if hint else None
    if exc_info and is_graphql_client_error(exc_info[1]):
        return None
    return event
