from typing import Any

from graphql import GraphQLError


def is_graphql_client_error(exc: BaseException | None) -> bool:
    """
    A GraphQLError without an original_error is raised by graphql-core itself
    while parsing/validating the request (syntax errors, unknown fields,
    invalid variable values, ...). These are caused by bad client input, not
    by a bug in our resolvers, so they should not be reported to Sentry.
    """
    return isinstance(exc, GraphQLError) and exc.original_error is None


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    exc_info = hint.get("exc_info") if hint else None
    if exc_info and is_graphql_client_error(exc_info[1]):
        return None
    return event
