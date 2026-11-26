from typing import Any

from graphql import GraphQLError


def is_graphql_schema_validation_error(exc: BaseException | None) -> bool:
    """
    A GraphQLError without an `original_error` that complains about an unknown
    field means the client sent a query that does not match our schema. The
    request is rejected during validation, before any resolver runs, so it is a
    harmless client mistake (typos, stale clients, scanners) and not a bug.
    """
    return (
        isinstance(exc, GraphQLError)
        and exc.original_error is None
        and "Cannot query field" in exc.message
    )


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    """
    Drop Sentry events auto-captured by the Ariadne integration for GraphQL
    schema validation errors. These are client errors that are already logged
    as warnings by `AsyncGraphqlView.error_formatter`. All other events,
    including GraphQL errors raised by resolvers, are sent unchanged.
    """
    exc_info = hint.get("exc_info") if hint else None
    if exc_info and is_graphql_schema_validation_error(exc_info[1]):
        return None
    return event
