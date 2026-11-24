from typing import Any

from graphql import GraphQLError


def drop_graphql_validation_errors(
    event: dict[str, Any], hint: dict[str, Any]
) -> dict[str, Any] | None:
    """
    Sentry's auto-enabled Ariadne integration reports every GraphQLError in a
    response, including query validation errors (e.g. "Cannot query field 'x'
    on type 'Owner'"). Those are caused by malformed client queries, not by
    server failures, so drop them. Errors raised while executing resolvers carry
    an `original_error` and/or a `path` and are still reported.
    """
    exc_info = hint.get("exc_info") if hint else None
    if not exc_info:
        return event

    exc_value = exc_info[1]
    if not isinstance(exc_value, GraphQLError):
        return event

    if exc_value.original_error is None and exc_value.path is None:
        return None

    return event
