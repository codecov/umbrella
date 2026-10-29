from typing import Any

from graphql import GraphQLError


def is_query_validation_error(error: Any) -> bool:
    """
    True for GraphQL parse/validation errors (e.g. "Cannot query field 'x'
    on type 'Query'"). These are produced before execution: they wrap no
    original exception and have no execution path. Errors raised from
    resolvers always have a path, so they are never matched here.
    """
    return (
        isinstance(error, GraphQLError)
        and error.original_error is None
        and not error.path
    )


def before_send(event: dict, hint: dict) -> dict | None:
    """
    Drop events auto-captured by sentry_sdk's Ariadne integration for GraphQL
    query validation errors. These come from malformed client queries that
    are rejected before any resolver runs (harmless; the client receives a
    validation error response). Resolver/internal errors are kept.
    """
    exc_info = hint.get("exc_info") if hint else None
    if not exc_info or len(exc_info) < 2:
        return event
    exc = exc_info[1]
    if not is_query_validation_error(exc):
        return event
    values = (event.get("exception") or {}).get("values") or []
    mechanisms = {
        (v.get("mechanism") or {}).get("type") for v in values if isinstance(v, dict)
    }
    if "ariadne" in mechanisms:
        return None
    return event
