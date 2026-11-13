from typing import Any

from graphql import GraphQLError


def _is_graphql_client_validation_error(
    event: dict[str, Any], hint: dict[str, Any]
) -> bool:
    """
    Returns True for errors reported by Sentry's (auto-enabled) Ariadne
    integration when a client sends a GraphQL query that doesn't match our
    schema (e.g. "Cannot query field 'x' on type 'Query'").

    These errors are raised during GraphQL validation, before any resolver
    runs, so they carry no `original_error`. They come from bad client input
    and don't point to a fault in our code. Errors raised by resolvers always
    have `original_error` set and are still reported.
    """
    exc_info = hint.get("exc_info")
    if not exc_info:
        return False
    exc = exc_info[1]
    if not isinstance(exc, GraphQLError) or exc.original_error is not None:
        return False

    exceptions = (event.get("exception") or {}).get("values") or []
    return any(
        (value.get("mechanism") or {}).get("type") == "ariadne" for value in exceptions
    )


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    if _is_graphql_client_validation_error(event, hint):
        return None
    return event
