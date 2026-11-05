from typing import Any

from graphql import GraphQLError

# Substrings of graphql-core validation messages produced by malformed client
# queries (e.g. requesting a field that doesn't exist on a type). These are
# client errors that fail before any resolver runs, so they are harmless.
CLIENT_GRAPHQL_VALIDATION_MESSAGES = ("Cannot query field",)


def is_client_graphql_validation_error(
    event: dict[str, Any], hint: dict[str, Any]
) -> bool:
    exc_info = hint.get("exc_info") if hint else None
    if not exc_info or len(exc_info) < 2:
        return False
    exc = exc_info[1]
    if not isinstance(exc, GraphQLError) or exc.original_error is not None:
        return False
    mechanisms = [
        (value.get("mechanism") or {}).get("type")
        for value in (event.get("exception") or {}).get("values") or []
    ]
    if "ariadne" not in mechanisms:
        return False
    return any(msg in exc.message for msg in CLIENT_GRAPHQL_VALIDATION_MESSAGES)


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    """Drop Ariadne-captured GraphQL validation errors caused by malformed client
    queries. They are still logged as warnings by the GraphQL view."""
    if is_client_graphql_validation_error(event, hint):
        return None
    return event
