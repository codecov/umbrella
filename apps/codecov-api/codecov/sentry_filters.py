from typing import Any

from graphql import GraphQLError


def is_client_query_error(error: Any) -> bool:
    """
    A GraphQLError without an ``original_error`` comes from parsing or schema
    validation (e.g. "Cannot query field 'x' on type 'Y'"). These are raised
    before any resolver runs, so they are client mistakes, not server failures.
    """
    return isinstance(error, GraphQLError) and error.original_error is None


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    """
    Drop events that the auto-enabled Ariadne integration captures for
    invalid client queries (parse/validation errors). Nothing on the server
    ran for those, so they are harmless; they are still logged as warnings
    by the GraphQL view. All other events are sent unchanged.
    """
    exc_info = hint.get("exc_info") if hint else None
    if not exc_info:
        return event
    exc = exc_info[1]
    if not is_client_query_error(exc):
        return event
    for value in (event.get("exception") or {}).get("values") or []:
        if (value.get("mechanism") or {}).get("type") == "ariadne":
            return None
    return event
