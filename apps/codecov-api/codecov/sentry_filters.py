from typing import Any

from graphql import GraphQLError


def _is_ariadne_event(event: dict[str, Any]) -> bool:
    values = (event.get("exception") or {}).get("values") or []
    return any(
        (value.get("mechanism") or {}).get("type") == "ariadne" for value in values
    )


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    """
    Drop GraphQL validation errors (e.g. "Cannot query field 'x' on type 'Y'")
    reported by the Ariadne integration. These are caused by malformed client
    queries, not server bugs. Errors raised inside resolvers carry an
    `original_error` and are still reported.
    """
    exc_info = hint.get("exc_info") if hint else None
    if not exc_info:
        return event

    exc = exc_info[1]
    if (
        isinstance(exc, GraphQLError)
        and getattr(exc, "original_error", None) is None
        and _is_ariadne_event(event)
    ):
        return None

    return event
