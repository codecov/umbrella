from typing import Any

from graphql import GraphQLError


def _is_ariadne_event(event: dict[str, Any]) -> bool:
    values = (event.get("exception") or {}).get("values") or []
    return any(
        ((value or {}).get("mechanism") or {}).get("type") == "ariadne"
        for value in values
    )


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    """
    Drop GraphQL request errors auto-captured by Sentry's Ariadne integration
    that are caused purely by malformed client input.

    graphql-core raises a ``GraphQLError`` whose ``original_error`` is either
    ``None`` or another ``GraphQLError``
    when a request fails parsing, validation or variable coercion, e.g. an
    unknown field on an input type. Those requests are rejected before any
    resolver runs, so they are harmless to the server; they are still recorded
    via a warning log and the ``api_gql_errors`` metric in
    ``graphql_api.views.AsyncGraphqlView.error_formatter``.

    Errors wrapping a real exception raised by our code are kept.
    """
    exc_info = hint.get("exc_info") if hint else None
    if not exc_info:
        return event
    exc = exc_info[1]
    if (
        isinstance(exc, GraphQLError)
        and (exc.original_error is None or isinstance(exc.original_error, GraphQLError))
        and _is_ariadne_event(event)
    ):
        return None
    return event
