"""``before_send`` filter for the Sentry SDK.

The Sentry SDK auto-enables ``AriadneIntegration``, which reports every
``GraphQLError`` that Ariadne returns. That includes errors raised while
parsing or validating a client's query, before any resolver runs. Examples are
``Cannot query field 'x' on type 'Query'``, syntax errors, and depth, alias,
cost or required-variable violations.

Those errors come from the client sending a query that doesn't match the
schema. Nothing runs on the server, so they aren't server bugs. Anonymous
scanners send them all the time (see API-FF6), so they would flood Sentry with
unhandled errors that nobody can act on. They are still recorded as warning
logs by ``graphql_api.views.AsyncGraphqlView.error_formatter``.

Errors raised while executing a query (they carry a ``path`` or an
``original_error``) are not affected and are still reported.
"""

from __future__ import annotations

from typing import Any

from graphql import GraphQLError


def _is_ariadne_event(event: dict[str, Any]) -> bool:
    values = (event.get("exception") or {}).get("values") or []
    return any(
        (value.get("mechanism") or {}).get("type") == "ariadne" for value in values
    )


def is_graphql_client_query_error(error: Any) -> bool:
    """True for GraphQL errors raised while parsing or validating the request."""
    return (
        isinstance(error, GraphQLError)
        and error.original_error is None
        and not error.path
    )


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    exc_info = hint.get("exc_info") if hint else None
    if exc_info and len(exc_info) > 1:
        error = exc_info[1]
        if is_graphql_client_query_error(error) and _is_ariadne_event(event):
            return None
    return event
