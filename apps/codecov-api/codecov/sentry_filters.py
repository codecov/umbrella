"""``before_send`` hook for the Sentry SDK.

The Ariadne integration (auto-enabled by sentry-sdk) reports every GraphQL error
as an unhandled exception, including pure validation errors such as a client
querying a field that does not exist in the schema. Those errors are raised by
graphql-core during validation, before any resolver executes, so they carry no
``original_error`` and represent malformed client requests rather than server
faults. They are harmless to the server and are dropped here.

GraphQL errors that wrap a real exception raised by a resolver
(``original_error`` is set) are still reported.
"""

from __future__ import annotations

from typing import Any

from graphql import GraphQLError


def _is_ariadne_event(event: dict[str, Any]) -> bool:
    values = (event.get("exception") or {}).get("values") or []
    return any(
        ((value or {}).get("mechanism") or {}).get("type") == "ariadne"
        for value in values
    )


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    exc_info = (hint or {}).get("exc_info")
    if not exc_info or len(exc_info) < 2:
        return event
    exc = exc_info[1]
    if (
        isinstance(exc, GraphQLError)
        and exc.original_error is None
        and _is_ariadne_event(event)
    ):
        return None
    return event
