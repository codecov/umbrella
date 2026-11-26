from typing import Any

from graphql import GraphQLError


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    """
    Drop GraphQL validation/syntax errors reported by the (auto-enabled) Ariadne
    integration. These errors have no ``original_error``: they are raised while
    validating the client's query document, before any resolver runs, so they
    reflect bad client input rather than a server failure. They are still logged
    as warnings by the GraphQL view's ``error_formatter``. Errors wrapping a real
    exception raised in a resolver are kept.
    """
    exc_info = hint.get("exc_info") if hint else None
    if exc_info:
        exc = exc_info[1]
        if isinstance(exc, GraphQLError) and exc.original_error is None:
            return None
    return event
