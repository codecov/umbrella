"""``before_send`` hook for the Sentry SDK.

The Sentry Ariadne integration (auto-enabled because ``ariadne`` is installed)
captures *every* ``GraphQLError`` in a GraphQL response as an unhandled error,
including schema-validation errors caused by malformed client queries such as
``{ certificates { id } }`` (a field that doesn't exist on ``Query``).

Those validation errors are client mistakes, frequently from anonymous scanners
probing the API. They never reach a resolver, so no server code fails and they
aren't actionable. They are still logged by ``AsyncGraphqlView.error_formatter``.
Resolver errors and every other GraphQL error keep being reported.
"""

from __future__ import annotations

from typing import Any

from graphql import GraphQLError

INVALID_FIELD_MESSAGE = "Cannot query field"


def is_graphql_invalid_field_error(exc: BaseException | None) -> bool:
    """True if ``exc`` is a schema-validation error for an unknown field."""
    return (
        isinstance(exc, GraphQLError)
        and exc.original_error is None
        and exc.message.startswith(INVALID_FIELD_MESSAGE)
    )


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    exc_info = hint.get("exc_info") if hint else None
    if exc_info and is_graphql_invalid_field_error(exc_info[1]):
        return None
    return event
