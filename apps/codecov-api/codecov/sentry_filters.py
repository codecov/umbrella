"""Sentry event filters for codecov-api."""

from __future__ import annotations

from typing import Any

from graphql import GraphQLError


def is_query_validation_error(error: Any) -> bool:
    """Return True for GraphQL errors produced by query validation.

    Validation errors (unknown fields, bad syntax, etc.) are raised by
    graphql-core before execution starts: they wrap no original exception and
    have no execution ``path``. Errors raised from resolvers always carry a
    path, so they are never matched here.
    """
    return (
        isinstance(error, GraphQLError)
        and error.original_error is None
        and not error.path
    )


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    """Drop GraphQL query validation errors captured by the Ariadne integration.

    These come from clients sending queries that don't match the schema (for
    example ``config { isSelfHosted }``). They are rejected during validation
    before any of our code runs, so they are harmless client errors rather
    than server faults. They are still logged at warning level by
    the GraphQL view's ``error_formatter`` in ``graphql_api.views``. Resolver errors
    and every other exception are reported as before.
    """
    exc_info = hint.get("exc_info") if hint else None
    if exc_info and is_query_validation_error(exc_info[1]):
        return None
    return event
