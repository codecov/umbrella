from typing import Any

from graphql import GraphQLError


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    """
    Drop GraphQL errors that are client query mistakes.

    The Sentry Ariadne integration (auto-enabled) captures every GraphQL error,
    including syntax/validation errors such as "Cannot query field 'x' on type
    'Query'". Those are raised by graphql-core before any resolver runs (so they
    carry no `original_error`), are returned to the caller in the response and
    are logged as warnings by `AsyncGraphqlView.error_formatter`. They do not
    indicate a server-side problem, so we don't report them as issues.

    Errors raised from resolvers are wrapped in a `GraphQLError` with
    `original_error` set and are still reported.
    """
    exc_info = (hint or {}).get("exc_info")
    if exc_info:
        exc = exc_info[1]
        if isinstance(exc, GraphQLError) and exc.original_error is None:
            return None
    return event
