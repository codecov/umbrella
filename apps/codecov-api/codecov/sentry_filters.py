"""``before_send`` hook for the Sentry SDK.

sentry-sdk auto-enables its Ariadne integration because ``ariadne`` is
installed. That integration reports *every* ``GraphQLError`` in a response as
an unhandled exception, including query validation errors such as
``Cannot query field 'x' on type 'Query'``. Those happen when a client sends a
query that doesn't match our schema: a client mistake, not a server failure.
The API answers with an error and nothing breaks server-side, so they don't
belong in Sentry.

Only validation errors are dropped (no ``original_error`` and a
"Cannot query field" message). Errors raised from resolvers carry an
``original_error`` and are still reported.
"""

from __future__ import annotations

from typing import Any

from graphql import GraphQLError

BAD_QUERY_MESSAGE = "Cannot query field"


def _is_graphql_bad_query(exc: BaseException | None) -> bool:
    return (
        isinstance(exc, GraphQLError)
        and exc.original_error is None
        and BAD_QUERY_MESSAGE in exc.message
    )


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    exc_info = hint.get("exc_info") if hint else None
    if exc_info and _is_graphql_bad_query(exc_info[1]):
        return None
    return event
