from typing import Any

from graphql import GraphQLError


def _is_graphql_bad_query(exc: BaseException | None) -> bool:
    """
    True for GraphQL schema validation errors caused by a client querying a
    field that does not exist (e.g. "Cannot query field 'x' on type 'Y'").

    These are raised by graphql-core during query validation, before any
    resolver runs, so they carry no ``original_error``. They are client
    mistakes (bad/stale queries, probing) and not server failures. The
    sentry_sdk Ariadne integration (auto-enabled) reports every GraphQL error
    as an unhandled exception, which turns these into noisy issues.
    """
    return (
        isinstance(exc, GraphQLError)
        and exc.original_error is None
        and "Cannot query field" in (exc.message or "")
    )


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    exc_info = hint.get("exc_info") if hint else None
    if exc_info and _is_graphql_bad_query(exc_info[1]):
        return None
    return event
