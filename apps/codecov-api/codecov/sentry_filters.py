from typing import Any

from graphql import GraphQLError


def _is_graphql_client_validation_error(
    event: dict[str, Any], hint: dict[str, Any]
) -> bool:
    """
    Sentry's auto-enabled Ariadne integration captures every GraphQL error,
    including validation/syntax errors caused by malformed client queries
    (e.g. "Cannot query field 'team' on type 'Query'"). Those errors are raised
    before any resolver runs and carry no ``original_error``: they describe bad
    client input, not a server failure, so they are dropped.

    Errors raised from within resolvers wrap the underlying exception in
    ``original_error`` and are still reported.
    """
    exc_info = hint.get("exc_info")
    if not exc_info:
        return False
    exc = exc_info[1]
    if not isinstance(exc, GraphQLError) or exc.original_error is not None:
        return False

    for exception in (event.get("exception") or {}).get("values") or []:
        mechanism = exception.get("mechanism") or {}
        if mechanism.get("type") == "ariadne":
            return True
    return False


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    if _is_graphql_client_validation_error(event, hint):
        return None
    return event
