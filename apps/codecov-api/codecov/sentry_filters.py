from typing import Any

from graphql import GraphQLError


def is_graphql_client_error(error: Any) -> bool:
    """
    True for GraphQL errors caused by a malformed client request (syntax or
    schema validation errors, e.g. querying a field that does not exist).

    These errors are produced before execution starts: they never wrap an
    underlying exception (`original_error`) and are not tied to a response
    path. Errors raised while resolving fields always carry a `path`, and
    unexpected exceptions carry an `original_error`, so neither is matched.
    """
    return (
        isinstance(error, GraphQLError)
        and error.original_error is None
        and not error.path
    )


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    """
    Drop events the Ariadne integration (auto-enabled by sentry_sdk) emits
    for GraphQL client errors. A client asking for a nonexistent field is a
    bad request, not a server failure, so it is harmless to drop. Every other
    event, including resolver exceptions captured by Ariadne, is kept.
    """
    exc_info = hint.get("exc_info")
    if not exc_info:
        return event

    mechanism_types = {
        (value.get("mechanism") or {}).get("type")
        for value in (event.get("exception") or {}).get("values") or []
    }
    if "ariadne" in mechanism_types and is_graphql_client_error(exc_info[1]):
        return None
    return event
