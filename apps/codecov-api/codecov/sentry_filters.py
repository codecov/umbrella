from typing import Any

from graphql import GraphQLError


def is_graphql_client_query_error(exc: Any) -> bool:
    """
    A GraphQL syntax/validation error produced from the client's query document
    (e.g. "Cannot query field 'person' on type 'Query'."). These have no
    underlying exception and no execution path, meaning no server code failed.
    """
    return (
        isinstance(exc, GraphQLError)
        and exc.original_error is None
        and exc.path is None
    )


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    """
    Drop events auto-captured by Sentry's Ariadne integration for client-side
    GraphQL query validation errors. These are harmless (typos or scanners
    probing the schema); the API already answers them with an error response,
    and they are still logged at info level by the GraphQL view. Real resolver
    exceptions keep an `original_error` and are still reported.
    """
    exc_info = hint.get("exc_info")
    if not exc_info:
        return event
    exc = exc_info[1]
    if not is_graphql_client_query_error(exc):
        return event
    mechanisms = [
        (value.get("mechanism") or {}).get("type")
        for value in (event.get("exception") or {}).get("values") or []
    ]
    if "ariadne" in mechanisms:
        return None
    return event
