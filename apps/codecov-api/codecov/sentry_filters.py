from typing import Any

from graphql import GraphQLError


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    """
    Drop GraphQL errors that originate from the client's query itself
    (syntax / schema validation errors). These are GraphQLErrors without an
    `original_error`, meaning no resolver code raised them - they are caused by
    malformed requests (e.g. scanners or bad clients) and aren't actionable.
    """
    exc_info = hint.get("exc_info") if hint else None
    if exc_info:
        exc = exc_info[1]
        if isinstance(exc, GraphQLError) and exc.original_error is None:
            return None
    return event
