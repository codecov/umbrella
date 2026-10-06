from typing import Any

from graphql import GraphQLError


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    """
    Drop GraphQL query errors caused by malformed client queries (syntax or
    validation errors such as missing subfield selections). These errors are
    not wrapping an underlying exception (`original_error is None`) and are
    client mistakes, not server bugs.
    """
    exc_info = hint.get("exc_info") if hint else None
    if exc_info:
        exc = exc_info[1]
        if isinstance(exc, GraphQLError) and exc.original_error is None:
            return None
    return event
