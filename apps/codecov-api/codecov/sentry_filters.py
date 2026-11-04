from typing import Any

from graphql import GraphQLError


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    """
    Drop GraphQL query validation errors (e.g. "Cannot query field 'x' on type
    'Query'") captured by the auto-enabled Ariadne integration.

    These errors have no `original_error`: they are produced while validating the
    client's query document, before any resolver runs, so they describe a bad
    client request rather than a server failure. They are still logged as a
    warning and returned to the client by `GraphQLView.error_formatter`.
    Errors raised from resolvers keep their `original_error` and are still sent.
    """
    exc_info = hint.get("exc_info") if hint else None
    if exc_info:
        exc = exc_info[1]
        if isinstance(exc, GraphQLError) and exc.original_error is None:
            return None
    return event
