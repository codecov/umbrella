from typing import Any

from graphql import GraphQLError

# Messages produced by GraphQL schema validation when a client queries a field
# that does not exist in our schema.
CLIENT_VALIDATION_ERROR_MARKERS = ("Cannot query field",)


def is_client_graphql_validation_error(exc: object) -> bool:
    """
    True if `exc` is a GraphQL validation error caused by a malformed client
    query (e.g. querying a field that doesn't exist). These never reach a
    resolver, the client already receives an error response, and they are
    logged as warnings by `AsyncGraphqlView.error_formatter`, so they are not
    actionable server errors.
    """
    if not isinstance(exc, GraphQLError):
        return False
    if exc.original_error is not None:
        return False
    return any(marker in exc.message for marker in CLIENT_VALIDATION_ERROR_MARKERS)


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    """
    Drop GraphQL client validation errors that the auto-enabled Sentry Ariadne
    integration reports as unhandled exceptions. Everything else is sent.
    """
    exc_info = hint.get("exc_info") if hint else None
    if exc_info and is_client_graphql_validation_error(exc_info[1]):
        return None
    return event
