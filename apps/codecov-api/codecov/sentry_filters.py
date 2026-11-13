from typing import Any

from graphql import GraphQLError

UNKNOWN_FIELD_PREFIX = "Cannot query field"


def is_client_graphql_validation_error(exc: BaseException | None) -> bool:
    """
    True for GraphQL schema-validation errors caused by a client querying a
    field that does not exist. These never reach a resolver and are harmless
    client mistakes (or probes); they are logged as warnings by the GraphQL
    view instead of being reported as server errors.
    """
    return (
        isinstance(exc, GraphQLError)
        and exc.original_error is None
        and exc.message.startswith(UNKNOWN_FIELD_PREFIX)
    )


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    exc_info = hint.get("exc_info") if hint else None
    if exc_info and is_client_graphql_validation_error(exc_info[1]):
        return None
    return event
