from typing import Any

from graphql import GraphQLError


def _is_graphql_client_query_error(exc: BaseException | None) -> bool:
    """
    GraphQL parse/validation errors (unknown field, syntax error, depth/cost limits)
    are raised by graphql-core itself and have no ``original_error``. They are caused
    by the query a client sent, the client already receives an error response, and
    we track them via logs and the ``api_gql_errors_type`` "validation" counter.
    """
    return isinstance(exc, GraphQLError) and exc.original_error is None


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    """
    Drop events auto-captured by the Ariadne integration for client query
    validation errors. Errors raised from resolvers (which carry an
    ``original_error``) and every other event are kept.
    """
    exc_info = hint.get("exc_info") if hint else None
    if exc_info and _is_graphql_client_query_error(exc_info[1]):
        return None
    return event
