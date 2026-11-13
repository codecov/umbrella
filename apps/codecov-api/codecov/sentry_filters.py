from typing import Any

from graphql import GraphQLError


def is_graphql_client_validation_error(exc: BaseException | None) -> bool:
    """
    True for GraphQL errors raised while validating a client's query against the
    schema (e.g. "Cannot query field 'x' on type 'Query'."). These are client
    mistakes (bad clients / scanners), not server errors: no resolver ran, so
    there is no `original_error`. Errors raised from resolvers always wrap an
    `original_error` and are not matched here.
    """
    return (
        isinstance(exc, GraphQLError)
        and exc.original_error is None
        and exc.message.startswith("Cannot query field")
    )


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    exc_info = hint.get("exc_info") if hint else None
    if exc_info and is_graphql_client_validation_error(exc_info[1]):
        # Harmless client-side query validation error; it is still logged as a
        # warning by the GraphQL view (graphql_api.views.error_formatter).
        return None
    return event
