from typing import Any

from graphql import GraphQLError


def is_graphql_schema_validation_error(exc: object) -> bool:
    """
    True for GraphQL errors raised while validating a client-supplied query
    against our schema (e.g. querying a field that does not exist). These are
    rejected before any resolver runs, so they are client errors, not server
    faults.
    """
    return (
        isinstance(exc, GraphQLError)
        and exc.original_error is None
        and exc.message.startswith("Cannot query field")
    )


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    exc_info = hint.get("exc_info")
    if exc_info and is_graphql_schema_validation_error(exc_info[1]):
        # Harmless client error: the query never executed. It is still logged as
        # a warning by the GraphQL view's error formatter.
        return None
    return event
