from typing import Any

from graphql import GraphQLError


def _is_schema_mismatch_error(exc: BaseException) -> bool:
    """
    Whether `exc` is a GraphQL validation error raised because the client
    queried a field that doesn't exist in our schema.

    Such errors are produced while validating the client's query document,
    before any resolver runs: they carry no `path` and wrap no underlying
    exception. They only reflect bad client input (e.g. outdated clients or
    schema probing) and are harmless to the server.
    """
    return (
        type(exc) is GraphQLError
        and exc.original_error is None
        and exc.path is None
        and "Cannot query field" in exc.message
    )


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    """
    Drop GraphQL schema-mismatch validation errors that the Sentry Ariadne
    integration automatically reports as unhandled exceptions. These are client
    errors (the API answers them with a "BadRequest"/validation error) and are
    already logged as warnings by `AsyncGraphqlView.error_formatter`.

    Any other event, including GraphQL errors wrapping a real exception raised
    by a resolver, is passed through unchanged.
    """
    exc_info = hint.get("exc_info") if hint else None
    if exc_info and len(exc_info) > 1 and isinstance(exc_info[1], BaseException):
        if _is_schema_mismatch_error(exc_info[1]):
            return None
    return event
