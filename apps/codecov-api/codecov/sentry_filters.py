from typing import Any

from graphql import GraphQLError


def is_client_document_error(error: Any) -> bool:
    """
    Whether a GraphQLError was raised while parsing/validating the client's
    query document (e.g. "Cannot query field 'x' on type 'Query'"), rather
    than by a resolver during execution.

    Parse/validation errors have no `original_error` and no `path`;
    execution errors always carry the path of the failing field.
    """
    return (
        isinstance(error, GraphQLError)
        and error.original_error is None
        and not error.path
    )


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    """
    Drop events auto-captured by the Ariadne integration for invalid client
    query documents. These are client mistakes (unknown fields, syntax errors),
    not server failures, and are already logged at warning level by the
    GraphQL view. Resolver exceptions carry `original_error`/`path` and are
    still reported.
    """
    exc_info = hint.get("exc_info")
    if not exc_info or len(exc_info) < 2:
        return event

    mechanisms = {
        (value.get("mechanism") or {}).get("type")
        for value in (event.get("exception") or {}).get("values") or []
    }
    if "ariadne" in mechanisms and is_client_document_error(exc_info[1]):
        return None
    return event
