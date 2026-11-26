from typing import Any

from graphql import GraphQLError

# Message prefix graphql-core uses when a query requests a field that does not
# exist on the schema. These are produced during query validation, before any
# resolver runs, and are caused entirely by the client sending a bad query.
BAD_QUERY_MESSAGE_PREFIX = "Cannot query field"


def _is_ariadne_mechanism(event: dict[str, Any]) -> bool:
    values = (event.get("exception") or {}).get("values") or []
    return any(
        (value.get("mechanism") or {}).get("type") == "ariadne" for value in values
    )


def is_graphql_bad_query_event(event: dict[str, Any], hint: dict[str, Any]) -> bool:
    """
    True when the event is the Ariadne integration's automatic report of a
    GraphQL schema-validation error (e.g. "Cannot query field 'x' on type
    'Query'"). Such errors come from invalid client input, are returned to the
    client as GraphQL errors, and do not indicate a problem in our code.
    """
    exc_info = hint.get("exc_info") if hint else None
    if not exc_info or len(exc_info) < 2:
        return False
    exc = exc_info[1]
    if not isinstance(exc, GraphQLError) or exc.original_error is not None:
        return False
    if not exc.message.startswith(BAD_QUERY_MESSAGE_PREFIX):
        return False
    return _is_ariadne_mechanism(event)


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    if is_graphql_bad_query_event(event, hint):
        return None
    return event
