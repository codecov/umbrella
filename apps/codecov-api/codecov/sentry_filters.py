from typing import Any

UNKNOWN_FIELD_MESSAGE = "Cannot query field"


def _is_unknown_field_validation_error(event: dict[str, Any]) -> bool:
    """
    True for events auto-captured by Sentry's Ariadne integration when a client
    queries a field that does not exist in the schema.

    These are harmless client errors: graphql-core rejects the document during
    validation, before any resolver executes, so no server code fails. They are
    still logged as warnings (with the query) by AsyncGraphqlView.error_formatter.
    """
    values = (event.get("exception") or {}).get("values") or []
    if not values:
        return False
    for exc in values:
        mechanism = exc.get("mechanism") or {}
        if mechanism.get("type") != "ariadne":
            return False
        if exc.get("type") != "GraphQLError":
            return False
        if UNKNOWN_FIELD_MESSAGE not in (exc.get("value") or ""):
            return False
    return True


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    if _is_unknown_field_validation_error(event):
        return None
    return event
