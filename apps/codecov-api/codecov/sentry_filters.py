from typing import Any

# GraphQL validation errors caused by malformed client queries (e.g. querying
# a field that doesn't exist on a type). Sentry's Ariadne integration reports
# every GraphQL error, but these are client input errors, not server faults.
GRAPHQL_CLIENT_VALIDATION_MESSAGES = ("Cannot query field",)


def is_graphql_client_validation_error(event: dict[str, Any]) -> bool:
    exceptions = (event.get("exception") or {}).get("values") or []
    if not exceptions:
        return False
    for exc in exceptions:
        mechanism = exc.get("mechanism") or {}
        if mechanism.get("type") != "ariadne":
            return False
        if exc.get("type") != "GraphQLError":
            return False
        value = exc.get("value") or ""
        if not any(msg in value for msg in GRAPHQL_CLIENT_VALIDATION_MESSAGES):
            return False
    return True


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    if is_graphql_client_validation_error(event):
        return None
    return event
