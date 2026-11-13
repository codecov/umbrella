from typing import Any

# Messages produced by GraphQL schema validation when a client sends a query that
# references fields that don't exist. These are rejected before execution and are
# caused purely by client input, so they are harmless from the server's perspective.
GRAPHQL_VALIDATION_MESSAGE_PREFIXES = ("Cannot query field",)


def drop_graphql_validation_errors(
    event: dict[str, Any], hint: dict[str, Any]
) -> dict[str, Any] | None:
    """
    Drop GraphQLError events auto-captured by the Ariadne integration when they are
    pure schema-validation failures caused by the client's query. All other events
    (including GraphQL errors raised during resolver execution) are kept.
    """
    exceptions = (event.get("exception") or {}).get("values") or []
    for exc in exceptions:
        mechanism = exc.get("mechanism") or {}
        if (
            exc.get("type") == "GraphQLError"
            and mechanism.get("type") == "ariadne"
            and isinstance(exc.get("value"), str)
            and exc["value"].startswith(GRAPHQL_VALIDATION_MESSAGE_PREFIXES)
        ):
            return None
    return event
