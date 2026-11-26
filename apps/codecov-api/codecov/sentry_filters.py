from typing import Any


def sentry_before_send(
    event: dict[str, Any], hint: dict[str, Any]
) -> dict[str, Any] | None:
    """
    Drop GraphQL schema validation errors (e.g. "Cannot query field 'x' on type
    'Query'") that the auto-enabled Sentry Ariadne integration captures for every
    invalid client query. These are caused by clients sending queries that don't
    match the schema; no server code runs, so they are harmless. They are still
    logged as warnings by `AsyncGraphqlView.error_formatter`.
    """
    mechanism_types = {
        (exc.get("mechanism") or {}).get("type")
        for exc in (event.get("exception") or {}).get("values") or []
    }
    if "ariadne" in mechanism_types:
        exc_info = hint.get("exc_info") if hint else None
        error = exc_info[1] if exc_info else None
        if (
            error is not None
            and type(error).__name__ == "GraphQLError"
            and getattr(error, "original_error", None) is None
            and "Cannot query field" in str(getattr(error, "message", ""))
        ):
            return None
    return event
