from typing import Any

# Messages produced by GraphQL query parsing/validation. These are caused by
# malformed client queries and are not actionable server errors.
GRAPHQL_CLIENT_ERROR_PATTERNS = (
    "Cannot query field",
    "Unknown argument",
    "Unknown type",
    "Unknown fragment",
    "Syntax Error",
    "must have a selection of subfields",
    "must not have a selection since type",
    "is required, but it was not provided",
    "of required type",
    "got invalid value",
)


def _is_graphql_client_error(exc: BaseException | None, message: str | None) -> bool:
    if exc is not None:
        try:
            from graphql import GraphQLError
        except ImportError:  # pragma: no cover
            GraphQLError = None  # type: ignore

        if GraphQLError is not None and isinstance(exc, GraphQLError):
            # Validation/parse errors never wrap an underlying exception;
            # resolver errors do, and those must still be reported.
            if exc.original_error is not None:
                return False
            return True

    if message:
        return any(pattern in message for pattern in GRAPHQL_CLIENT_ERROR_PATTERNS)
    return False


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    """
    Drop events captured by the (auto-enabled) Ariadne integration that are
    really GraphQL validation errors caused by malformed client queries.
    """
    exception_values = (event.get("exception") or {}).get("values") or []
    if not exception_values:
        return event

    is_ariadne = any(
        ((value.get("mechanism") or {}).get("type") == "ariadne")
        for value in exception_values
    )
    if not is_ariadne:
        return event

    exc = None
    exc_info = hint.get("exc_info") if hint else None
    if exc_info and len(exc_info) > 1:
        exc = exc_info[1]

    last_value = exception_values[-1]
    if last_value.get("type") != "GraphQLError" and exc is None:
        return event

    if _is_graphql_client_error(exc, last_value.get("value")):
        return None

    return event
