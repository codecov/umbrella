"""``before_send`` filter for the Sentry SDK.

The Ariadne integration (auto-enabled by sentry_sdk) captures every
``GraphQLError`` raised by a resolver, including the ones that wrap our own
expected command exceptions (e.g. ``ValidationError`` for a malformed
pagination cursor). Those are client errors that ``GraphQLView.error_formatter``
already turns into user-facing messages and intentionally does not report, so
we drop them here to keep Sentry free of noise (e.g. SQL-injection scanners
fuzzing GraphQL variables).
"""

from typing import Any


def _iter_exception_chain(exc: BaseException | None):
    # Follow only GraphQLError.original_error wrapping; do not walk
    # __cause__/__context__ so genuine bugs raised while handling an expected
    # error are still reported.
    seen: set[int] = set()
    while isinstance(exc, BaseException) and id(exc) not in seen:
        seen.add(id(exc))
        yield exc
        exc = getattr(exc, "original_error", None)


def _is_expected_error(exc: BaseException) -> bool:
    # Imported lazily: this module is loaded from settings, before apps are ready.
    from rest_framework.exceptions import APIException

    from codecov.commands.exceptions import BaseException as CommandException
    from services import ServiceException

    return isinstance(exc, CommandException | ServiceException | APIException)


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    exc_info = hint.get("exc_info") if hint else None
    if not exc_info:
        return event

    exc = exc_info[1]
    try:
        for chained in _iter_exception_chain(exc):
            if _is_expected_error(chained):
                return None
    except Exception:
        # Never let filtering break error reporting.
        return event

    return event
