"""Compat helpers for Django 5 admin ChangeList → Redis querysets.

Django 4.2 applied leftover changelist lookups as::

    qs.filter(**remaining_lookup_params)

Django 5.x always builds a ``Q`` object (including an empty ``Q()`` when
nothing remains) and calls::

    qs.filter(q_object)

Redis-backed querysets historically only accepted kwargs. Translate simple
positional ``Q`` trees into kwargs so admin changelists keep working.
"""

from __future__ import annotations

from typing import Any

from django.db.models import Q


def kwargs_from_filter_args(args: tuple[Any, ...]) -> dict[str, Any]:
    """Flatten positional ``filter(*args)`` Q objects into kwargs."""
    if not args:
        return {}
    merged: dict[str, Any] = {}
    for arg in args:
        merged.update(_q_to_flat_kwargs(arg))
    return merged


def _q_to_flat_kwargs(q: Any) -> dict[str, Any]:
    if not isinstance(q, Q):
        raise NotImplementedError(
            "Redis admin filter() only accepts Q objects as positional "
            f"args, got {type(q)!r}"
        )
    if q.negated:
        raise NotImplementedError(
            "Redis admin filter() does not support negated Q objects"
        )
    if not q.children:
        return {}
    if q.connector == Q.OR and len(q.children) > 1:
        raise NotImplementedError(
            "Redis admin filter() does not support OR-combined Q objects"
        )
    out: dict[str, Any] = {}
    for child in q.children:
        if isinstance(child, Q):
            out.update(_q_to_flat_kwargs(child))
        elif isinstance(child, tuple) and len(child) == 2:
            key, value = child
            out[key] = value
        else:
            raise NotImplementedError(
                f"Redis admin filter() cannot interpret Q child {child!r}"
            )
    return out
