"""Unit tests for Django 5 admin filter(Q) compat helpers."""

from __future__ import annotations

import pytest
from django.db.models import Q

from redis_admin.filter_compat import kwargs_from_filter_args


def test_kwargs_from_filter_args_empty():
    assert kwargs_from_filter_args(()) == {}


def test_kwargs_from_filter_args_empty_q():
    assert kwargs_from_filter_args((Q(),)) == {}


def test_kwargs_from_filter_args_simple_q():
    assert kwargs_from_filter_args((Q(repoid=1, family__exact="uploads"),)) == {
        "repoid": 1,
        "family__exact": "uploads",
    }


def test_kwargs_from_filter_args_and_combined():
    assert kwargs_from_filter_args((Q(repoid=1) & Q(commitid__startswith="abc"),)) == {
        "repoid": 1,
        "commitid__startswith": "abc",
    }


def test_kwargs_from_filter_args_rejects_or():
    with pytest.raises(NotImplementedError):
        kwargs_from_filter_args((Q(repoid=1) | Q(repoid=2),))


def test_kwargs_from_filter_args_rejects_non_q():
    with pytest.raises(NotImplementedError):
        kwargs_from_filter_args(("repoid",))
