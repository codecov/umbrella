"""Smoke tests for the public-bot usage admin changelist."""

from __future__ import annotations

import time
from io import StringIO

import fakeredis
import pytest
from django.core.management import call_command
from django.test import TestCase

import redis_admin.admin as redis_admin_module
import redis_admin.public_bot_usage_queryset as public_bot_queryset
import shared.helpers.redis as shared_redis
from redis_admin.models import PublicBotUsage
from redis_admin.public_bot_usage_queryset import format_reset_at
from shared.django_apps.codecov_auth.tests.factories import UserFactory
from shared.rate_limits.public_bot import (
    list_repo_usage,
    record_pool_state,
    record_repo_request,
)
from utils.test_utils import Client


@pytest.fixture
def patched_redis(monkeypatch) -> fakeredis.FakeStrictRedis:
    server = fakeredis.FakeStrictRedis()

    def _connection():
        return server

    monkeypatch.setattr(shared_redis, "get_redis_connection", _connection)
    monkeypatch.setattr(public_bot_queryset, "get_redis_connection", _connection)
    monkeypatch.setattr(
        "redis_admin.management.commands.seed_public_bot_usage.get_redis_connection",
        _connection,
    )
    return server


def test_queryset_admin_compat_shims():
    qs = PublicBotUsage.objects.all()
    assert qs.query.order_by == ("-hits",)
    assert qs.ordered is True
    assert qs.db == "default"
    assert qs.verbose_name == "Public bot usage"
    assert qs.verbose_name_plural == "Public bot usage"


def test_queryset_none_is_empty():
    assert list(PublicBotUsage.objects.all().none()) == []
    assert PublicBotUsage.objects.all().none().count() == 0


def test_queryset_filter_and_order_by_clone(patched_redis):
    record_pool_state(patched_redis, "commit", 7500, 15000, int(time.time()) + 3600)
    record_repo_request(
        patched_redis, "commit", "org/a", reset_ts=int(time.time()) + 3600
    )
    record_repo_request(
        patched_redis, "pull", "org/b", reset_ts=int(time.time()) + 3600
    )

    rows = list(PublicBotUsage.objects.all().filter(bot="commit").order_by("repo"))
    assert len(rows) == 1
    assert rows[0].repo == "org/a"


def test_queryset_iterator_matches_list(patched_redis):
    record_pool_state(patched_redis, "commit", 7500, 15000, int(time.time()) + 3600)
    record_repo_request(
        patched_redis, "commit", "org/a", reset_ts=int(time.time()) + 3600
    )

    qs = PublicBotUsage.objects.all()
    assert list(qs.iterator()) == list(qs)


def test_format_reset_at_none():
    assert format_reset_at(None) == "—"


def test_seed_public_bot_usage_writes_rows(patched_redis):
    out = StringIO()
    call_command("seed_public_bot_usage", "--clear", stdout=out)

    output = out.getvalue()
    assert "Seeded" in output
    assert "publicbotusage" in output

    rows = list_repo_usage(patched_redis)
    assert len(rows) >= 10
    assert any(row.repo == "mongodb/mongo-python-driver" for row in rows)


def test_seed_public_bot_usage_clear_removes_existing_keys(patched_redis):
    record_pool_state(patched_redis, "commit", 7500, 15000, int(time.time()) + 3600)
    record_repo_request(
        patched_redis, "commit", "org/old", reset_ts=int(time.time()) + 3600
    )

    call_command("seed_public_bot_usage", "--clear", stdout=StringIO())

    rows = list_repo_usage(patched_redis, repo_filter="org/old")
    assert rows == []


class PublicBotUsageAdminSmokeTest(TestCase):
    def setUp(self):
        self.redis = fakeredis.FakeStrictRedis()
        self._orig_shared = shared_redis.get_redis_connection
        self._orig_queryset = public_bot_queryset.get_redis_connection
        self._orig_admin = redis_admin_module.get_redis_connection
        shared_redis.get_redis_connection = lambda: self.redis  # type: ignore[assignment]
        public_bot_queryset.get_redis_connection = lambda: self.redis  # type: ignore[assignment]
        redis_admin_module.get_redis_connection = lambda: self.redis  # type: ignore[assignment]

        self.user = UserFactory(is_staff=True)
        self.client = Client()
        self.client.force_login(self.user)
        self.reset_ts = int(time.time()) + 3600

    def tearDown(self):
        shared_redis.get_redis_connection = self._orig_shared  # type: ignore[assignment]
        public_bot_queryset.get_redis_connection = self._orig_queryset  # type: ignore[assignment]
        redis_admin_module.get_redis_connection = self._orig_admin  # type: ignore[assignment]

    def test_changelist_renders_usage_and_top_ten(self):
        record_pool_state(self.redis, "commit", 7500, 15000, self.reset_ts)
        record_pool_state(self.redis, "pull", 7000, 15000, self.reset_ts)
        for _ in range(10):
            record_repo_request(
                self.redis, "commit", "org/heavy", reset_ts=self.reset_ts
            )
        record_repo_request(self.redis, "pull", "org/light", reset_ts=self.reset_ts)

        response = self.client.get("/admin/redis_admin/publicbotusage/")

        assert response.status_code == 200, response.content[:500]
        body = response.content.decode("utf-8", errors="replace")
        assert "Top 10 repositories by usage" in body
        assert "org/heavy" in body
        assert "org/light" in body
        assert "commit" in body

    def test_bot_filter_narrows_rows(self):
        record_pool_state(self.redis, "commit", 7500, 15000, self.reset_ts)
        record_pool_state(self.redis, "pull", 7000, 15000, self.reset_ts)
        record_repo_request(self.redis, "commit", "org/a", reset_ts=self.reset_ts)
        record_repo_request(self.redis, "pull", "org/b", reset_ts=self.reset_ts)

        response = self.client.get(
            "/admin/redis_admin/publicbotusage/",
            {"bot": "commit"},
        )

        assert response.status_code == 200
        body = response.content.decode("utf-8", errors="replace")
        assert "org/a" in body
        assert "org/b" not in body
