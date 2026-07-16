"""Smoke tests for the public-bot usage admin changelist."""

from __future__ import annotations

import time

import fakeredis
from django.test import TestCase

import shared.helpers.redis as shared_redis
from shared.django_apps.codecov_auth.tests.factories import UserFactory
from shared.rate_limits.public_bot import record_pool_state, record_repo_request
from utils.test_utils import Client


class PublicBotUsageAdminSmokeTest(TestCase):
    def setUp(self):
        self.redis = fakeredis.FakeStrictRedis()
        self._orig_get_redis = shared_redis.get_redis_connection
        shared_redis.get_redis_connection = lambda: self.redis  # type: ignore[assignment]

        self.user = UserFactory(is_staff=True)
        self.client = Client()
        self.client.force_login(self.user)
        self.reset_ts = int(time.time()) + 3600

    def tearDown(self):
        shared_redis.get_redis_connection = self._orig_get_redis  # type: ignore[assignment]

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
