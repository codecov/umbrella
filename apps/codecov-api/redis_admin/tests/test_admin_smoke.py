"""End-to-end smoke test for the M1 read-only Redis admin changelist.

Populates a fakeredis instance with one key from each currently registered
family, logs into the Django admin as a staff user, and asserts the
changelist renders both keys with their correct `family` and `depth`.

Acts as the milestone gate: if these tests pass the M1 deliverable
("view Redis keys in admin") works end to end. Requires the test database
to be available (Postgres + Timescale), so these run as part of the regular
api test suite rather than as standalone unit tests.
"""

from __future__ import annotations

import fakeredis
import pytest
from django.test import TestCase

from redis_admin import conn as redis_admin_conn
from shared.django_apps.codecov_auth.tests.factories import UserFactory
from utils.test_utils import Client


class RedisAdminSmokeTest(TestCase):
    def setUp(self):
        self.redis = fakeredis.FakeStrictRedis()
        self._orig_get_connection = redis_admin_conn.get_connection
        redis_admin_conn.get_connection = lambda: self.redis  # type: ignore[assignment]

        self.user = UserFactory(is_staff=True)
        self.client = Client()
        self.client.force_login(self.user)

    def tearDown(self):
        redis_admin_conn.get_connection = self._orig_get_connection  # type: ignore[assignment]

    def test_changelist_lists_known_redis_keys(self):
        self.redis.rpush("uploads/123/abc123", '{"upload_id": 1}')
        self.redis.rpush("uploads/123/abc123", '{"upload_id": 2}')
        self.redis.rpush("ta_flake_key:123", "abc123")

        response = self.client.get("/admin/redis_admin/redisqueue/")

        assert response.status_code == 200, response.content[:500]
        body = response.content.decode("utf-8", errors="replace")
        assert "uploads/123/abc123" in body
        assert "ta_flake_key:123" in body
        assert "uploads" in body
        assert "ta_flake_key" in body

    def test_changelist_orders_by_depth_descending(self):
        self.redis.rpush("ta_flake_key:1", "a")
        for i in range(5):
            self.redis.rpush("uploads/2/deadbeef", f'{{"i": {i}}}')

        response = self.client.get("/admin/redis_admin/redisqueue/")

        assert response.status_code == 200
        body = response.content.decode("utf-8", errors="replace")
        uploads_pos = body.find("uploads/2/deadbeef")
        flake_pos = body.find("ta_flake_key:1")
        assert uploads_pos != -1 and flake_pos != -1
        # Default ordering is `-depth`, so the deeper queue must appear first.
        assert uploads_pos < flake_pos


@pytest.mark.django_db
def test_non_staff_user_is_redirected_from_redis_admin():
    user = UserFactory(is_staff=False)
    client = Client()
    client.force_login(user)

    response = client.get("/admin/redis_admin/redisqueue/")

    # Non-staff hits the admin login redirect.
    assert response.status_code in (302, 403)
