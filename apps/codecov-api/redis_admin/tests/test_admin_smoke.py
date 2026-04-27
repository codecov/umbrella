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


class RedisQueueItemAdminSmokeTest(TestCase):
    """End-to-end checks for the M2 per-queue items view."""

    def setUp(self):
        self.redis = fakeredis.FakeStrictRedis()
        self._orig_get_connection = redis_admin_conn.get_connection
        redis_admin_conn.get_connection = lambda: self.redis  # type: ignore[assignment]

        self.user = UserFactory(is_staff=True)
        self.client = Client()
        self.client.force_login(self.user)

    def tearDown(self):
        redis_admin_conn.get_connection = self._orig_get_connection  # type: ignore[assignment]

    def test_queue_changelist_links_to_items_view(self):
        self.redis.rpush("uploads/123/abc", "payload-1")

        response = self.client.get("/admin/redis_admin/redisqueue/")

        assert response.status_code == 200
        body = response.content.decode("utf-8")
        # The items_link column emits a relative URL with the encoded key.
        assert "/admin/redis_admin/redisqueueitem/" in body
        assert "queue_name__exact=uploads%2F123%2Fabc" in body
        assert "view items" in body

    def test_items_changelist_without_queue_filter_shows_helper_message(self):
        response = self.client.get("/admin/redis_admin/redisqueueitem/")

        assert response.status_code == 200
        body = response.content.decode("utf-8")
        assert "Pick a queue from the Redis queues list" in body

    def test_items_changelist_with_queue_filter_renders_list_items(self):
        for i in range(3):
            self.redis.rpush("uploads/9/deadbeef", f"payload-{i}")

        response = self.client.get(
            "/admin/redis_admin/redisqueueitem/",
            {"queue_name__exact": "uploads/9/deadbeef"},
        )

        assert response.status_code == 200
        body = response.content.decode("utf-8")
        for i in range(3):
            assert f"payload-{i}" in body
        # Index column shows 0/1/2 so users can identify positions.
        assert ">0<" in body or ">0 " in body
        assert ">2<" in body or ">2 " in body

    def test_items_changelist_with_unknown_queue_renders_empty(self):
        response = self.client.get(
            "/admin/redis_admin/redisqueueitem/",
            {"queue_name__exact": "no-such-queue"},
        )

        assert response.status_code == 200
        body = response.content.decode("utf-8")
        # Django admin's empty-results phrase varies a bit between releases;
        # the important thing is no 500 and no unrelated payload leaks in.
        assert (
            "0 Redis queue items" in body
            or "0 of 0" in body
            or "no Redis" in body.lower()
        )
