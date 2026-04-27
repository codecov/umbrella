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


class RedisAdminFiltersAndSearchTest(TestCase):
    """M3 end-to-end: list_filter, search, repo/commit links."""

    def setUp(self):
        self.redis = fakeredis.FakeStrictRedis()
        self._orig_get_connection = redis_admin_conn.get_connection
        redis_admin_conn.get_connection = lambda: self.redis  # type: ignore[assignment]

        self.user = UserFactory(is_staff=True)
        self.client = Client()
        self.client.force_login(self.user)

    def tearDown(self):
        redis_admin_conn.get_connection = self._orig_get_connection  # type: ignore[assignment]

    def _populate_diverse_keys(self):
        # repo 1234 has several queues across coverage / test_results /
        # ta_flake_key, and repo 9999 has an unrelated queue we expect to
        # be filtered out by repoid searches.
        self.redis.rpush("uploads/1234/sha-aaa", "p1")
        self.redis.rpush("uploads/1234/sha-aaa", "p2")
        self.redis.rpush("uploads/1234/sha-bbb/test_results", "p3")
        self.redis.rpush("ta_flake_key:1234", "p4")
        self.redis.rpush("uploads/9999/sha-zzz", "p5")

    def test_search_by_repoid_token_returns_only_that_repos_queues(self):
        """End-of-M3 acceptance: 'repoid:1234' surfaces all backed-up
        queues for that repo and excludes others."""

        self._populate_diverse_keys()

        response = self.client.get(
            "/admin/redis_admin/redisqueue/", {"q": "repoid:1234"}
        )

        assert response.status_code == 200
        body = response.content.decode("utf-8")
        assert "uploads/1234/sha-aaa" in body
        assert "uploads/1234/sha-bbb/test_results" in body
        assert "ta_flake_key:1234" in body
        assert "uploads/9999/sha-zzz" not in body

    def test_search_by_commitid_token_filters_to_matching_prefix(self):
        self._populate_diverse_keys()

        response = self.client.get(
            "/admin/redis_admin/redisqueue/", {"q": "commitid:sha-aaa"}
        )

        assert response.status_code == 200
        body = response.content.decode("utf-8")
        assert "uploads/1234/sha-aaa" in body
        assert "uploads/1234/sha-bbb/test_results" not in body
        assert "ta_flake_key:1234" not in body

    def test_family_list_filter_narrows_results(self):
        self._populate_diverse_keys()

        response = self.client.get(
            "/admin/redis_admin/redisqueue/", {"family": "ta_flake_key"}
        )

        assert response.status_code == 200
        body = response.content.decode("utf-8")
        assert "ta_flake_key:1234" in body
        assert "uploads/1234/sha-aaa" not in body

    def test_min_depth_filter_drops_shallow_queues(self):
        self.redis.rpush("uploads/1/shallow", "x")
        for i in range(5):
            self.redis.rpush("uploads/1/deep", f"x{i}")

        response = self.client.get("/admin/redis_admin/redisqueue/", {"min_depth": "5"})

        assert response.status_code == 200
        body = response.content.decode("utf-8")
        assert "uploads/1/deep" in body
        assert "uploads/1/shallow" not in body

    def test_report_type_filter_narrows_to_typed_uploads(self):
        self.redis.rpush("uploads/1/sha", "x")  # coverage default
        self.redis.rpush("uploads/1/sha/test_results", "x")
        self.redis.rpush("uploads/1/sha/bundle_analysis", "x")

        response = self.client.get(
            "/admin/redis_admin/redisqueue/",
            {"report_type": "test_results"},
        )

        assert response.status_code == 200
        body = response.content.decode("utf-8")
        assert "uploads/1/sha/test_results" in body
        assert "uploads/1/sha/bundle_analysis" not in body

    def test_repoid_link_points_at_repository_admin(self):
        self.redis.rpush("uploads/4242/sha", "x")

        response = self.client.get("/admin/redis_admin/redisqueue/")

        assert response.status_code == 200
        body = response.content.decode("utf-8")
        # Default Django admin URL for `core.Repository` keyed by repoid.
        assert "/admin/core/repository/4242/change/" in body

    def test_commitid_link_points_at_commit_changelist_search(self):
        self.redis.rpush("uploads/1/cafef00dcafef00d", "x")

        response = self.client.get("/admin/redis_admin/redisqueue/")

        assert response.status_code == 200
        body = response.content.decode("utf-8")
        assert "/admin/core/commit/?q=cafef00dcafef00d" in body
