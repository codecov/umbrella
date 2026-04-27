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
from django.contrib.admin.models import LogEntry
from django.test import TestCase

from redis_admin import conn as redis_admin_conn
from shared.django_apps.codecov_auth.tests.factories import UserFactory
from shared.django_apps.core.tests.factories import OwnerFactory, RepositoryFactory
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

    def test_item_change_page_renders_for_list_item(self):
        # Reproduces the user-reported NotImplementedError: clicking an
        # item row on the changelist hits `/<pk_token>/change/`, which
        # calls `queryset.get(pk_token=…)`. The pk_token is
        # `<queue>#<index>` for LIST items, url-encoded by Django so
        # `/` → `_2F` and `#` → `_23`.
        self.redis.rpush("uploads/123/abcdef", "first-payload")
        self.redis.rpush("uploads/123/abcdef", "second-payload")
        url = "/admin/redis_admin/redisqueueitem/uploads_2F123_2Fabcdef_230/change/"

        response = self.client.get(url)

        assert response.status_code == 200
        body = response.content.decode("utf-8")
        # The first item's value renders on the readonly page.
        assert "first-payload" in body
        # No save buttons because the item is a strict read-only inspector.
        assert 'name="_save"' not in body

    def test_item_change_page_for_string_key(self):
        self.redis.set("latest_upload/1/sha", "string-payload")
        url = "/admin/redis_admin/redisqueueitem/latest_5Fupload_2F1_2Fsha_23v/change/"

        response = self.client.get(url)

        assert response.status_code == 200
        body = response.content.decode("utf-8")
        assert "string-payload" in body

    def test_item_change_page_404s_for_missing_index(self):
        self.redis.rpush("uploads/1/sha", "only-one")
        # Index 99 doesn't exist; admin should 302/404 rather than 500.
        url = "/admin/redis_admin/redisqueueitem/uploads_2F1_2Fsha_2399/change/"

        response = self.client.get(url)

        assert response.status_code in (302, 404)

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

    def test_repo_display_column_renders_service_owner_name(self):
        owner = OwnerFactory(service="github", username="codecov")
        repo = RepositoryFactory(author=owner, name="example")
        self.redis.rpush(f"uploads/{repo.repoid}/abc", "x")

        response = self.client.get("/admin/redis_admin/redisqueue/")

        assert response.status_code == 200
        body = response.content.decode("utf-8")
        assert f"uploads/{repo.repoid}/abc" in body
        assert "github:codecov/example" in body

    def test_repo_display_column_falls_back_to_dash_when_repo_missing(self):
        # Redis still has a queue for a repoid that no longer exists in
        # the DB (e.g. repo was deleted); the column degrades gracefully
        # rather than 500ing the changelist.
        self.redis.rpush("uploads/9999999/abc", "x")

        response = self.client.get("/admin/redis_admin/redisqueue/")

        assert response.status_code == 200
        body = response.content.decode("utf-8")
        # Fallback is the em-dash placeholder.
        assert "—" in body


class RedisLockAdminSmokeTest(TestCase):
    """M4.3 end-to-end checks for the read-only locks changelist."""

    def setUp(self):
        self.redis = fakeredis.FakeStrictRedis()
        self._orig_get_connection = redis_admin_conn.get_connection
        redis_admin_conn.get_connection = lambda: self.redis  # type: ignore[assignment]

        self.user = UserFactory(is_staff=True)
        self.client = Client()
        self.client.force_login(self.user)

    def tearDown(self):
        redis_admin_conn.get_connection = self._orig_get_connection  # type: ignore[assignment]

    def test_lock_changelist_lists_lock_keys_only(self):
        # Mix of lock and queue keys; only the lock ones should show up
        # on /redislock/ and only the queue ones on /redisqueue/.
        self.redis.set("upload_lock_1234_abc", "1")
        self.redis.set("ta_flake_lock:1234", "1")
        self.redis.set("upload_finisher_gate_1234_abc", "1")
        self.redis.rpush("uploads/1234/abc", "payload")

        response = self.client.get("/admin/redis_admin/redislock/")

        assert response.status_code == 200
        body = response.content.decode("utf-8")
        assert "upload_lock_1234_abc" in body
        assert "ta_flake_lock:1234" in body
        assert "upload_finisher_gate_1234_abc" in body
        # Queue keys must not bleed into the locks page.
        assert "uploads/1234/abc" not in body

    def test_locks_changelist_has_no_delete_action(self):
        self.redis.set("upload_lock_1_abc", "1")

        response = self.client.get("/admin/redis_admin/redislock/")

        assert response.status_code == 200
        body = response.content.decode("utf-8")
        # The default "Delete selected" action must be stripped.
        assert "delete_selected" not in body


class RedisQueueAdminChangePageTest(TestCase):
    """Clicking a row on the changelist opens the per-key inspector."""

    def setUp(self):
        self.redis = fakeredis.FakeStrictRedis()
        self._orig_get_connection = redis_admin_conn.get_connection
        redis_admin_conn.get_connection = lambda: self.redis  # type: ignore[assignment]

        self.user = UserFactory(is_staff=True, is_superuser=True)
        self.client = Client()
        self.client.force_login(self.user)

    def tearDown(self):
        redis_admin_conn.get_connection = self._orig_get_connection  # type: ignore[assignment]

    def test_change_page_renders_for_keys_with_slashes(self):
        # Reproduces the user-reported NotImplementedError: a `latest_upload/
        # 123/<sha>` key is the changelist row's pk, so Django's admin
        # url-quotes the slashes (`/` → `_2F`) and looks the row up via
        # `queryset.get(name=…)`.
        self.redis.set(
            "latest_upload/123/8ebf8abc9d07cceb42ead4b94edb494d52eefd2f", "1"
        )
        url = "/admin/redis_admin/redisqueue/latest_5Fupload_2F123_2F8ebf8abc9d07cceb42ead4b94edb494d52eefd2f/change/"

        response = self.client.get(url)

        assert response.status_code == 200
        body = response.content.decode("utf-8")
        # Family + repoid render in the inspector pane.
        assert "latest_upload" in body
        assert "123" in body
        # Save buttons must be suppressed; only Delete (and history) render.
        assert 'name="_save"' not in body
        assert 'name="_continue"' not in body
        assert 'name="_addanother"' not in body
        # Superuser sees the delete link.
        assert "delete/" in body

    def test_change_page_404s_when_redis_key_missing(self):
        url = "/admin/redis_admin/redisqueue/no_2Dsuch_2Fkey/change/"
        response = self.client.get(url)
        # Django's admin returns a 302 → "?" or a 404 for a missing object;
        # both are fine, the important part is no 500 from
        # NotImplementedError.
        assert response.status_code in (302, 404)

    def test_change_page_renders_inline_items_preview_for_list_queue(self):
        # Operator clicks a backed-up `uploads/<repoid>/<sha>` queue;
        # the change page should show the queue's items inline so they
        # can investigate without a second tab.
        for i in range(3):
            self.redis.rpush("uploads/123/abc123", f'{{"upload_id": {i}}}'.encode())

        url = "/admin/redis_admin/redisqueue/uploads_2F123_2Fabc123/change/"
        response = self.client.get(url)

        assert response.status_code == 200
        body = response.content.decode("utf-8")
        # The items render as a Django tabular inline (matching the
        # admin's `TabularInline` look). The wrapper, headers, and
        # row values should all be present. Values are HTML-escaped
        # by `format_html`, so look for the escaped form.
        assert 'class="js-inline-admin-formset inline-group"' in body
        assert "Index / field" in body
        for i in range(3):
            assert f"upload_id&quot;: {i}" in body
        # Each row should link to the per-item inspector for drill-in.
        assert (
            "/admin/redis_admin/redisqueueitem/uploads_2F123_2Fabc123_230/change/"
            in body
        )
        # And the footer link points at the full items changelist.
        assert "queue_name__exact=uploads/123/abc123" in body

    def test_change_page_renders_inline_items_preview_for_string_key(self):
        # `latest_upload/<repoid>/<sha>` is a STRING; the held value
        # should appear in the preview.
        self.redis.set(
            "latest_upload/123/8ebf8abc9d07cceb42ead4b94edb494d52eefd2f",
            "the-stored-id",
        )

        url = "/admin/redis_admin/redisqueue/latest_5Fupload_2F123_2F8ebf8abc9d07cceb42ead4b94edb494d52eefd2f/change/"
        response = self.client.get(url)

        assert response.status_code == 200
        body = response.content.decode("utf-8")
        assert "the-stored-id" in body
        assert "(value)" in body


class RedisQueueAdminDeleteActionsTest(TestCase):
    """M5.2 end-to-end: dry-run action, real delete, non-superuser blocked."""

    def setUp(self):
        self.redis = fakeredis.FakeStrictRedis()
        self._orig_get_connection = redis_admin_conn.get_connection
        redis_admin_conn.get_connection = lambda: self.redis  # type: ignore[assignment]

        self.client = Client()

    def tearDown(self):
        redis_admin_conn.get_connection = self._orig_get_connection  # type: ignore[assignment]

    def _populate(self):
        self.redis.rpush("uploads/1/aaa", "p1")
        self.redis.rpush("uploads/1/bbb", "p2")

    def test_non_superuser_cannot_see_clear_selected(self):
        staff = UserFactory(is_staff=True, is_superuser=False)
        self.client.force_login(staff)
        self._populate()

        response = self.client.get("/admin/redis_admin/redisqueue/")

        assert response.status_code == 200
        body = response.content.decode("utf-8")
        # Non-superusers can dry-run but never see the destructive action.
        assert "Clear selected" not in body
        # Stock Django delete is stripped out for everyone.
        assert "Delete selected" not in body
        assert "Dry-run" in body

    def test_stock_delete_selected_action_is_unavailable(self):
        # `delete_selected` was Django's stock no-dry-run path; we
        # deliberately strip it so the only real-mutation bulk action
        # is `clear_selected`, which forces a dry-run interstitial.
        superuser = UserFactory(is_staff=True, is_superuser=True)
        self.client.force_login(superuser)
        self._populate()

        response = self.client.get("/admin/redis_admin/redisqueue/")

        assert response.status_code == 200
        body = response.content.decode("utf-8")
        assert 'value="delete_selected"' not in body
        # Sanity: our replacement is offered.
        assert 'value="clear_selected"' in body

    def test_clear_dry_run_action_does_not_delete(self):
        superuser = UserFactory(is_staff=True, is_superuser=True)
        self.client.force_login(superuser)
        self._populate()

        response = self.client.post(
            "/admin/redis_admin/redisqueue/",
            {
                "action": "clear_dry_run",
                "_selected_action": ["uploads/1/aaa", "uploads/1/bbb"],
            },
            follow=True,
        )

        assert response.status_code == 200
        body = response.content.decode("utf-8")
        assert "Dry-run: would delete 2" in body
        # Keys must still be in Redis after a dry-run.
        assert self.redis.exists("uploads/1/aaa") == 1
        assert self.redis.exists("uploads/1/bbb") == 1
        # Audit trail: dry-runs are recorded.
        assert LogEntry.objects.count() >= 1

    def test_clear_selected_renders_dry_run_preview_first(self):
        # Stage 1 of the new bulk-clear flow: posting `clear_selected`
        # with no `confirm` flag must render the dry-run preview page,
        # NOT delete anything.
        superuser = UserFactory(is_staff=True, is_superuser=True)
        self.client.force_login(superuser)
        self._populate()
        prior_logs = LogEntry.objects.count()

        response = self.client.post(
            "/admin/redis_admin/redisqueue/",
            {
                "action": "clear_selected",
                "_selected_action": ["uploads/1/aaa", "uploads/1/bbb"],
            },
        )

        assert response.status_code == 200
        body = response.content.decode("utf-8")
        # Preview page renders the dry-run summary…
        assert "Dry-run summary" in body
        assert "Would delete:" in body
        assert "<strong>2</strong>" in body
        # …a sample list…
        assert "uploads/1/aaa" in body
        assert "uploads/1/bbb" in body
        # …and a confirmation form that re-posts with `confirm=yes`.
        assert 'name="confirm" value="yes"' in body
        assert 'name="action" value="clear_selected"' in body
        assert 'name="_selected_action" value="uploads/1/aaa"' in body
        # No keys deleted yet.
        assert self.redis.exists("uploads/1/aaa") == 1
        assert self.redis.exists("uploads/1/bbb") == 1
        # Audit trail still has the dry-run recorded.
        assert LogEntry.objects.count() == prior_logs + 1

    def test_clear_selected_with_confirm_actually_clears_keys(self):
        # Stage 2: re-post with `confirm=yes` runs the real delete.
        superuser = UserFactory(is_staff=True, is_superuser=True)
        self.client.force_login(superuser)
        self._populate()
        prior_logs = LogEntry.objects.count()

        # Stage 1: open the confirmation page (records a dry-run audit).
        stage1 = self.client.post(
            "/admin/redis_admin/redisqueue/",
            {
                "action": "clear_selected",
                "_selected_action": ["uploads/1/aaa", "uploads/1/bbb"],
            },
        )
        assert stage1.status_code == 200
        # Mid-flight: keys still present.
        assert self.redis.exists("uploads/1/aaa") == 1

        # Stage 2: submit the confirmation form.
        stage2 = self.client.post(
            "/admin/redis_admin/redisqueue/",
            {
                "action": "clear_selected",
                "_selected_action": ["uploads/1/aaa", "uploads/1/bbb"],
                "confirm": "yes",
            },
            follow=True,
        )

        assert stage2.status_code == 200
        # Both keys should now be gone.
        assert self.redis.exists("uploads/1/aaa") == 0
        assert self.redis.exists("uploads/1/bbb") == 0
        # Audit log should record both the dry-run AND the real delete.
        assert LogEntry.objects.count() == prior_logs + 2

    def test_clear_selected_cancel_does_not_delete(self):
        # Operators must be able to back out of the dry-run preview
        # without deleting anything.
        superuser = UserFactory(is_staff=True, is_superuser=True)
        self.client.force_login(superuser)
        self._populate()

        # Stage 1.
        self.client.post(
            "/admin/redis_admin/redisqueue/",
            {
                "action": "clear_selected",
                "_selected_action": ["uploads/1/aaa", "uploads/1/bbb"],
            },
        )
        # The "No, take me back" link is just a GET to the changelist;
        # exercise it explicitly.
        response = self.client.get("/admin/redis_admin/redisqueue/")

        assert response.status_code == 200
        # Nothing was deleted because stage 2 never happened.
        assert self.redis.exists("uploads/1/aaa") == 1
        assert self.redis.exists("uploads/1/bbb") == 1


class RedisAdminClearByScopeTest(TestCase):
    """M6: cross-family clear-by-scope view.

    Verifies the dedicated `/clear-by-scope/` URL aggregates matching
    keys across multiple deletable families and gates the destructive
    action behind a typed-confirmation guard. Locks (`is_deletable=
    False`) must never be cleared, even if the operator's scope picks
    them up.
    """

    URL = "/admin/redis_admin/redisqueue/clear-by-scope/"

    def setUp(self):
        self.redis = fakeredis.FakeStrictRedis()
        self._orig_get_connection = redis_admin_conn.get_connection
        redis_admin_conn.get_connection = lambda: self.redis  # type: ignore[assignment]
        self.client = Client()

    def tearDown(self):
        redis_admin_conn.get_connection = self._orig_get_connection  # type: ignore[assignment]

    def _populate_repo_42(self):
        # Two queue-family keys for repoid=42; one for a different repo
        # to verify scoping; one lock that must be refused.
        self.redis.rpush("uploads/42/abc123", "p1")
        self.redis.set("latest_upload/42/abc123", "u1")
        self.redis.rpush("uploads/99/zzz999", "p2")
        # Lock family — must be excluded from the matched targets.
        self.redis.set("upload_finisher_gate_42_abc123", "1")

    def test_link_visible_in_changelist_object_tools_for_superuser(self):
        superuser = UserFactory(is_staff=True, is_superuser=True)
        self.client.force_login(superuser)
        self._populate_repo_42()

        response = self.client.get("/admin/redis_admin/redisqueue/")

        assert response.status_code == 200
        body = response.content.decode("utf-8")
        assert "Clear by scope" in body
        assert self.URL in body

    def test_link_hidden_from_non_superuser(self):
        staff = UserFactory(is_staff=True, is_superuser=False)
        self.client.force_login(staff)
        self._populate_repo_42()

        response = self.client.get("/admin/redis_admin/redisqueue/")

        assert response.status_code == 200
        body = response.content.decode("utf-8")
        assert "Clear by scope" not in body

    def test_non_superuser_gets_permission_denied(self):
        staff = UserFactory(is_staff=True, is_superuser=False)
        self.client.force_login(staff)

        response = self.client.get(self.URL)

        # Django's admin_view wraps PermissionDenied as a 403.
        assert response.status_code == 403

    def test_get_without_scope_renders_empty_form(self):
        superuser = UserFactory(is_staff=True, is_superuser=True)
        self.client.force_login(superuser)

        response = self.client.get(self.URL)

        assert response.status_code == 200
        body = response.content.decode("utf-8")
        assert "Clear Redis by scope" in body
        assert "Repository ID" in body
        # No "Matched: N keys" panel appears until a scope is supplied.
        assert "Matched:" not in body

    def test_get_with_repoid_lists_matching_keys_across_families(self):
        superuser = UserFactory(is_staff=True, is_superuser=True)
        self.client.force_login(superuser)
        self._populate_repo_42()

        response = self.client.get(self.URL + "?repoid=42")

        assert response.status_code == 200
        body = response.content.decode("utf-8")
        # Aggregated count: uploads/42/abc123 + latest_upload/42/abc123,
        # NOT uploads/99/zzz999, NOT the lock.
        assert "Matched: 2 key(s)" in body
        assert "uploads/42/abc123" in body
        assert "latest_upload/42/abc123" in body
        assert "uploads/99/zzz999" not in body
        assert "upload_finisher_gate" not in body
        # Both family names render.
        assert "uploads" in body
        assert "latest_upload" in body

    def test_get_with_invalid_repoid_shows_error(self):
        superuser = UserFactory(is_staff=True, is_superuser=True)
        self.client.force_login(superuser)

        response = self.client.get(self.URL + "?repoid=not-an-int")

        assert response.status_code == 200
        body = response.content.decode("utf-8")
        assert "repoid must be an integer" in body

    def test_post_dry_run_does_not_delete_and_audits(self):
        superuser = UserFactory(is_staff=True, is_superuser=True)
        self.client.force_login(superuser)
        self._populate_repo_42()
        prior_logs = LogEntry.objects.count()

        response = self.client.post(
            self.URL,
            {
                "repoid": "42",
                "commitid": "",
                "typed_confirm": "42",
                "action": "dry_run",
            },
            follow=True,
        )

        assert response.status_code == 200
        body = response.content.decode("utf-8")
        assert "Dry-run: would clear 2" in body
        # Nothing is actually deleted.
        assert self.redis.exists("uploads/42/abc123") == 1
        assert self.redis.exists("latest_upload/42/abc123") == 1
        # Out-of-scope and lock keys remain untouched.
        assert self.redis.exists("uploads/99/zzz999") == 1
        assert self.redis.exists("upload_finisher_gate_42_abc123") == 1
        # Audit log still records the dry-run.
        assert LogEntry.objects.count() == prior_logs + 1

    def test_post_confirm_clears_matching_keys_only(self):
        superuser = UserFactory(is_staff=True, is_superuser=True)
        self.client.force_login(superuser)
        self._populate_repo_42()

        response = self.client.post(
            self.URL,
            {
                "repoid": "42",
                "commitid": "",
                "typed_confirm": "42",
                "action": "confirm",
            },
            follow=True,
        )

        assert response.status_code == 200
        # Repo-42 keys are gone.
        assert self.redis.exists("uploads/42/abc123") == 0
        assert self.redis.exists("latest_upload/42/abc123") == 0
        # Other repo's keys are untouched.
        assert self.redis.exists("uploads/99/zzz999") == 1
        # Lock is untouched (refused by `is_deletable=False`).
        assert self.redis.exists("upload_finisher_gate_42_abc123") == 1
        # Confirm runs redirect to the changelist with a success
        # message; both should appear in the followed response.
        body = response.content.decode("utf-8")
        assert "Cleared 2 key(s)" in body

    def test_post_with_mismatched_typed_confirmation_blocks_delete(self):
        superuser = UserFactory(is_staff=True, is_superuser=True)
        self.client.force_login(superuser)
        self._populate_repo_42()

        response = self.client.post(
            self.URL,
            {
                "repoid": "42",
                "commitid": "",
                "typed_confirm": "wrong",
                "action": "confirm",
            },
        )

        assert response.status_code == 200
        body = response.content.decode("utf-8")
        assert "Typed confirmation must equal" in body
        # Nothing was deleted.
        assert self.redis.exists("uploads/42/abc123") == 1
        assert self.redis.exists("latest_upload/42/abc123") == 1

    def test_post_without_scope_is_refused(self):
        superuser = UserFactory(is_staff=True, is_superuser=True)
        self.client.force_login(superuser)
        self._populate_repo_42()

        response = self.client.post(
            self.URL,
            {
                "repoid": "",
                "commitid": "",
                "typed_confirm": "",
                "action": "confirm",
            },
        )

        assert response.status_code == 200
        body = response.content.decode("utf-8")
        assert "scope must include a repoid, commitid, or at least one family" in body
        # Sanity: nothing was cleared.
        assert self.redis.exists("uploads/42/abc123") == 1

    def test_post_confirm_with_commitid_only_clears_matching_keys(self):
        superuser = UserFactory(is_staff=True, is_superuser=True)
        self.client.force_login(superuser)
        # Two different repos sharing a commit prefix.
        self.redis.rpush("uploads/42/abc123", "p1")
        self.redis.rpush("uploads/99/abc456", "p2")

        response = self.client.post(
            self.URL,
            {
                "repoid": "",
                "commitid": "abc",
                "typed_confirm": "abc",
                "action": "confirm",
            },
            follow=True,
        )

        assert response.status_code == 200
        # Both commits-prefixed-with "abc" should be cleared.
        assert self.redis.exists("uploads/42/abc123") == 0
        assert self.redis.exists("uploads/99/abc456") == 0

    # ---- Family-scope (M6 follow-up) -----------------------------------

    def test_get_with_repoid_and_family_narrows_to_that_family(self):
        superuser = UserFactory(is_staff=True, is_superuser=True)
        self.client.force_login(superuser)
        self._populate_repo_42()

        # Repo 42 has both `uploads/*` and `latest_upload/*` keys; the
        # family filter restricts the scope to `uploads/*`.
        response = self.client.get(self.URL + "?repoid=42&family=uploads")

        assert response.status_code == 200
        body = response.content.decode("utf-8")
        assert "Matched: 1 key(s)" in body
        assert "uploads/42/abc123" in body
        assert "latest_upload/42/abc123" not in body

    def test_post_confirm_with_family_only_clears_all_keys_of_that_family(self):
        superuser = UserFactory(is_staff=True, is_superuser=True)
        self.client.force_login(superuser)
        # Two repos with `uploads/*`; one repo with `latest_upload/*`.
        self.redis.rpush("uploads/42/abc123", "p1")
        self.redis.rpush("uploads/99/zzz999", "p2")
        self.redis.set("latest_upload/42/abc123", "u1")

        response = self.client.post(
            self.URL,
            {
                "repoid": "",
                "commitid": "",
                "family": ["uploads"],
                # Family-only scope confirms by typing the joined family
                # names.
                "typed_confirm": "uploads",
                "action": "confirm",
            },
            follow=True,
        )

        assert response.status_code == 200
        # Both `uploads/*` keys are gone, regardless of repo.
        assert self.redis.exists("uploads/42/abc123") == 0
        assert self.redis.exists("uploads/99/zzz999") == 0
        # Other family is untouched.
        assert self.redis.exists("latest_upload/42/abc123") == 1
        body = response.content.decode("utf-8")
        assert "Cleared 2 key(s)" in body

    def test_post_confirm_with_multiple_families_clears_each(self):
        superuser = UserFactory(is_staff=True, is_superuser=True)
        self.client.force_login(superuser)
        self.redis.rpush("uploads/42/abc123", "p1")
        self.redis.set("latest_upload/42/abc123", "u1")
        # Third family (intermediate-report) intentionally untouched —
        # it's not in the requested family list.
        self.redis.hset("intermediate-report/42/abc123/coverage", "f", "v")

        response = self.client.post(
            self.URL,
            {
                "repoid": "42",
                "commitid": "",
                "family": ["uploads", "latest_upload"],
                "typed_confirm": "42",
                "action": "confirm",
            },
            follow=True,
        )

        assert response.status_code == 200
        assert self.redis.exists("uploads/42/abc123") == 0
        assert self.redis.exists("latest_upload/42/abc123") == 0
        assert self.redis.exists("intermediate-report/42/abc123/coverage") == 1

    def test_post_with_unknown_family_is_refused(self):
        superuser = UserFactory(is_staff=True, is_superuser=True)
        self.client.force_login(superuser)
        self._populate_repo_42()

        response = self.client.post(
            self.URL,
            {
                "repoid": "42",
                "commitid": "",
                "family": ["nope"],
                "typed_confirm": "42",
                "action": "confirm",
            },
        )

        assert response.status_code == 200
        body = response.content.decode("utf-8")
        assert "unknown or non-deletable family" in body
        # Sanity: nothing was cleared.
        assert self.redis.exists("uploads/42/abc123") == 1

    def test_post_with_lock_family_is_refused(self):
        superuser = UserFactory(is_staff=True, is_superuser=True)
        self.client.force_login(superuser)
        self._populate_repo_42()

        # `upload_finisher_gate` is a lock family — it's a real family
        # name in the registry but `is_deletable=False`, so it must be
        # rejected by the `_deletable_family_names` allowlist.
        response = self.client.post(
            self.URL,
            {
                "repoid": "42",
                "commitid": "",
                "family": ["upload_finisher_gate"],
                "typed_confirm": "42",
                "action": "confirm",
            },
        )

        assert response.status_code == 200
        body = response.content.decode("utf-8")
        assert "unknown or non-deletable family" in body
        # Lock key still in Redis.
        assert self.redis.exists("upload_finisher_gate_42_abc123") == 1

    def test_form_renders_a_checkbox_for_each_deletable_family(self):
        superuser = UserFactory(is_staff=True, is_superuser=True)
        self.client.force_login(superuser)

        response = self.client.get(self.URL)

        assert response.status_code == 200
        body = response.content.decode("utf-8")
        # Every deletable queue family gets a checkbox; locks do not.
        for name in (
            "uploads",
            "ta_flake_key",
            "latest_upload",
            "upload-processing-state",
            "intermediate-report",
            "celery_broker",
        ):
            assert f'value="{name}"' in body
        # Locks must not appear in the family checkbox list.
        assert 'value="upload_finisher_gate"' not in body
        assert 'value="coordination_lock"' not in body
