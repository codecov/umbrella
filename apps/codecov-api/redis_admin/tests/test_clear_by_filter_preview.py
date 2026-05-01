"""Coverage for the lazy clear-by-filter preview machinery.

The previous synchronous GET path called `streaming_celery_count`
+ materialised up to `CELERY_BROKER_DISPLAY_LIMIT` rows before
painting anything; on a 1M-deep queue that's tens of seconds of
blocking work in the gunicorn request thread. This file pins the
four-tier replacement:

* **Tier 1**: lazy preview — GET handler renders the form +
  skeleton in <200ms, no Redis scans.
* **Tier 2**: background count job for queues above
  `CLEAR_BY_FILTER_PREVIEW_INLINE_LIMIT`.
* **Tier 3**: 60s synchronous-mode count cache with `?bust=1`
  override.
* **Tier 4**: skip-count escape hatch.

All tests run under `-m 'not django_db'`. The audit-log
integration for the job-mode dry-run lives in
`test_celery_broker_clear_job.py` and is exercised by CI's
`@pytest.mark.django_db` job.
"""

from __future__ import annotations

import json
import uuid
from types import SimpleNamespace
from unittest import mock

import fakeredis
import pytest
from django.contrib.admin.sites import AdminSite
from django.contrib.messages.storage.fallback import FallbackStorage
from django.core.exceptions import PermissionDenied
from django.test import RequestFactory

from redis_admin import conn as redis_admin_conn
from redis_admin import services as redis_admin_services
from redis_admin import settings as redis_admin_settings
from redis_admin.admin import CeleryBrokerQueueAdmin
from redis_admin.models import CeleryBrokerQueue
from redis_admin.tests.test_celery_broker_clear_job import (
    _push_envelopes,
    _wait_for_job,
)


@pytest.fixture
def patched_broker(monkeypatch) -> fakeredis.FakeStrictRedis:
    """Single fakeredis backing both the cache (`kind="default"`,
    where the count cache + job hash live) and the broker
    (`kind="broker"`). Mirrors the fixture in
    `test_celery_broker_clear_job.py` so push + count cohabit
    one in-memory server.
    """

    server = fakeredis.FakeStrictRedis()
    monkeypatch.setattr(
        redis_admin_conn, "get_connection", lambda kind="default": server
    )
    return server


@pytest.fixture
def superuser():
    """A `SimpleNamespace` superuser stand-in. The view-layer
    permission checks read `is_superuser`; the audit-log writes
    only need `id` / `pk`.
    """

    return SimpleNamespace(
        id=42,
        pk=42,
        is_superuser=True,
        is_staff=True,
        is_active=True,
        is_authenticated=True,
        is_anonymous=False,
        username="testop",
    )


@pytest.fixture
def staff_only_user():
    """A staff user without superuser. Used to pin the 403 gate."""

    return SimpleNamespace(
        id=43,
        pk=43,
        is_superuser=False,
        is_staff=True,
        is_active=True,
        is_authenticated=True,
        is_anonymous=False,
        username="staffop",
    )


def _admin_instance() -> CeleryBrokerQueueAdmin:
    return CeleryBrokerQueueAdmin(model=CeleryBrokerQueue, admin_site=AdminSite())


def _attach_messages(request) -> None:
    """Pin the standard `RequestFactory` boilerplate for views that
    call `messages.error(...)` or `messages.info(...)` so the bare
    request doesn't blow up on a missing storage backend.
    """

    request.session = {}
    request._messages = FallbackStorage(request)


# ---- Tier 1: lazy preview --------------------------------------------------


def test_preview_endpoint_returns_synchronous_count_for_small_queue(
    patched_broker, superuser
):
    """Below the inline-limit threshold the endpoint computes the
    count synchronously and returns the sample inline. Pins the
    JSON response shape the JS client unmarshals (`mode`,
    `match_count`, `kept_index`, `sample`).
    """

    queue = "notify"
    _push_envelopes(patched_broker, queue, n=100, repoid=42)

    rf = RequestFactory()
    request = rf.get(
        "/admin/redis_admin/celerybrokerqueue/clear-by-filter/preview/",
        data={"queue_name": queue, "repoid": "42"},
    )
    request.user = superuser

    admin_instance = _admin_instance()
    response = admin_instance.clear_by_filter_preview_view(request)

    assert response.status_code == 200
    payload = json.loads(response.content)
    assert payload["mode"] == "synchronous"
    assert payload["match_count"] == 100
    # _CLEAR_BY_FILTER_SAMPLE_SIZE is 20 in admin.py; pin via the
    # fact that we pushed 100 messages and the table caps at 20.
    assert len(payload["sample"]) == 20
    sample_row = payload["sample"][0]
    assert sample_row["repoid"] == 42
    assert "task_name" in sample_row
    assert "index_in_queue" in sample_row
    # `kept_index` is the lowest index_in_queue match; for our push
    # pattern that's 0 (the first message we pushed).
    assert payload["kept_index"] == 0


def test_preview_endpoint_uses_cache_on_second_call(patched_broker, superuser):
    """Tier 3: the second call returns the cached count instead of
    re-walking the queue. Asserted by counting calls into the
    monkeypatched `streaming_celery_count`.
    """

    queue = "notify"
    _push_envelopes(patched_broker, queue, n=10, repoid=1)

    rf = RequestFactory()
    admin_instance = _admin_instance()

    call_counter = {"n": 0}
    real_count = redis_admin_services.streaming_celery_count

    def _counting_count(*args, **kwargs):
        call_counter["n"] += 1
        return real_count(*args, **kwargs)

    with mock.patch.object(
        redis_admin_services, "streaming_celery_count", _counting_count
    ):
        for _ in range(2):
            request = rf.get(
                "/admin/redis_admin/celerybrokerqueue/clear-by-filter/preview/",
                data={"queue_name": queue, "repoid": "1"},
            )
            request.user = superuser
            response = admin_instance.clear_by_filter_preview_view(request)
            assert response.status_code == 200

    # First call walks the queue; second call hits the cache. If
    # this drifts (someone disables the cache or moves it out of
    # the orchestrator), the assertion shouts about it.
    assert call_counter["n"] == 1
    payload = json.loads(response.content)
    assert payload["match_count"] == 10
    # Second call advertises that it came from cache so the JS can
    # surface "Cached at …" in the UI.
    assert "cached_at" in payload


def test_preview_endpoint_bust_param_skips_cache(patched_broker, superuser):
    """Tier 3: `?bust=1` forces a recompute. Pins the manual
    refresh-count button against future cache-layer refactors.
    """

    queue = "notify"
    _push_envelopes(patched_broker, queue, n=5, repoid=1)

    rf = RequestFactory()
    admin_instance = _admin_instance()

    call_counter = {"n": 0}
    real_count = redis_admin_services.streaming_celery_count

    def _counting_count(*args, **kwargs):
        call_counter["n"] += 1
        return real_count(*args, **kwargs)

    with mock.patch.object(
        redis_admin_services, "streaming_celery_count", _counting_count
    ):
        # First call populates the cache.
        request = rf.get(
            "/admin/redis_admin/celerybrokerqueue/clear-by-filter/preview/",
            data={"queue_name": queue, "repoid": "1"},
        )
        request.user = superuser
        admin_instance.clear_by_filter_preview_view(request)
        assert call_counter["n"] == 1

        # Bust forces a recompute even though the cache is warm.
        request = rf.get(
            "/admin/redis_admin/celerybrokerqueue/clear-by-filter/preview/",
            data={"queue_name": queue, "repoid": "1", "bust": "1"},
        )
        request.user = superuser
        admin_instance.clear_by_filter_preview_view(request)
        assert call_counter["n"] == 2


def test_preview_endpoint_returns_job_mode_for_deep_queue(
    patched_broker, superuser, monkeypatch
):
    """Tier 2: above the inline-limit threshold the endpoint hands
    the count off to a `dry_run=True` chunked job and returns
    `{mode: "job", job_id, status_url}` instead. We lower the
    threshold under test rather than push 200_000 envelopes (which
    would be slow even on fakeredis).
    """

    queue = "notify"
    _push_envelopes(patched_broker, queue, n=50, repoid=1)

    monkeypatch.setattr(
        redis_admin_settings, "CLEAR_BY_FILTER_PREVIEW_INLINE_LIMIT", 10
    )

    rf = RequestFactory()
    request = rf.get(
        "/admin/redis_admin/celerybrokerqueue/clear-by-filter/preview/",
        data={"queue_name": queue, "repoid": "1"},
    )
    request.user = superuser

    admin_instance = _admin_instance()
    response = admin_instance.clear_by_filter_preview_view(request)

    assert response.status_code == 200
    payload = json.loads(response.content)
    assert payload["mode"] == "job"
    # job_id is a UUID string (start_celery_broker_clear_job uses
    # `str(uuid.uuid4())`); parse round-trips assert that.
    parsed = uuid.UUID(payload["job_id"])
    assert str(parsed) == payload["job_id"]
    assert payload["status_url"].endswith(f"/job/{payload['job_id']}/status/")
    # Sample still rendered synchronously even in job mode (LRANGE
    # 0 N is fast); operator gets a head-of-queue preview to scan
    # while the count job runs.
    assert isinstance(payload["sample"], list)
    # Wait for the worker thread to drain so subsequent tests don't
    # see a leaked daemon thread.
    _wait_for_job(payload["job_id"], timeout=10.0)


def test_preview_endpoint_inline_limit_setting_overrides_threshold(
    patched_broker, superuser, monkeypatch
):
    """Tier 2: the threshold is overridable through the Django
    setting `REDIS_ADMIN_CLEAR_BY_FILTER_PREVIEW_INLINE_LIMIT`.
    We pin the same `n=20` push to flip from synchronous to job
    mode when the threshold drops below the queue depth.
    """

    queue = "notify"
    _push_envelopes(patched_broker, queue, n=20, repoid=1)

    rf = RequestFactory()
    admin_instance = _admin_instance()

    # With the default high threshold (200_000) → synchronous.
    request = rf.get(
        "/admin/redis_admin/celerybrokerqueue/clear-by-filter/preview/",
        data={"queue_name": queue, "repoid": "1"},
    )
    request.user = superuser
    response = admin_instance.clear_by_filter_preview_view(request)
    assert json.loads(response.content)["mode"] == "synchronous"

    # Lower the threshold below the queue depth → job mode.
    monkeypatch.setattr(redis_admin_settings, "CLEAR_BY_FILTER_PREVIEW_INLINE_LIMIT", 5)
    request = rf.get(
        "/admin/redis_admin/celerybrokerqueue/clear-by-filter/preview/",
        data={"queue_name": queue, "repoid": "1"},
    )
    request.user = superuser
    response = admin_instance.clear_by_filter_preview_view(request)
    payload = json.loads(response.content)
    assert payload["mode"] == "job"
    _wait_for_job(payload["job_id"], timeout=10.0)


def test_preview_endpoint_validates_required_filters(patched_broker, superuser):
    """Empty `queue_name` and "queue_name only with no narrowing
    filter" both 400 with a JSON error body so the JS surfaces
    the inline error pill rather than following a redirect.
    """

    rf = RequestFactory()
    admin_instance = _admin_instance()

    # Empty queue_name.
    request = rf.get("/admin/redis_admin/celerybrokerqueue/clear-by-filter/preview/")
    request.user = superuser
    _attach_messages(request)
    response = admin_instance.clear_by_filter_preview_view(request)
    assert response.status_code == 400
    payload = json.loads(response.content)
    assert payload["error"] == "invalid_filter"
    assert "queue_name" in payload["detail"]

    # queue_name set, but no task / repoid / commitid → still 400.
    request = rf.get(
        "/admin/redis_admin/celerybrokerqueue/clear-by-filter/preview/",
        data={"queue_name": "notify"},
    )
    request.user = superuser
    _attach_messages(request)
    response = admin_instance.clear_by_filter_preview_view(request)
    assert response.status_code == 400
    payload = json.loads(response.content)
    assert payload["error"] == "invalid_filter"
    assert (
        "task_name" in payload["detail"]
        or "repoid" in payload["detail"]
        or "commitid" in payload["detail"]
    )


def test_preview_endpoint_requires_superuser(
    patched_broker, superuser, staff_only_user
):
    """Anonymous + staff-but-not-superuser hit the same `PermissionDenied`
    as `clear_by_filter_view`. Pins the gate against a future
    refactor that splits permissions per view.
    """

    rf = RequestFactory()
    admin_instance = _admin_instance()

    # Staff-only user (is_superuser=False) is refused.
    request = rf.get(
        "/admin/redis_admin/celerybrokerqueue/clear-by-filter/preview/",
        data={"queue_name": "notify", "repoid": "1"},
    )
    request.user = staff_only_user
    with pytest.raises(PermissionDenied):
        admin_instance.clear_by_filter_preview_view(request)

    # Anonymous user is refused.
    request = rf.get(
        "/admin/redis_admin/celerybrokerqueue/clear-by-filter/preview/",
        data={"queue_name": "notify", "repoid": "1"},
    )
    request.user = SimpleNamespace(
        is_superuser=False,
        is_staff=False,
        is_active=False,
        is_authenticated=False,
    )
    with pytest.raises(PermissionDenied):
        admin_instance.clear_by_filter_preview_view(request)

    # Superuser sails through.
    _push_envelopes(patched_broker, "notify", n=2, repoid=1)
    request = rf.get(
        "/admin/redis_admin/celerybrokerqueue/clear-by-filter/preview/",
        data={"queue_name": "notify", "repoid": "1"},
    )
    request.user = superuser
    response = admin_instance.clear_by_filter_preview_view(request)
    assert response.status_code == 200


def test_clear_by_filter_get_renders_skeleton_without_redis_scans(
    patched_broker, superuser
):
    """Tier 1 invariant: the GET handler renders a 200 even when
    every Redis-touching helper raises. Pins that the slimmed
    handler no longer calls `streaming_celery_count` or
    materialises the queryset on the GET path.
    """

    rf = RequestFactory()
    admin_instance = _admin_instance()

    def _boom(*args, **kwargs):
        raise AssertionError("GET handler should not call streaming_celery_count")

    def _boom_queryset(*args, **kwargs):
        raise AssertionError("GET handler should not materialise the queryset")

    request = rf.get(
        "/admin/redis_admin/celerybrokerqueue/clear-by-filter/",
        data={"queue_name": "notify", "repoid": "1"},
    )
    request.user = superuser
    _attach_messages(request)

    with (
        mock.patch("redis_admin.admin.streaming_celery_count", _boom),
        mock.patch.object(
            CeleryBrokerQueueAdmin, "_materialise_sample_targets", _boom_queryset
        ),
    ):
        response = admin_instance.clear_by_filter_view(request)

    assert response.status_code == 200
    body = response.content.decode()
    # Skeleton placeholders + data attributes survive the slim
    # render path.
    assert "celery-clear-by-filter-preview-root" in body
    assert "data-preview-url" in body
    assert "celery-clear-preview-loader" in body
    # Submit buttons start disabled until the JS unlocks them.
    assert "disabled" in body


# ---- Tier 4: skip-count escape hatch ---------------------------------------


def test_skip_count_view_redirects_to_clear_form_with_count_skipped_flag(
    patched_broker, superuser
):
    """The skip-count link 302s back to `clear_by_filter_view`
    with `count_skipped=1` set so the GET handler renders the
    form without the count card and with the destructive submit
    buttons unlocked.
    """

    rf = RequestFactory()
    admin_instance = _admin_instance()

    request = rf.get(
        "/admin/redis_admin/celerybrokerqueue/clear-by-filter/skip-count/",
        data={"queue_name": "notify", "repoid": "1"},
    )
    request.user = superuser
    _attach_messages(request)
    response = admin_instance.clear_by_filter_skip_count_view(request)

    assert response.status_code == 302
    location = response["Location"]
    assert "clear-by-filter/" in location
    assert "count_skipped=1" in location
    assert "queue_name=notify" in location
    assert "repoid=1" in location


def test_skip_count_view_validates_filters(patched_broker, superuser):
    """Skip-count re-validates the same filter combo. A hand-
    crafted skip-count URL without a narrowing filter still
    bounces the operator back to the changelist with a flash.
    """

    rf = RequestFactory()
    admin_instance = _admin_instance()

    request = rf.get(
        "/admin/redis_admin/celerybrokerqueue/clear-by-filter/skip-count/",
        data={"queue_name": "notify"},  # no narrowing filter
    )
    request.user = superuser
    _attach_messages(request)
    response = admin_instance.clear_by_filter_skip_count_view(request)
    # Validation flash → redirect to the changelist (the pattern
    # used by `clear_by_filter_view`).
    assert response.status_code == 302
    assert "count_skipped=1" not in response["Location"]


def test_clear_by_filter_get_with_count_skipped_renders_form_without_loader(
    patched_broker, superuser
):
    """When `count_skipped=1` is set the GET handler suppresses
    the loader card + count card and unlocks the submit buttons
    from page-load. Pins the Tier 4 render shape.
    """

    rf = RequestFactory()
    admin_instance = _admin_instance()

    request = rf.get(
        "/admin/redis_admin/celerybrokerqueue/clear-by-filter/",
        data={
            "queue_name": "notify",
            "repoid": "1",
            "count_skipped": "1",
        },
    )
    request.user = superuser
    _attach_messages(request)

    response = admin_instance.clear_by_filter_view(request)
    assert response.status_code == 200
    body = response.content.decode()
    # Loader card is suppressed (the {% if not count_skipped %}
    # block doesn't emit it).
    assert "Counting matches…" not in body
    # The skip-count flag round-trips on the form so a typed-
    # confirmation error doesn't snap the loader back into view.
    assert 'name="count_skipped"' in body
    # Submit buttons are NOT disabled in the count-skipped render.
    assert " disabled>" not in body and " disabled " not in body


# ---- Filter validation parity ---------------------------------------------


def test_preview_endpoint_repoid_must_be_integer(patched_broker, superuser):
    """`repoid` validation matches `clear_by_filter_view` — non-
    integer input → 400. Lifts a parity invariant out of the
    shared `_parse_clear_by_filter_params` helper so a future
    rewrite that divorces them gets a loud test failure.
    """

    rf = RequestFactory()
    admin_instance = _admin_instance()

    request = rf.get(
        "/admin/redis_admin/celerybrokerqueue/clear-by-filter/preview/",
        data={"queue_name": "notify", "repoid": "not-a-number"},
    )
    request.user = superuser
    _attach_messages(request)
    response = admin_instance.clear_by_filter_preview_view(request)
    assert response.status_code == 400
    payload = json.loads(response.content)
    assert payload["error"] == "invalid_filter"
    assert "repoid" in payload["detail"]
