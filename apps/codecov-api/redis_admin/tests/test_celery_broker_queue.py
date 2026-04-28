"""End-to-end coverage for the M6 `CeleryBrokerQueue` drill-down.

Covers the four moving pieces:

* envelope parser (`parse_celery_envelope`) — pulls structured fields
  out of a kombu broker payload, including the `task_id` / `ownerid`
  / `pullid` extensions added in M6 on top of the existing
  `(task, repoid, commitid)` shape.
* `_summarise_kwargs_for_preview` — keeps the per-message preview
  under `MAX_DECODE_BYTES` even when a kwarg like `commit_yaml`
  ships a multi-megabyte payload.
* `CeleryBrokerQueueQuerySet` — filtering by repoid / commitid /
  task / etc.
* `services.celery_broker_clear` — LSET-tombstone clear path,
  including a concurrent-consumer race simulation and the audit
  log.

Plus an admin smoke test that the changelist refuses to fan out
without a `queue_name__exact` and that the celery_broker `view
items` link routes here instead of the generic `RedisQueueItem`.
"""

from __future__ import annotations

import base64
import json
from typing import Any

import fakeredis
import pytest
from django.contrib.admin.models import LogEntry
from django.test import TestCase

from redis_admin import conn as redis_admin_conn
from redis_admin import services as redis_admin_services
from redis_admin.families import parse_celery_envelope
from redis_admin.models import CeleryBrokerQueue, RedisQueue
from redis_admin.queryset import (
    CeleryBrokerQueueQuerySet,
    _summarise_kwargs_for_preview,
)
from redis_admin.services import (
    _CELERY_TOMBSTONE_PREFIX,
    celery_broker_clear,
)
from shared.django_apps.codecov_auth.tests.factories import UserFactory
from utils.test_utils import Client

# ---- helpers ---------------------------------------------------------------


def _build_envelope(
    *,
    task: str | None = "app.tasks.notify.NotifyTask",
    task_id: str | None = "task-uuid-1",
    args: list | None = None,
    kwargs: dict[str, Any] | None = None,
) -> str:
    """Build a kombu broker envelope JSON string suitable for LRANGE."""

    body = json.dumps([args or [], kwargs or {}, {}])
    body_b64 = base64.b64encode(body.encode("utf-8")).decode("ascii")
    headers: dict[str, Any] = {}
    if task is not None:
        headers["task"] = task
    if task_id is not None:
        headers["id"] = task_id
    return json.dumps({"body": body_b64, "headers": headers})


@pytest.fixture
def patched_broker(monkeypatch) -> fakeredis.FakeStrictRedis:
    """Route both the cache and broker get_connection to one fakeredis."""

    server = fakeredis.FakeStrictRedis()
    monkeypatch.setattr(
        redis_admin_conn, "get_connection", lambda kind="default": server
    )
    return server


# ---- envelope parser -------------------------------------------------------


def test_parse_envelope_extracts_task_repoid_commit_from_kwargs():
    envelope = _build_envelope(
        task="app.tasks.notify.NotifyTask",
        task_id="abc-uuid",
        kwargs={
            "repoid": 21222368,
            "commitid": "0832c110a744ddb8185bfdf0524aad41d3c3d21a",
        },
    )

    meta = parse_celery_envelope(envelope)

    assert meta.task == "app.tasks.notify.NotifyTask"
    assert meta.task_id == "abc-uuid"
    assert meta.repoid == 21222368
    assert meta.commitid == "0832c110a744ddb8185bfdf0524aad41d3c3d21a"
    assert meta.ownerid is None
    assert meta.pullid is None
    assert meta.kwargs == {
        "repoid": 21222368,
        "commitid": "0832c110a744ddb8185bfdf0524aad41d3c3d21a",
    }


def test_parse_envelope_extracts_ownerid_and_pullid():
    envelope = _build_envelope(
        task="app.tasks.sync_pull.PullSyncTask",
        kwargs={"repoid": 7, "pullid": 42},
    )

    meta = parse_celery_envelope(envelope)

    assert meta.task == "app.tasks.sync_pull.PullSyncTask"
    assert meta.repoid == 7
    assert meta.pullid == 42
    assert meta.commitid is None
    assert meta.ownerid is None


def test_parse_envelope_owner_only_task():
    envelope = _build_envelope(
        task="app.tasks.sync_repos.SyncRepos",
        kwargs={"ownerid": 99, "username": "octocat"},
    )

    meta = parse_celery_envelope(envelope)

    assert meta.ownerid == 99
    assert meta.repoid is None
    assert meta.pullid is None


def test_parse_envelope_handles_string_int_repoid():
    """JSON ints from celery sometimes round-trip as strings."""

    envelope = _build_envelope(kwargs={"repoid": "1234", "commitid": "abc"})

    meta = parse_celery_envelope(envelope)

    assert meta.repoid == 1234


def test_parse_envelope_rejects_bool_in_int_fields():
    """`bool` subclasses `int`; refuse to coerce True/False to 1/0."""

    envelope = _build_envelope(kwargs={"repoid": True})

    meta = parse_celery_envelope(envelope)

    assert meta.repoid is None


def test_parse_envelope_returns_empty_for_garbled_json():
    meta = parse_celery_envelope("not-json {")

    assert meta.task is None
    assert meta.repoid is None
    assert meta.commitid is None
    assert meta.kwargs is None


def test_parse_envelope_returns_empty_for_missing_headers():
    envelope = json.dumps({"body": ""})

    meta = parse_celery_envelope(envelope)

    assert meta.task is None
    assert meta.repoid is None


def test_parse_envelope_ignores_non_dict_kwargs():
    """Some tasks pass positional-only args; `body[1]` is a list."""

    body = json.dumps([[1, 2, 3], [], {}])
    body_b64 = base64.b64encode(body.encode("utf-8")).decode("ascii")
    envelope = json.dumps({"body": body_b64, "headers": {"task": "t"}})

    meta = parse_celery_envelope(envelope)

    assert meta.task == "t"
    assert meta.repoid is None
    assert meta.kwargs is None


# ---- payload preview -------------------------------------------------------


def test_payload_preview_truncates_known_large_keys():
    big_yaml = {"codecov": {"foo": "x" * 5000}}
    preview = _summarise_kwargs_for_preview({"repoid": 1, "commit_yaml": big_yaml})

    assert "<truncated:" in preview
    assert "5000" not in preview or "<truncated:" in preview
    assert '"repoid": 1' in preview
    # Even with a 5KB kwarg, the rendered preview stays bounded.
    assert len(preview) < 1000


def test_payload_preview_truncates_oversize_unknown_keys():
    """Unknown kwargs above the per-value cap fall back to a `… (+N chars)` marker."""

    long_value = "x" * 1024
    preview = _summarise_kwargs_for_preview({"weird_blob": long_value})

    # JSON-rendered, the ellipsis is escaped as `\u2026` rather than
    # the literal codepoint, so check for the pattern that follows it.
    assert "(+" in preview
    assert "chars)" in preview
    assert "weird_blob" in preview


def test_payload_preview_handles_empty_kwargs():
    assert _summarise_kwargs_for_preview(None) == ""
    assert _summarise_kwargs_for_preview({}) == ""


# ---- queryset filtering ----------------------------------------------------


def _push(redis, queue, **kwargs):
    """Push one envelope into `queue` and return the raw value."""

    raw = _build_envelope(**kwargs)
    redis.rpush(queue, raw)
    return raw


def test_queryset_requires_queue_name_to_yield_rows(patched_broker):
    _push(patched_broker, "notify", kwargs={"repoid": 1})

    qs = CeleryBrokerQueueQuerySet(CeleryBrokerQueue)

    assert list(qs) == []
    assert qs.count() == 0


def test_queryset_yields_one_row_per_message(patched_broker):
    _push(patched_broker, "notify", task="t1", kwargs={"repoid": 1, "commitid": "a"})
    _push(patched_broker, "notify", task="t2", kwargs={"repoid": 2, "commitid": "b"})

    rows = list(CeleryBrokerQueueQuerySet(CeleryBrokerQueue, queue_name="notify"))

    assert [r.index_in_queue for r in rows] == [0, 1]
    assert [r.task_name for r in rows] == ["t1", "t2"]
    assert [r.repoid for r in rows] == [1, 2]
    assert [r.pk_token for r in rows] == ["notify#0", "notify#1"]


def test_queryset_filter_by_repoid(patched_broker):
    _push(patched_broker, "notify", kwargs={"repoid": 1, "commitid": "a"})
    _push(patched_broker, "notify", kwargs={"repoid": 2, "commitid": "b"})
    _push(patched_broker, "notify", kwargs={"repoid": 1, "commitid": "c"})

    qs = CeleryBrokerQueueQuerySet(CeleryBrokerQueue, queue_name="notify").filter(
        repoid__exact=1
    )

    rows = list(qs)
    assert {r.commitid for r in rows} == {"a", "c"}
    assert all(r.repoid == 1 for r in rows)


def test_queryset_filter_by_commitid_startswith_matches_short_or_full_sha(
    patched_broker,
):
    _push(
        patched_broker,
        "notify",
        kwargs={"repoid": 1, "commitid": "0832c110a744ddb8185bfdf0524aad41d3c3d21a"},
    )
    _push(patched_broker, "notify", kwargs={"repoid": 1, "commitid": "deadbeef0"})

    short_hits = list(
        CeleryBrokerQueueQuerySet(CeleryBrokerQueue, queue_name="notify").filter(
            commitid__startswith="0832c11"
        )
    )
    full_hits = list(
        CeleryBrokerQueueQuerySet(CeleryBrokerQueue, queue_name="notify").filter(
            commitid__startswith="0832c110a744ddb8185bfdf0524aad41d3c3d21a"
        )
    )

    assert len(short_hits) == 1
    assert len(full_hits) == 1
    assert short_hits[0].commitid == full_hits[0].commitid


def test_queryset_commitid_exact_does_not_match_prefix(patched_broker):
    _push(patched_broker, "notify", kwargs={"repoid": 1, "commitid": "abcdef0"})
    _push(patched_broker, "notify", kwargs={"repoid": 1, "commitid": "abc"})

    rows = list(
        CeleryBrokerQueueQuerySet(CeleryBrokerQueue, queue_name="notify").filter(
            commitid__exact="abc"
        )
    )

    assert {r.commitid for r in rows} == {"abc"}


def test_queryset_filter_by_task_name_icontains(patched_broker):
    _push(patched_broker, "bundle_analysis", task="app.tasks.bundle_analysis.A")
    _push(patched_broker, "bundle_analysis", task="app.tasks.notify.B")

    rows = list(
        CeleryBrokerQueueQuerySet(
            CeleryBrokerQueue, queue_name="bundle_analysis"
        ).filter(task_name__icontains="bundle")
    )

    assert {r.task_name for r in rows} == {"app.tasks.bundle_analysis.A"}


def test_queryset_filter_by_task_id_exact(patched_broker):
    _push(patched_broker, "notify", task_id="aaaa")
    _push(patched_broker, "notify", task_id="bbbb")

    rows = list(
        CeleryBrokerQueueQuerySet(CeleryBrokerQueue, queue_name="notify").filter(
            task_id__exact="bbbb"
        )
    )

    assert [r.task_id for r in rows] == ["bbbb"]


def test_queryset_combined_filters(patched_broker):
    _push(
        patched_broker,
        "notify",
        task="app.tasks.notify.NotifyTask",
        kwargs={"repoid": 1, "commitid": "abc"},
    )
    _push(
        patched_broker,
        "notify",
        task="app.tasks.upload.UploadTask",
        kwargs={"repoid": 1, "commitid": "abc"},
    )
    _push(
        patched_broker,
        "notify",
        task="app.tasks.notify.NotifyTask",
        kwargs={"repoid": 2, "commitid": "abc"},
    )

    rows = list(
        CeleryBrokerQueueQuerySet(CeleryBrokerQueue, queue_name="notify").filter(
            repoid=1, task_name__icontains="notify"
        )
    )

    assert len(rows) == 1
    assert rows[0].repoid == 1
    assert "Notify" in (rows[0].task_name or "")


def test_queryset_filter_pk_in_resolves_indexes(patched_broker):
    _push(patched_broker, "notify", kwargs={"repoid": 1})
    _push(patched_broker, "notify", kwargs={"repoid": 2})
    _push(patched_broker, "notify", kwargs={"repoid": 3})

    qs = CeleryBrokerQueueQuerySet(CeleryBrokerQueue).filter(
        pk__in=["notify#0", "notify#2"]
    )
    rows = list(qs)

    assert {r.index_in_queue for r in rows} == {0, 2}


def test_queryset_filter_pk_in_refuses_cross_queue_selection(patched_broker):
    _push(patched_broker, "notify", kwargs={"repoid": 1})
    _push(patched_broker, "uploads", kwargs={"repoid": 1})

    qs = CeleryBrokerQueueQuerySet(CeleryBrokerQueue).filter(
        pk__in=["notify#0", "uploads#0"]
    )

    assert list(qs) == []


def test_queryset_filter_unknown_kwarg_raises(patched_broker):
    _push(patched_broker, "notify", kwargs={"repoid": 1})

    qs = CeleryBrokerQueueQuerySet(CeleryBrokerQueue, queue_name="notify")

    with pytest.raises(NotImplementedError):
        qs.filter(some_random_field="x")


def test_queryset_get_by_pk_token(patched_broker):
    _push(patched_broker, "notify", kwargs={"repoid": 1, "commitid": "first"})
    _push(patched_broker, "notify", kwargs={"repoid": 2, "commitid": "second"})

    row = CeleryBrokerQueueQuerySet(CeleryBrokerQueue).get(pk="notify#1")

    assert row.commitid == "second"
    assert row.index_in_queue == 1


def test_queryset_get_raises_for_missing_index(patched_broker):
    _push(patched_broker, "notify", kwargs={"repoid": 1})

    with pytest.raises(CeleryBrokerQueue.DoesNotExist):
        CeleryBrokerQueueQuerySet(CeleryBrokerQueue).get(pk="notify#99")


def test_queryset_ordering_by_repoid(patched_broker):
    _push(patched_broker, "notify", kwargs={"repoid": 3})
    _push(patched_broker, "notify", kwargs={"repoid": 1})
    _push(patched_broker, "notify", kwargs={"repoid": 2})

    rows = list(
        CeleryBrokerQueueQuerySet(CeleryBrokerQueue, queue_name="notify").order_by(
            "repoid"
        )
    )

    assert [r.repoid for r in rows] == [1, 2, 3]


# ---- service: celery_broker_clear ------------------------------------------


@pytest.mark.django_db
def test_celery_broker_clear_dry_run_does_not_mutate(patched_broker):
    _push(patched_broker, "notify", kwargs={"repoid": 1})
    _push(patched_broker, "notify", kwargs={"repoid": 2})
    user = UserFactory()

    qs = CeleryBrokerQueueQuerySet(CeleryBrokerQueue, queue_name="notify").filter(
        repoid=1
    )

    result = celery_broker_clear(list(qs), user=user, dry_run=True)

    assert result.dry_run is True
    assert result.count == 1
    assert patched_broker.llen("notify") == 2


@pytest.mark.django_db
def test_celery_broker_clear_removes_only_matching_messages(patched_broker):
    _push(patched_broker, "notify", kwargs={"repoid": 1, "commitid": "a"})
    _push(patched_broker, "notify", kwargs={"repoid": 2, "commitid": "b"})
    _push(patched_broker, "notify", kwargs={"repoid": 1, "commitid": "c"})
    user = UserFactory()

    targets = list(
        CeleryBrokerQueueQuerySet(CeleryBrokerQueue, queue_name="notify").filter(
            repoid=1
        )
    )

    result = celery_broker_clear(targets, user=user, dry_run=False)

    assert result.count == 2
    assert patched_broker.llen("notify") == 1
    survivor = json.loads(patched_broker.lrange("notify", 0, -1)[0].decode("utf-8"))
    body_decoded = json.loads(base64.b64decode(survivor["body"]).decode("utf-8"))
    assert body_decoded[1]["repoid"] == 2


@pytest.mark.django_db
def test_celery_broker_clear_writes_audit_log_entry(patched_broker):
    _push(patched_broker, "notify", kwargs={"repoid": 1})
    user = UserFactory()

    targets = list(CeleryBrokerQueueQuerySet(CeleryBrokerQueue, queue_name="notify"))

    celery_broker_clear(targets, user=user, dry_run=False)

    entries = list(LogEntry.objects.filter(user_id=user.id))
    assert len(entries) == 1
    payload = json.loads(entries[0].change_message)
    assert payload["scope"] == "celery_broker_clear"
    assert payload["count"] == 1
    assert payload["dry_run"] is False
    assert payload["families"] == ["celery_broker"]


@pytest.mark.django_db
def test_celery_broker_clear_handles_concurrent_consumer_pop(
    patched_broker, monkeypatch
):
    """Simulate a celery worker BLPOPing a message between LSET and LREM.

    The LSET-tombstone path must still:
      1. produce a consistent delete count (LREM reply),
      2. never resurrect the popped message,
      3. never accidentally delete other unrelated rows.
    """

    _push(patched_broker, "notify", task="t0", kwargs={"repoid": 0})
    _push(patched_broker, "notify", task="t1", kwargs={"repoid": 1})
    _push(patched_broker, "notify", task="t2", kwargs={"repoid": 2})
    user = UserFactory()

    targets = list(
        CeleryBrokerQueueQuerySet(CeleryBrokerQueue, queue_name="notify").filter(
            repoid=2
        )
    )
    assert len(targets) == 1

    real_execute_celery_clear = redis_admin_services._execute_celery_clear

    def racing_execute_celery_clear(redis, queue_name, indexes, *, tombstone):
        # Consumer drains the head right before we'd LSET. The
        # surviving list reads `[t1-msg, t2-msg]`, so what was
        # `targets[0].index_in_queue == 2` no longer points at the
        # t2 message — it now points past the end. The tombstone
        # write fails (LSET rejects out-of-range), and the LREM is
        # a no-op. The cleared count must be 0, not 1, and the
        # surviving list must not have lost t1.
        redis.lpop(queue_name)
        return real_execute_celery_clear(
            redis, queue_name, indexes, tombstone=tombstone
        )

    monkeypatch.setattr(
        redis_admin_services,
        "_execute_celery_clear",
        racing_execute_celery_clear,
    )

    result = celery_broker_clear(targets, user=user, dry_run=False)

    surviving_raw = patched_broker.lrange("notify", 0, -1)
    surviving_tasks = []
    for raw in surviving_raw:
        envelope = json.loads(raw.decode("utf-8"))
        surviving_tasks.append(envelope["headers"]["task"])

    # Consumer popped t0 (the head); we never touched t1 or t2.
    assert "t1" in surviving_tasks
    assert "t2" in surviving_tasks
    # No tombstone leaked into the surviving list.
    for raw in surviving_raw:
        assert _CELERY_TOMBSTONE_PREFIX.encode() not in raw
    # LREM on a missing tombstone returns 0; the result count
    # reflects what actually got removed in Redis.
    assert result.count == 0


@pytest.mark.django_db
def test_celery_broker_clear_uses_unique_tombstone_per_call(patched_broker):
    """Two simultaneous `celery_broker_clear` calls must not share a
    tombstone — otherwise one's LREM would wipe the other's pending
    LSETs.
    """

    user = UserFactory()
    _push(patched_broker, "notify", kwargs={"repoid": 1})

    rows = list(CeleryBrokerQueueQuerySet(CeleryBrokerQueue, queue_name="notify"))

    # Track every tombstone the service writes by intercepting LREM.
    tombstones: list[str] = []
    real_lrem = patched_broker.lrem

    def spying_lrem(name, count, value):
        tombstones.append(value.decode() if isinstance(value, bytes) else str(value))
        return real_lrem(name, count, value)

    patched_broker.lrem = spying_lrem  # type: ignore[assignment]

    celery_broker_clear(rows, user=user, dry_run=False)
    _push(patched_broker, "notify", kwargs={"repoid": 1})
    rows2 = list(CeleryBrokerQueueQuerySet(CeleryBrokerQueue, queue_name="notify"))
    celery_broker_clear(rows2, user=user, dry_run=False)

    assert len(tombstones) == 2
    assert tombstones[0] != tombstones[1]
    assert all(t.startswith(_CELERY_TOMBSTONE_PREFIX) for t in tombstones)


@pytest.mark.django_db
def test_celery_broker_clear_ignores_non_celery_targets(patched_broker):
    user = UserFactory()
    not_celery = RedisQueue(
        name="uploads/1/abc", family="uploads", redis_type="list", depth=1
    )

    result = celery_broker_clear([not_celery], user=user, dry_run=False)

    assert result.count == 0
    assert result.families == ()


# ---- admin smoke tests -----------------------------------------------------


class CeleryBrokerQueueAdminSmokeTest(TestCase):
    """Click-through coverage for the `CeleryBrokerQueueAdmin` changelist."""

    def setUp(self):
        self.redis = fakeredis.FakeStrictRedis()
        self._orig_get_connection = redis_admin_conn.get_connection
        redis_admin_conn.get_connection = lambda kind="default": self.redis  # type: ignore[assignment]
        self.user = UserFactory(is_staff=True, is_superuser=True)
        self.client = Client()
        self.client.force_login(self.user)

    def tearDown(self):
        redis_admin_conn.get_connection = self._orig_get_connection  # type: ignore[assignment]

    def _push(self, queue, **kwargs):
        self.redis.rpush(queue, _build_envelope(**kwargs))

    def test_changelist_requires_queue_name_filter(self):
        self._push("notify", kwargs={"repoid": 1})

        response = self.client.get("/admin/redis_admin/celerybrokerqueue/")

        assert response.status_code == 200
        body = response.content.decode("utf-8", errors="replace")
        # Without the queue filter we render the info nudge and zero rows.
        assert "Pick a celery_broker queue" in body

    def test_changelist_shows_messages_for_selected_queue(self):
        self._push(
            "notify",
            task="app.tasks.notify.NotifyTask",
            kwargs={"repoid": 1, "commitid": "abcdef0123"},
        )

        response = self.client.get(
            "/admin/redis_admin/celerybrokerqueue/?queue_name__exact=notify"
        )

        assert response.status_code == 200
        body = response.content.decode("utf-8", errors="replace")
        assert "app.tasks.notify.NotifyTask" in body
        assert "abcdef0" in body  # short commit rendered in the changelist

    def test_changelist_renders_task_and_repoid_columns(self):
        self._push(
            "notify",
            task="app.tasks.bundle_analysis.BundleAnalysisProcessor",
            kwargs={"repoid": 21222368, "commitid": "0832c110a744ddb"},
        )

        response = self.client.get(
            "/admin/redis_admin/celerybrokerqueue/?queue_name__exact=notify"
        )

        assert response.status_code == 200
        body = response.content.decode("utf-8", errors="replace")
        assert "BundleAnalysisProcessor" in body
        assert "21222368" in body
        # 7-char commit prefix appears in the rendered changelist column.
        assert "0832c11" in body

    def test_redisqueue_items_link_routes_celery_to_celery_admin(self):
        # The `celery_broker` family enumerates `_resolve_celery_queue_names()`
        # for its `fixed_keys`; in a minimal test env those resolve to
        # `("celery", "healthcheck")`, so push to one of those to be sure
        # the queue surfaces on the changelist.
        self.redis.rpush("celery", _build_envelope(kwargs={"repoid": 1}))

        response = self.client.get("/admin/redis_admin/redisqueue/")

        assert response.status_code == 200
        body = response.content.decode("utf-8", errors="replace")
        assert "celerybrokerqueue/" in body
        assert "queue_name__exact=celery" in body


@pytest.mark.django_db
def test_non_staff_user_cannot_view_celery_broker_admin():
    user = UserFactory(is_staff=False)
    client = Client()
    client.force_login(user)

    response = client.get("/admin/redis_admin/celerybrokerqueue/")

    assert response.status_code in (302, 403)
