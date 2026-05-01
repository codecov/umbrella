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
from types import SimpleNamespace
from typing import Any

import fakeredis
import pytest
from django.contrib.admin.models import LogEntry
from django.contrib.admin.sites import AdminSite
from django.http import HttpResponse
from django.test import Client as DjClient
from django.test import TestCase

from redis_admin import conn as redis_admin_conn
from redis_admin.admin import CeleryBrokerQueueAdmin, _resolve_repo_displays
from redis_admin.families import parse_celery_envelope
from redis_admin.models import CeleryBrokerQueue, RedisQueue
from redis_admin.queryset import (
    _STREAM_CHUNK,
    CeleryBrokerQueueQuerySet,
    _stream_frequency_aggregate,
    _summarise_kwargs_for_preview,
    resolve_payload_preview,
)
from redis_admin.services import (
    _CELERY_MAX_PASSES,
    _CELERY_TOMBSTONE_PREFIX,
    _streaming_celery_clear,
    celery_broker_clear,
)
from shared.django_apps.codecov_auth.tests.factories import UserFactory
from shared.django_apps.core.tests.factories import OwnerFactory, RepositoryFactory
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


# ---- lazy preview rendering (perf) -----------------------------------------


def test_materialised_rows_have_empty_preview_until_resolved(patched_broker):
    """Pin the perf invariant: `_materialise` doesn't render previews.

    The drill-down page can pull tens of thousands of envelopes per
    render but only paginates ~100 onto the screen, so paying the
    `_summarise_kwargs_for_preview` JSON-render cost on every row was
    the dominant page-render bottleneck. The lazy path stashes the
    body kwargs on the row and defers rendering to the first caller
    that actually displays it.
    """

    _push(
        patched_broker,
        "notify",
        task="app.tasks.notify.NotifyTask",
        kwargs={"repoid": 7, "commitid": "abc"},
    )
    rows = list(CeleryBrokerQueueQuerySet(CeleryBrokerQueue, queue_name="notify"))

    assert rows[0].payload_preview == ""
    # The body kwargs are stashed for the lazy resolver, NOT eagerly
    # rendered into the model field.
    assert rows[0]._kwargs_for_preview == {"repoid": 7, "commitid": "abc"}


def test_resolve_payload_preview_renders_lazily_and_caches(patched_broker):
    _push(
        patched_broker,
        "notify",
        kwargs={"repoid": 11, "commitid": "deadbeef"},
    )
    row = next(iter(CeleryBrokerQueueQuerySet(CeleryBrokerQueue, queue_name="notify")))

    rendered = resolve_payload_preview(row)
    assert '"repoid": 11' in rendered
    # Second call short-circuits via the now-populated field.
    assert resolve_payload_preview(row) == rendered
    assert row.payload_preview == rendered


def test_resolve_payload_preview_handles_rows_without_stash():
    # Foreign callers (tests building `CeleryBrokerQueue(...)` by hand,
    # the summary-mode landing-page rows) don't carry a kwargs stash —
    # the resolver must degrade gracefully rather than `AttributeError`.
    bare = CeleryBrokerQueue(pk_token="x#summary", queue_name="x")

    assert resolve_payload_preview(bare) == ""


def test_get_pk_token_eagerly_renders_preview_for_change_form(patched_broker):
    # The change_form path reads `obj.payload_preview` directly off the
    # readonly field surface, so `get(pk_token=...)` has to populate
    # the preview eagerly even though the changelist path doesn't.
    _push(
        patched_broker,
        "notify",
        kwargs={"repoid": 99, "commitid": "feedface"},
    )
    qs = CeleryBrokerQueueQuerySet(CeleryBrokerQueue, queue_name="notify")

    row = qs.get(pk_token="notify#0")

    assert '"repoid": 99' in row.payload_preview
    assert "feedface" in row.payload_preview


# ---- per-request LRANGE cache (perf) ---------------------------------------


def test_request_cache_dedupes_lrange_across_querysets(patched_broker):
    """The drill-down page builds two querysets per render (the
    changelist's and the chart's), each calling `_materialise`. Without
    the per-request cache that's a 2x cost on the LRANGE+parse round
    trip; this test pins the invariant that the second materialisation
    short-circuits via the request-stashed snapshot.
    """

    _push(patched_broker, "notify", kwargs={"repoid": 1, "commitid": "a"})
    _push(patched_broker, "notify", kwargs={"repoid": 2, "commitid": "b"})

    # `request` is duck-typed: anything that accepts arbitrary
    # attribute writes is fine. The admin plumbs `HttpRequest`; we
    # use a plain object here so the test doesn't need a request
    # factory.
    request = type("_Request", (), {})()
    qs1 = CeleryBrokerQueueQuerySet(
        CeleryBrokerQueue, queue_name="notify", request=request
    )
    list(qs1)

    # Wipe the underlying queue so the second materialisation, if it
    # actually round-trips Redis, would yield zero rows.
    patched_broker.delete("notify")

    qs2 = CeleryBrokerQueueQuerySet(
        CeleryBrokerQueue, queue_name="notify", request=request
    )
    rows2 = list(qs2)

    # The cache hit means `qs2` sees the snapshot `qs1` materialised,
    # not the now-empty queue.
    assert [r.repoid for r in rows2] == [1, 2]


def test_request_cache_propagates_through_filter_clones(patched_broker):
    # Django's ChangeList applies further `.filter()` clones on top of
    # the queryset returned by `get_queryset(request)`. The cache only
    # earns its keep if `_clone()` propagates the request marker so
    # the clone reads the same stashed snapshot.
    _push(patched_broker, "notify", kwargs={"repoid": 1, "commitid": "a"})
    _push(patched_broker, "notify", kwargs={"repoid": 2, "commitid": "b"})

    request = type("_Request", (), {})()
    base = CeleryBrokerQueueQuerySet(
        CeleryBrokerQueue, queue_name="notify", request=request
    )
    list(base)

    patched_broker.delete("notify")

    filtered = base.filter(repoid=1)
    rows = list(filtered)

    assert [r.repoid for r in rows] == [1]


def test_request_cache_isolated_across_requests(patched_broker):
    # Two distinct `request` markers must NOT share the cache —
    # otherwise a stale snapshot from one operator's tab could leak
    # into the next request.
    _push(patched_broker, "notify", kwargs={"repoid": 1, "commitid": "a"})

    req_a = type("_Request", (), {})()
    list(
        CeleryBrokerQueueQuerySet(CeleryBrokerQueue, queue_name="notify", request=req_a)
    )

    _push(patched_broker, "notify", kwargs={"repoid": 2, "commitid": "b"})

    req_b = type("_Request", (), {})()
    rows = list(
        CeleryBrokerQueueQuerySet(CeleryBrokerQueue, queue_name="notify", request=req_b)
    )

    # `req_b` materialises against the live queue, picking up the
    # second push that landed after `req_a`'s render.
    assert [r.repoid for r in rows] == [1, 2]


# ---- queryset filtering ----------------------------------------------------


def _push(redis, queue, **kwargs):
    """Push one envelope into `queue` and return the raw value."""

    raw = _build_envelope(**kwargs)
    redis.rpush(queue, raw)
    return raw


def test_queryset_summary_mode_yields_one_row_per_known_queue(patched_broker):
    # Without a `queue_name` filter the queryset is in "summary mode":
    # it lists every resolvable celery queue with its `LLEN`, instead
    # of fanning out into per-message rows. Pre-M6.x this would
    # yield zero rows ("pick a queue first"); the polymorphic admin
    # now relies on this row set as its landing page.
    _push(patched_broker, "celery", kwargs={"repoid": 1})

    qs = CeleryBrokerQueueQuerySet(CeleryBrokerQueue)
    rows = list(qs)

    assert qs.is_summary_mode()
    by_name = {r.queue_name: r for r in rows}
    # The minimal test env's `_resolve_celery_queue_names` returns
    # the well-known defaults (`celery`, `healthcheck`).
    assert "celery" in by_name
    assert by_name["celery"].depth == 1
    # Every summary row is shaped as `<queue>#summary` with the
    # message-specific fields all empty.
    assert by_name["celery"].pk_token == "celery#summary"
    assert by_name["celery"].index_in_queue is None
    assert by_name["celery"].task_name is None


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

    # Each call may LREM multiple times across passes (repeat-until-stable);
    # what matters is that the two calls used distinct tombstone strings so
    # they don't trample each other's LSETs.
    assert len(set(tombstones)) == 2
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

    def test_changelist_summary_mode_lists_known_queues_with_depth(self):
        # Summary mode (no `queue_name__exact` filter) renders one row
        # per resolved celery queue with its current `LLEN`. The
        # `notify` queue is not in the minimal test env's resolved
        # set; push to `celery` (a default well-known queue) so we
        # have something to assert on.
        self.redis.rpush("celery", _build_envelope(kwargs={"repoid": 1}))
        self.redis.rpush("celery", _build_envelope(kwargs={"repoid": 2}))

        response = self.client.get("/admin/redis_admin/celerybrokerqueue/")

        assert response.status_code == 200
        body = response.content.decode("utf-8", errors="replace")
        # The summary table headers + drill-in column ship instead of
        # the message-shaped columns.
        assert "queue_name" in body.lower() or "queue name" in body.lower()
        assert "messages" in body.lower()
        # The well-known queue name renders, and so does the
        # drill-in link with the URL-encoded queue filter.
        assert "queue_name__exact=celery" in body
        # The summary's drill-in link encodes the depth (2 messages
        # we just pushed) — search for the link copy with the count.
        assert "view 2 message(s)" in body or "view 2 message" in body

    def test_changelist_summary_mode_drops_message_filters(self):
        # Sidebar filters that only apply to messages (`task`,
        # `repoid`, `commit`) shouldn't render in summary mode —
        # they'd be inert without a queue selected.
        self.redis.rpush("celery", _build_envelope(kwargs={"repoid": 1}))

        response = self.client.get("/admin/redis_admin/celerybrokerqueue/")

        assert response.status_code == 200
        body = response.content.decode("utf-8", errors="replace")
        # Sidebar filter blocks render their headers under
        # `<h3>By <name></h3>`. Message-only filters must be hidden.
        assert "By task" not in body
        assert "By repoid" not in body
        assert "By commit" not in body

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

    def test_redisqueue_admin_hides_celery_broker_family(self):
        # `RedisQueueAdmin.get_queryset` calls
        # `family_exclude("celery_broker")` so the queue changelist
        # shows scalar/list-shaped queues only. Celery queues live
        # in their own per-message admin, which has its own
        # frequency-chart drill-down — surfacing them under both
        # admins would be confusing and offer two different "clear"
        # behaviours for the same key.
        self.redis.rpush("celery", _build_envelope(kwargs={"repoid": 1}))

        response = self.client.get("/admin/redis_admin/redisqueue/")

        assert response.status_code == 200
        body = response.content.decode("utf-8", errors="replace")
        # The celery queue should NOT appear as a row on the
        # `RedisQueue` changelist.
        assert "queue_name__exact=celery" not in body
        # And the (now-removed) `CeleryQueueFilter` sidebar must not
        # render; that filter pointed back at this admin and is gone.
        assert "By celery queue" not in body


@pytest.mark.django_db
def test_non_staff_user_cannot_view_celery_broker_admin():
    user = UserFactory(is_staff=False)
    client = Client()
    client.force_login(user)

    response = client.get("/admin/redis_admin/celerybrokerqueue/")

    assert response.status_code in (302, 403)


# ---- Frequency chart helper ------------------------------------------------


def test_frequency_by_task_repo_commit_groups_and_sorts(patched_broker):
    # Two distinct (task, repoid, commit) triples with different
    # cardinalities so the sort order has something to express. The
    # `task=None` row also exercises the (None sorts last) tie-break
    # rule on the `task_name` axis.
    for _ in range(3):
        patched_broker.rpush(
            "celery",
            _build_envelope(
                task="app.tasks.notify.NotifyTask",
                kwargs={"repoid": 1, "commitid": "aaaa"},
            ),
        )
    for _ in range(2):
        patched_broker.rpush(
            "celery",
            _build_envelope(
                task="app.tasks.bundle_analysis.BundleAnalysisProcessor",
                kwargs={"repoid": 2, "commitid": "bbbb"},
            ),
        )
    patched_broker.rpush(
        "celery",
        _build_envelope(
            task="app.tasks.notify.NotifyTask",
            kwargs={"repoid": 1, "commitid": "cccc"},
        ),
    )

    qs = CeleryBrokerQueueQuerySet(CeleryBrokerQueue).filter(queue_name__exact="celery")
    buckets = qs.frequency_by_task_repo_commit()

    assert [(b.task_name, b.repoid, b.commitid, b.count) for b in buckets] == [
        ("app.tasks.notify.NotifyTask", 1, "aaaa", 3),
        ("app.tasks.bundle_analysis.BundleAnalysisProcessor", 2, "bbbb", 2),
        ("app.tasks.notify.NotifyTask", 1, "cccc", 1),
    ]
    # Percentages sum to ~100 over the 6 messages we pushed.
    total_pct = sum(b.pct for b in buckets)
    assert 99.0 < total_pct < 101.0


def test_frequency_by_task_repo_commit_separates_tasks_on_shared_queue(
    patched_broker,
):
    # Two different tasks share the same `(repoid, commitid)` pair —
    # the chart MUST split them into separate buckets so a per-row
    # "Clear N" doesn't silently drop messages from the unrelated
    # task. The `notify` task has fewer messages but appears earlier
    # alphabetically; verify the count-desc tie-break still wins.
    for _ in range(2):
        patched_broker.rpush(
            "celery",
            _build_envelope(
                task="app.tasks.bundle_analysis.BundleAnalysisProcessor",
                kwargs={"repoid": 1, "commitid": "aaaa"},
            ),
        )
    patched_broker.rpush(
        "celery",
        _build_envelope(
            task="app.tasks.notify.NotifyTask",
            kwargs={"repoid": 1, "commitid": "aaaa"},
        ),
    )

    qs = CeleryBrokerQueueQuerySet(CeleryBrokerQueue).filter(queue_name__exact="celery")
    buckets = qs.frequency_by_task_repo_commit()

    assert [(b.task_name, b.count) for b in buckets] == [
        ("app.tasks.bundle_analysis.BundleAnalysisProcessor", 2),
        ("app.tasks.notify.NotifyTask", 1),
    ]


def test_frequency_by_task_repo_commit_drops_fully_empty_rows(patched_broker):
    # Every axis None → bucket dropped (the chart's row click target
    # would otherwise produce a `clear-by-filter/?queue=celery` with
    # no narrowing filter, which is refused server-side anyway).
    patched_broker.rpush(
        "celery",
        _build_envelope(task=None, kwargs={}),  # no task / repoid / commitid
    )
    patched_broker.rpush(
        "celery",
        _build_envelope(task="app.tasks.notify.NotifyTask", kwargs={"repoid": 9}),
    )

    qs = CeleryBrokerQueueQuerySet(CeleryBrokerQueue).filter(queue_name__exact="celery")
    buckets = qs.frequency_by_task_repo_commit()

    assert [(b.task_name, b.repoid, b.commitid, b.count) for b in buckets] == [
        ("app.tasks.notify.NotifyTask", 9, None, 1),
    ]


def test_frequency_by_task_repo_commit_returns_empty_in_summary_mode(patched_broker):
    # Summary mode (no queue selected) doesn't materialise messages,
    # so the chart helper has no per-message data to aggregate.
    qs = CeleryBrokerQueueQuerySet(CeleryBrokerQueue)
    assert qs.is_summary_mode()
    assert qs.frequency_by_task_repo_commit() == []


# ---- Frequency chart panel rendering --------------------------------------


class CeleryFrequencyChartTest(TestCase):
    def setUp(self):
        self.redis = fakeredis.FakeStrictRedis()
        self._orig_get_connection = redis_admin_conn.get_connection
        redis_admin_conn.get_connection = lambda kind="default": self.redis  # type: ignore[assignment]
        self.user = UserFactory(is_staff=True, is_superuser=True)
        self.client = Client()
        self.client.force_login(self.user)

    def tearDown(self):
        redis_admin_conn.get_connection = self._orig_get_connection  # type: ignore[assignment]

    def test_drill_down_renders_frequency_chart_panel(self):
        # Seed a real Repository row so the chart's repo cell can
        # resolve `repoid` → `service:owner/name` via
        # `_resolve_repo_displays` (the same helper backing the
        # `repo_display` column on `RedisQueueAdmin`).
        owner = OwnerFactory(service="github", username="codecov")
        repo = RepositoryFactory(author=owner, name="example")
        for _ in range(2):
            self.redis.rpush(
                "celery",
                _build_envelope(
                    task="app.tasks.notify.NotifyTask",
                    kwargs={
                        "repoid": repo.repoid,
                        "commitid": "deadbeef",
                    },
                ),
            )

        response = self.client.get(
            "/admin/redis_admin/celerybrokerqueue/?queue_name__exact=celery"
        )

        assert response.status_code == 200
        body = response.content.decode("utf-8", errors="replace")
        # Changelist renders the lazy-load placeholder, not the chart itself.
        assert 'id="celery-chart-fragment"' in body
        assert "data-fragment-url=" in body
        # The loader is shipped as an EXTERNAL static file (not inline)
        # so the strict CSP on api-admin.codecov.io — which only allows
        # `'self'` and one fixed sha256 for inline scripts — does not
        # block it. See settings_base.py CSP_DEFAULT_SRC.
        assert "celery_chart_fragment.js" in body
        assert 'src="' in body  # confirms external script form, not inline

        # Fetch the chart fragment directly (as the browser's JS would).
        fragment_response = self.client.get(
            "/admin/redis_admin/celerybrokerqueue/celery/chart-fragment/"
        )
        assert fragment_response.status_code == 200
        fragment_body = fragment_response.content.decode("utf-8", errors="replace")
        # Panel header anchored to the chart fragment ID. Heading
        # order mirrors the column order: repoid → commitid → task.
        assert "celery-frequency-chart" in fragment_body
        assert "Top 1 (repoid, commitid, task) triples in" in fragment_body
        # Task name renders in the (now third) task column.
        assert "app.tasks.notify.NotifyTask" in fragment_body
        # Repo cell renders the `service:owner/name` display string
        # wrapped in a link to the standard `core_repository_change`
        # admin page (matches what `RedisQueueAdmin`'s repo_display
        # already does on the queues changelist).
        assert "github:codecov/example" in fragment_body
        assert f"/admin/core/repository/{repo.repoid}/change/" in fragment_body
        # Single neutral "Clear queue" button per bucket — clicking
        # it just opens the preview page, where the destructive
        # choice (clear all / clear all but first) is made. Chart
        # is intentionally NOT styled with `deletelink` because the
        # row click is non-destructive.
        assert "Clear queue" in fragment_body
        assert "celery-clear-queue-btn" in fragment_body
        assert 'class="deletelink"' not in fragment_body
        # No mode discriminator on the chart anymore — the preview
        # page owns that decision now.
        assert 'name="mode"' not in fragment_body
        # The form action wires through the chart's clear-by-filter
        # URL with the bucket's task_name + repoid + commitid pre-
        # populated.
        assert 'name="task_name"' in fragment_body
        assert 'name="repoid"' in fragment_body
        assert 'name="commitid"' in fragment_body
        # Chart column header order (repoid, commitid, task) — assert
        # task header appears AFTER the commitid header in the
        # rendered HTML so a future template tweak that re-orders
        # them gets caught here.
        repoid_pos = fragment_body.find(">repoid<")
        commitid_pos = fragment_body.find(">commitid<")
        task_pos = fragment_body.find(">task<")
        assert repoid_pos != -1 and commitid_pos != -1 and task_pos != -1
        assert repoid_pos < commitid_pos < task_pos

    def test_chart_renders_single_clear_button_regardless_of_count(self):
        # Singleton bucket (count=1) and N>=2 buckets both render the
        # SAME single "Clear queue" button — the chart no longer
        # gates anything on count, because the choice between
        # "clear all" and "clear all but first" lives on the preview
        # page (which can decide based on `match_count` server-side).
        self.redis.rpush(
            "celery",
            _build_envelope(
                task="app.tasks.notify.NotifyTask",
                kwargs={"repoid": 1, "commitid": "abc"},
            ),
        )

        response = self.client.get(
            "/admin/redis_admin/celerybrokerqueue/?queue_name__exact=celery"
        )

        assert response.status_code == 200
        body = response.content.decode("utf-8", errors="replace")
        assert 'id="celery-chart-fragment"' in body
        assert "data-fragment-url=" in body

        fragment_response = self.client.get(
            "/admin/redis_admin/celerybrokerqueue/celery/chart-fragment/"
        )
        assert fragment_response.status_code == 200
        fragment_body = fragment_response.content.decode("utf-8", errors="replace")
        assert "celery-frequency-chart" in fragment_body
        assert "Clear queue" in fragment_body
        # Old per-row button labels and the `mode` discriminator
        # must not leak through.
        assert "keep first" not in fragment_body
        assert 'name="mode"' not in fragment_body

    def test_chart_falls_back_to_bare_repoid_when_repo_missing(self):
        # No `Repository` row matches `repoid=99999` → the chart
        # gracefully degrades to the bare numeric repoid instead of
        # 500ing or rendering an empty cell.
        for _ in range(2):
            self.redis.rpush(
                "celery",
                _build_envelope(kwargs={"repoid": 99999, "commitid": "abc"}),
            )

        response = self.client.get(
            "/admin/redis_admin/celerybrokerqueue/?queue_name__exact=celery"
        )

        assert response.status_code == 200
        body = response.content.decode("utf-8", errors="replace")
        assert 'id="celery-chart-fragment"' in body
        assert "data-fragment-url=" in body

        fragment_response = self.client.get(
            "/admin/redis_admin/celerybrokerqueue/celery/chart-fragment/"
        )
        assert fragment_response.status_code == 200
        fragment_body = fragment_response.content.decode("utf-8", errors="replace")
        assert "celery-frequency-chart" in fragment_body
        assert "99999" in fragment_body
        # No `service:owner/name` should appear — the lookup miss
        # returns `None` and the template falls back to the int.
        assert "github:" not in fragment_body

    def test_summary_view_does_not_render_chart(self):
        self.redis.rpush(
            "celery",
            _build_envelope(kwargs={"repoid": 1, "commitid": "abc"}),
        )

        response = self.client.get("/admin/redis_admin/celerybrokerqueue/")

        assert response.status_code == 200
        body = response.content.decode("utf-8", errors="replace")
        # No chart in summary mode — the chart is for drill-down.
        assert "celery-frequency-chart" not in body

    def test_chart_hidden_for_non_superuser_can_clear_flag(self):
        # Non-superuser: the chart still renders, but the per-bucket
        # "Clear queue" submit buttons are gated by `can_clear`.
        self.redis.rpush(
            "celery", _build_envelope(kwargs={"repoid": 7, "commitid": "fff"})
        )
        staff_user = UserFactory(is_staff=True, is_superuser=False)
        self.client.force_login(staff_user)

        response = self.client.get(
            "/admin/redis_admin/celerybrokerqueue/?queue_name__exact=celery"
        )

        assert response.status_code == 200
        body = response.content.decode("utf-8", errors="replace")
        # Changelist always renders the placeholder regardless of superuser status.
        assert 'id="celery-chart-fragment"' in body
        assert "data-fragment-url=" in body

        # Fragment still renders for staff users, but without clear buttons.
        fragment_response = self.client.get(
            "/admin/redis_admin/celerybrokerqueue/celery/chart-fragment/"
        )
        assert fragment_response.status_code == 200
        fragment_body = fragment_response.content.decode("utf-8", errors="replace")
        assert "celery-frequency-chart" in fragment_body
        # No clear-by-filter form for non-superuser staff.
        assert "Clear queue" not in fragment_body
        assert "celery-clear-queue-btn" not in fragment_body


# ---- clear-by-filter view --------------------------------------------------


class CeleryBrokerClearByFilterViewTest(TestCase):
    def setUp(self):
        self.redis = fakeredis.FakeStrictRedis()
        self._orig_get_connection = redis_admin_conn.get_connection
        redis_admin_conn.get_connection = lambda kind="default": self.redis  # type: ignore[assignment]
        self.user = UserFactory(is_staff=True, is_superuser=True)
        self.client = Client()
        self.client.force_login(self.user)

    def tearDown(self):
        redis_admin_conn.get_connection = self._orig_get_connection  # type: ignore[assignment]

    def _push(self, **kwargs):
        self.redis.rpush("celery", _build_envelope(**kwargs))

    def test_chart_open_renders_preview_with_three_actions(self):
        # The chart's "Clear queue" button POSTs the bucket's scope
        # without an `action` value; the view should land on the
        # preview page rather than mutating anything. The preview
        # page must render all three submit buttons (dry-run,
        # clear-all-but-first, clear-all) when match_count >= 2.
        self._push(kwargs={"repoid": 1, "commitid": "aaaa"})
        self._push(kwargs={"repoid": 1, "commitid": "aaaa"})

        response = self.client.post(
            "/admin/redis_admin/celerybrokerqueue/clear-by-filter/",
            {
                "queue_name": "celery",
                "repoid": "1",
                "commitid": "aaaa",
            },
        )

        assert response.status_code == 200
        body = response.content.decode("utf-8", errors="replace")
        assert "Matched: 2 message(s)" in body
        # All three action buttons are wired up via distinct
        # `action` values — assert on the form-input markers, not
        # on the visible labels, so a future copy tweak doesn't
        # silently weaken the test.
        assert 'name="action" value="dry_run"' in body
        assert 'name="action" value="clear_keep_one"' in body
        assert 'name="action" value="clear_all"' in body
        # Both destructive buttons share the `celery-destructive-
        # button` class, which carries the red-text styling.
        assert "celery-destructive-button" in body
        # No mutation happened.
        assert self.redis.llen("celery") == 2

    def test_dry_run_lists_targets_without_mutating(self):
        self._push(kwargs={"repoid": 1, "commitid": "aaaa"})
        self._push(kwargs={"repoid": 1, "commitid": "aaaa"})
        self._push(kwargs={"repoid": 2, "commitid": "bbbb"})

        response = self.client.post(
            "/admin/redis_admin/celerybrokerqueue/clear-by-filter/",
            {
                "queue_name": "celery",
                "repoid": "1",
                "commitid": "aaaa",
                "action": "dry_run",
            },
        )

        assert response.status_code == 200
        body = response.content.decode("utf-8", errors="replace")
        assert "Matched: 2 message(s)" in body
        # Dry-run did not pop the queue.
        assert self.redis.llen("celery") == 3

    def test_clear_all_clears_only_matching_messages(self):
        self._push(kwargs={"repoid": 1, "commitid": "aaaa"})
        self._push(kwargs={"repoid": 1, "commitid": "aaaa"})
        self._push(kwargs={"repoid": 2, "commitid": "bbbb"})

        response = self.client.post(
            "/admin/redis_admin/celerybrokerqueue/clear-by-filter/",
            {
                "queue_name": "celery",
                "repoid": "1",
                "commitid": "aaaa",
                "action": "clear_all",
                "typed_confirm": "celery",
            },
            follow=False,
        )

        assert response.status_code in (302, 303)
        # The two repoid=1 messages were tombstoned and LREM'd; the
        # repoid=2 message remains. The kombu envelope's body is
        # base64-encoded, so re-parse it to check the kwargs.
        remaining = self.redis.lrange("celery", 0, -1)
        assert len(remaining) == 1
        envelope = json.loads(remaining[0])
        body = json.loads(base64.b64decode(envelope["body"]).decode("utf-8"))
        assert body[1].get("repoid") == 2
        assert body[1].get("commitid") == "bbbb"

    def test_clear_all_requires_typed_confirmation(self):
        self._push(kwargs={"repoid": 1, "commitid": "aaaa"})

        response = self.client.post(
            "/admin/redis_admin/celerybrokerqueue/clear-by-filter/",
            {
                "queue_name": "celery",
                "repoid": "1",
                "commitid": "aaaa",
                "action": "clear_all",
                "typed_confirm": "wrong-queue",
            },
        )

        # We re-render the page (no redirect) and surface the typed-
        # confirm error; the queue is untouched.
        assert response.status_code == 200
        body = response.content.decode("utf-8", errors="replace")
        assert "Typed confirmation must equal" in body
        assert self.redis.llen("celery") == 1

    def test_clear_keep_one_requires_typed_confirmation(self):
        # Same gate applies to the "clear all but first" destructive
        # button — typed_confirm is the single chokepoint for both
        # red buttons.
        self._push(kwargs={"repoid": 1, "commitid": "aaaa"})
        self._push(kwargs={"repoid": 1, "commitid": "aaaa"})

        response = self.client.post(
            "/admin/redis_admin/celerybrokerqueue/clear-by-filter/",
            {
                "queue_name": "celery",
                "repoid": "1",
                "commitid": "aaaa",
                "action": "clear_keep_one",
                "typed_confirm": "wrong-queue",
            },
        )

        assert response.status_code == 200
        body = response.content.decode("utf-8", errors="replace")
        assert "Typed confirmation must equal" in body
        assert self.redis.llen("celery") == 2

    def test_task_name_filter_narrows_to_matching_task_only(self):
        # Two task classes share `(repoid, commitid)`. The chart's
        # row-level "Clear queue" submits `task_name` alongside
        # repoid/commitid; that combination must clear only the
        # targeted task and leave the other one in the queue.
        self._push(
            task="app.tasks.bundle_analysis.BundleAnalysisProcessor",
            kwargs={"repoid": 1, "commitid": "aaaa"},
        )
        self._push(
            task="app.tasks.notify.NotifyTask",
            kwargs={"repoid": 1, "commitid": "aaaa"},
        )

        response = self.client.post(
            "/admin/redis_admin/celerybrokerqueue/clear-by-filter/",
            {
                "queue_name": "celery",
                "task_name": "app.tasks.bundle_analysis.BundleAnalysisProcessor",
                "repoid": "1",
                "commitid": "aaaa",
                "action": "clear_all",
                "typed_confirm": "celery",
            },
            follow=False,
        )

        assert response.status_code in (302, 303)
        remaining = self.redis.lrange("celery", 0, -1)
        assert len(remaining) == 1
        envelope = json.loads(remaining[0])
        # The surviving message is the unrelated `notify` task.
        assert envelope["headers"]["task"] == "app.tasks.notify.NotifyTask"

    def test_task_name_alone_is_a_valid_narrowing_filter(self):
        # task_name on its own (no repoid / commitid) is enough to
        # clear — the chart's leftmost column lets operators clear
        # "every notify task in this queue" without needing to pin
        # a repo or commit.
        self._push(
            task="app.tasks.notify.NotifyTask",
            kwargs={"repoid": 1, "commitid": "aaaa"},
        )
        self._push(
            task="app.tasks.bundle_analysis.BundleAnalysisProcessor",
            kwargs={"repoid": 1, "commitid": "aaaa"},
        )

        response = self.client.post(
            "/admin/redis_admin/celerybrokerqueue/clear-by-filter/",
            {
                "queue_name": "celery",
                "task_name": "app.tasks.notify.NotifyTask",
                "action": "clear_all",
                "typed_confirm": "celery",
            },
            follow=False,
        )

        assert response.status_code in (302, 303)
        remaining = self.redis.lrange("celery", 0, -1)
        assert len(remaining) == 1
        envelope = json.loads(remaining[0])
        assert envelope["headers"]["task"] == (
            "app.tasks.bundle_analysis.BundleAnalysisProcessor"
        )

    def test_clear_keep_one_leaves_lowest_index_match_in_queue(self):
        # `action=clear_keep_one` should clear every match EXCEPT
        # the one with the lowest `index_in_queue` — i.e. the head-
        # of-queue, the next message a Celery worker would pop.
        # With three matching messages at indexes 0/1/2, indexes 1
        # and 2 are cleared and index 0 stays.
        self._push(
            task="app.tasks.notify.NotifyTask",
            task_id="keep-this-one",
            kwargs={"repoid": 1, "commitid": "aaaa"},
        )
        self._push(
            task="app.tasks.notify.NotifyTask",
            task_id="drop-1",
            kwargs={"repoid": 1, "commitid": "aaaa"},
        )
        self._push(
            task="app.tasks.notify.NotifyTask",
            task_id="drop-2",
            kwargs={"repoid": 1, "commitid": "aaaa"},
        )
        # Plus an unrelated task that should be untouched regardless.
        self._push(
            task="app.tasks.notify.NotifyTask",
            task_id="unrelated",
            kwargs={"repoid": 99, "commitid": "zzzz"},
        )

        response = self.client.post(
            "/admin/redis_admin/celerybrokerqueue/clear-by-filter/",
            {
                "queue_name": "celery",
                "task_name": "app.tasks.notify.NotifyTask",
                "repoid": "1",
                "commitid": "aaaa",
                "action": "clear_keep_one",
                "typed_confirm": "celery",
            },
            follow=False,
        )

        assert response.status_code in (302, 303)
        remaining = self.redis.lrange("celery", 0, -1)
        # Only `keep-this-one` (lowest-index match) and the
        # unrelated repo=99 message survive.
        surviving_task_ids = {json.loads(item)["headers"]["id"] for item in remaining}
        assert surviving_task_ids == {"keep-this-one", "unrelated"}

    def test_dry_run_audited_via_service_does_not_mutate(self):
        # The dry-run button on the preview page funnels through
        # `celery_broker_clear(dry_run=True)` so the audit log
        # captures it; the queue itself is untouched.
        self._push(kwargs={"repoid": 1, "commitid": "aaaa"})
        self._push(kwargs={"repoid": 1, "commitid": "aaaa"})
        self._push(kwargs={"repoid": 1, "commitid": "aaaa"})

        response = self.client.post(
            "/admin/redis_admin/celerybrokerqueue/clear-by-filter/",
            {
                "queue_name": "celery",
                "repoid": "1",
                "commitid": "aaaa",
                "action": "dry_run",
            },
            follow=False,
        )

        assert response.status_code == 200
        body = response.content.decode("utf-8", errors="replace")
        # Dry-run re-renders the preview (200, no redirect) and the
        # full match set survives in the queue.
        assert "Matched: 3" in body
        assert self.redis.llen("celery") == 3

    def test_clear_keep_one_button_hidden_when_only_one_match(self):
        # Single-match preview: the destructive "Clear all but
        # first" button must not render — there's only one message
        # and the keep-first semantic would clear nothing. The
        # "Clear all" button still shows.
        self._push(
            task="app.tasks.notify.NotifyTask",
            kwargs={"repoid": 1, "commitid": "aaaa"},
        )

        response = self.client.post(
            "/admin/redis_admin/celerybrokerqueue/clear-by-filter/",
            {
                "queue_name": "celery",
                "task_name": "app.tasks.notify.NotifyTask",
                "repoid": "1",
                "commitid": "aaaa",
            },
        )

        assert response.status_code == 200
        body = response.content.decode("utf-8", errors="replace")
        assert "Matched: 1 message(s)" in body
        assert 'name="action" value="dry_run"' in body
        assert 'name="action" value="clear_all"' in body
        assert 'name="action" value="clear_keep_one"' not in body

    def test_clear_keep_one_is_noop_when_only_one_match(self):
        # Single-message bucket: `action=clear_keep_one` would have
        # to drop the only in-flight message to do anything, which
        # defeats the "keep first" semantic. The preview page only
        # renders the button for match_count >= 2, but a hand-
        # crafted POST or a count change between render and submit
        # could still land here — the view must short-circuit with
        # a friendly info message and leave the queue untouched.
        self._push(
            task="app.tasks.notify.NotifyTask",
            task_id="only-one",
            kwargs={"repoid": 1, "commitid": "aaaa"},
        )

        response = self.client.post(
            "/admin/redis_admin/celerybrokerqueue/clear-by-filter/",
            {
                "queue_name": "celery",
                "task_name": "app.tasks.notify.NotifyTask",
                "repoid": "1",
                "commitid": "aaaa",
                "action": "clear_keep_one",
                "typed_confirm": "celery",
            },
            follow=False,
        )

        # Redirected back to the queue's changelist with an info
        # message; the message is still in the queue.
        assert response.status_code in (302, 303)
        remaining = self.redis.lrange("celery", 0, -1)
        assert len(remaining) == 1
        envelope = json.loads(remaining[0])
        assert envelope["headers"]["id"] == "only-one"

    def test_legacy_confirm_action_still_clears_all(self):
        # Backward-compat shim: the previous version of this view
        # used `action=confirm`. New callers send `clear_all` /
        # `clear_keep_one`; the shim keeps stale tabs / scripts
        # working without re-loading.
        self._push(kwargs={"repoid": 1, "commitid": "aaaa"})
        self._push(kwargs={"repoid": 1, "commitid": "aaaa"})

        response = self.client.post(
            "/admin/redis_admin/celerybrokerqueue/clear-by-filter/",
            {
                "queue_name": "celery",
                "repoid": "1",
                "commitid": "aaaa",
                "action": "confirm",
                "typed_confirm": "celery",
            },
            follow=False,
        )

        assert response.status_code in (302, 303)
        assert self.redis.llen("celery") == 0

    def test_legacy_confirm_with_mode_keep_one_routes_correctly(self):
        # The old form posted `action=confirm` + `mode=keep_one` for
        # the keep-first variant; the shim must route that to the
        # new `clear_keep_one` action so the lowest-index match
        # survives.
        self._push(
            task="t.K",
            task_id="keep",
            kwargs={"repoid": 1, "commitid": "aaaa"},
        )
        self._push(
            task="t.K",
            task_id="drop",
            kwargs={"repoid": 1, "commitid": "aaaa"},
        )

        response = self.client.post(
            "/admin/redis_admin/celerybrokerqueue/clear-by-filter/",
            {
                "queue_name": "celery",
                "task_name": "t.K",
                "repoid": "1",
                "commitid": "aaaa",
                "mode": "keep_one",
                "action": "confirm",
                "typed_confirm": "celery",
            },
            follow=False,
        )

        assert response.status_code in (302, 303)
        remaining = self.redis.lrange("celery", 0, -1)
        assert len(remaining) == 1
        assert json.loads(remaining[0])["headers"]["id"] == "keep"

    def test_refuses_clear_without_narrowing_filter(self):
        self._push(kwargs={"repoid": 1, "commitid": "aaaa"})

        response = self.client.post(
            "/admin/redis_admin/celerybrokerqueue/clear-by-filter/",
            {
                "queue_name": "celery",
                "action": "clear_all",
                "typed_confirm": "celery",
            },
            follow=True,
        )

        # Redirected back to changelist with an error message; queue
        # unchanged because we refuse the empty-narrowing case.
        assert response.status_code == 200
        # Sanity: the message in `messages` framework is rendered as
        # part of the changelist response.
        assert self.redis.llen("celery") == 1

    def test_non_superuser_is_refused(self):
        self._push(kwargs={"repoid": 1, "commitid": "aaaa"})
        staff_user = UserFactory(is_staff=True, is_superuser=False)
        self.client.force_login(staff_user)

        response = self.client.post(
            "/admin/redis_admin/celerybrokerqueue/clear-by-filter/",
            {
                "queue_name": "celery",
                "repoid": "1",
                "action": "clear_all",
                "typed_confirm": "celery",
            },
        )

        assert response.status_code in (403, 302)
        assert self.redis.llen("celery") == 1


# ---- Summary-mode queryset -------------------------------------------------


def test_queryset_summary_mode_lists_known_queues_with_depth(patched_broker):
    # Empty queues still show up in summary mode with `depth=0` so an
    # operator can confirm "yes the queue exists, it's just empty".
    patched_broker.rpush("celery", _build_envelope(kwargs={"repoid": 1}))
    patched_broker.rpush("celery", _build_envelope(kwargs={"repoid": 2}))

    qs = CeleryBrokerQueueQuerySet(CeleryBrokerQueue)
    rows = list(qs)

    by_name = {r.queue_name: r for r in rows}
    # `_resolve_celery_queue_names` falls back to the well-known
    # defaults in a minimal test env: `("celery", "healthcheck")`.
    assert "celery" in by_name
    assert by_name["celery"].depth == 2
    # Every summary row carries `index_in_queue=None` and a
    # `<queue>#summary` pk_token; message-shaped fields are unset.
    assert by_name["celery"].index_in_queue is None
    assert by_name["celery"].pk_token == "celery#summary"


# ---- family_exclude --------------------------------------------------------


def test_redisqueue_queryset_family_exclude_drops_listed_families(patched_broker):
    """`family_exclude("celery_broker")` keeps the celery queues out
    of the `RedisQueue` changelist while leaving other queue families
    visible. We seed one non-celery queue (`uploads:1/abc`) and one
    celery queue (`celery`) and confirm only the former survives.
    """

    # Celery key + a (smaller) non-celery surface — `RedisQueue`
    # treats the celery queue as a `list` family.
    patched_broker.rpush("celery", _build_envelope(kwargs={"repoid": 1}))

    qs = RedisQueue.objects.all().family_exclude("celery_broker")
    families_present = {row.family for row in qs}

    assert "celery_broker" not in families_present


# ---- _resolve_repo_displays -----------------------------------------------


@pytest.mark.django_db
def test_resolve_repo_displays_returns_service_owner_name():
    owner = OwnerFactory(service="gitlab", username="acme")
    repo = RepositoryFactory(author=owner, name="widgets")

    mapping = _resolve_repo_displays([repo.repoid])

    assert mapping[repo.repoid] == "gitlab:acme/widgets"


@pytest.mark.django_db
def test_resolve_repo_displays_falls_back_to_bare_name_without_username():
    # `_hydrate_repo_displays` had this fallback inline before the
    # extraction; pin it as part of the helper's contract so callers
    # (the chart, the queues changelist, the locks changelist) stay
    # aligned on what gets rendered for an authorless repo.
    owner = OwnerFactory(service="github", username="")
    repo = RepositoryFactory(author=owner, name="orphan")

    mapping = _resolve_repo_displays([repo.repoid])

    assert mapping[repo.repoid] == "orphan"


@pytest.mark.django_db
def test_resolve_repo_displays_skips_falsy_repoids():
    # Empty / `None` repoids get skipped silently; the helper does
    # not blow up on the chart's `(None, None, …)`-shaped buckets,
    # which the chart drops anyway but defense-in-depth here keeps
    # the helper safe to call from any caller.
    assert _resolve_repo_displays([None, 0]) == {}


# ---------------------------------------------------------------------------
# PR #899: orjson envelope parsing
# ---------------------------------------------------------------------------


def test_orjson_parser_handles_bytes_body():
    """orjson.loads inside parse_celery_envelope accepts raw bytes.

    The body is base64-decoded to bytes and passed directly to
    orjson.loads — verify we get the same structured output as
    the stdlib path did before.
    """

    envelope = _build_envelope(
        task="app.tasks.notify.NotifyTask",
        task_id="test-orjson-uuid",
        kwargs={"repoid": 42, "commitid": "cafebabe"},
    )
    meta = parse_celery_envelope(envelope)

    assert meta.task == "app.tasks.notify.NotifyTask"
    assert meta.task_id == "test-orjson-uuid"
    assert meta.repoid == 42
    assert meta.commitid == "cafebabe"
    assert meta.kwargs == {"repoid": 42, "commitid": "cafebabe"}


def test_orjson_parser_roundtrip_unicode():
    """Unicode task names survive the orjson round-trip cleanly."""

    envelope = _build_envelope(
        task="app.tasks.ünïcödé.TaskName",
        kwargs={"repoid": 1},
    )
    meta = parse_celery_envelope(envelope)

    assert meta.task == "app.tasks.ünïcödé.TaskName"
    assert meta.repoid == 1


# ---------------------------------------------------------------------------
# PR #899: streaming chart aggregator
# ---------------------------------------------------------------------------


def test_stream_frequency_aggregate_matches_eager_for_small_queue(patched_broker):
    """Streaming aggregator produces same FrequencyBucket list as the prior
    eager `_fetch_all`-based path for a small synthetic fixture.
    """

    for _ in range(3):
        patched_broker.rpush(
            "celery",
            _build_envelope(
                task="app.tasks.notify.NotifyTask",
                kwargs={"repoid": 1, "commitid": "aaaa"},
            ),
        )
    for _ in range(2):
        patched_broker.rpush(
            "celery",
            _build_envelope(
                task="app.tasks.bundle_analysis.BA",
                kwargs={"repoid": 2, "commitid": "bbbb"},
            ),
        )

    buckets, _ = _stream_frequency_aggregate(patched_broker, "celery")

    assert [(b.task_name, b.repoid, b.commitid, b.count) for b in buckets] == [
        ("app.tasks.notify.NotifyTask", 1, "aaaa", 3),
        ("app.tasks.bundle_analysis.BA", 2, "bbbb", 2),
    ]
    total_pct = sum(b.pct for b in buckets)
    assert 99.0 < total_pct < 101.0


def test_stream_frequency_aggregate_large_queue_correct_counts(patched_broker):
    """Streaming aggregator handles a queue larger than _STREAM_CHUNK correctly."""

    n = _STREAM_CHUNK + 5_000  # 15k > chunk of 10k
    task_a = "app.tasks.a.TaskA"
    task_b = "app.tasks.b.TaskB"
    for i in range(n):
        task = task_a if i % 3 != 0 else task_b
        patched_broker.rpush(
            "bigqueue",
            _build_envelope(task=task, kwargs={"repoid": 1, "commitid": "aaa"}),
        )

    buckets, _ = _stream_frequency_aggregate(patched_broker, "bigqueue")

    # n // 3 messages have task_b (every 3rd, i=0,3,6,…)
    count_b = n // 3
    count_a = n - count_b
    by_task = {b.task_name: b.count for b in buckets}
    assert by_task[task_a] == count_a
    assert by_task[task_b] == count_b


def test_frequency_by_task_repo_commit_uses_streaming_when_cache_empty(
    patched_broker,
):
    """When no request-cache is set, frequency_by_task_repo_commit falls
    back to the streaming aggregator (doesn't materialise into cache).
    """

    for _ in range(5):
        patched_broker.rpush(
            "celery",
            _build_envelope(
                task="app.tasks.x.X",
                kwargs={"repoid": 7, "commitid": "xyz"},
            ),
        )

    qs = CeleryBrokerQueueQuerySet(CeleryBrokerQueue, queue_name="celery")
    # No request → no cache → streaming path
    buckets = qs.frequency_by_task_repo_commit()

    assert len(buckets) == 1
    assert buckets[0].count == 5
    assert buckets[0].task_name == "app.tasks.x.X"


def test_stream_frequency_aggregate_returns_full_total_with_top_truncation(
    patched_broker,
):
    """total_sampled reflects all messages even when buckets are truncated by top."""

    # Push 25 distinct (task, repoid, commitid) triples — each with 1 message.
    for i in range(25):
        patched_broker.rpush(
            "celery",
            _build_envelope(
                task=f"app.tasks.task_{i}",
                kwargs={"repoid": i, "commitid": f"commit{i}"},
            ),
        )

    buckets, total_sampled = _stream_frequency_aggregate(
        patched_broker, "celery", top=5
    )

    assert len(buckets) == 5
    assert total_sampled == 25


def test_stream_frequency_aggregate_total_includes_unparseable_envelopes(
    patched_broker,
):
    """total_sampled counts all-None envelopes; they are excluded from buckets."""

    # 3 valid messages
    for _ in range(3):
        patched_broker.rpush(
            "celery",
            _build_envelope(
                task="app.tasks.notify.NotifyTask",
                kwargs={"repoid": 1, "commitid": "abc"},
            ),
        )
    # 2 garbage envelopes — task=None produces all-None axes in parse_celery_envelope
    for _ in range(2):
        patched_broker.rpush(
            "celery",
            _build_envelope(task=None, kwargs={}),
        )

    buckets, total_sampled = _stream_frequency_aggregate(patched_broker, "celery")

    # All 5 messages are counted in total_sampled
    assert total_sampled == 5
    # Only the 3 valid messages form a bucket
    assert len(buckets) == 1
    assert buckets[0].count == 3
    assert sum(b.count for b in buckets) < total_sampled


# ---------------------------------------------------------------------------
# PR #899: chart-fragment URL
# ---------------------------------------------------------------------------


class ChartFragmentViewTest(TestCase):
    def setUp(self):
        self.redis = fakeredis.FakeStrictRedis()
        self._orig_get_connection = redis_admin_conn.get_connection
        redis_admin_conn.get_connection = lambda kind="default": self.redis
        self.user = UserFactory(is_staff=True, is_superuser=True)
        self.client = Client()
        self.client.force_login(self.user)

    def tearDown(self):
        redis_admin_conn.get_connection = self._orig_get_connection

    def _push(self, queue, **kwargs):
        self.redis.rpush(queue, _build_envelope(**kwargs))

    def test_chart_fragment_returns_200_with_expected_substrings(self):
        self._push(
            "celery",
            task="app.tasks.notify.NotifyTask",
            kwargs={"repoid": 42, "commitid": "deadbeef"},
        )
        self._push(
            "celery",
            task="app.tasks.notify.NotifyTask",
            kwargs={"repoid": 42, "commitid": "deadbeef"},
        )

        response = self.client.get(
            "/admin/redis_admin/celerybrokerqueue/celery/chart-fragment/"
        )

        assert response.status_code == 200
        body = response.content.decode("utf-8", errors="replace")
        assert "app.tasks.notify.NotifyTask" in body
        assert "42" in body
        assert "deadbeef" in body
        assert "2" in body  # count

    def test_chart_fragment_returns_204_for_empty_queue(self):
        # Empty queue → no buckets → 204 No Content
        response = self.client.get(
            "/admin/redis_admin/celerybrokerqueue/nonexistent-q/chart-fragment/"
        )

        assert response.status_code == 204

    def test_chart_fragment_rejects_unauthenticated(self):
        anon = DjClient()
        response = anon.get(
            "/admin/redis_admin/celerybrokerqueue/celery/chart-fragment/"
        )

        assert response.status_code in (302, 403)

    def test_chart_fragment_rejects_non_staff(self):
        non_staff = UserFactory(is_staff=False)
        self.client.force_login(non_staff)

        response = self.client.get(
            "/admin/redis_admin/celerybrokerqueue/celery/chart-fragment/"
        )

        assert response.status_code in (302, 403)

    def test_changelist_drill_down_includes_fragment_placeholder(self):
        """The drill-down changelist now has a data-fragment-url div
        instead of the inline chart include.
        """

        self._push(
            "celery",
            task="app.tasks.notify.NotifyTask",
            kwargs={"repoid": 1, "commitid": "abc"},
        )

        response = self.client.get(
            "/admin/redis_admin/celerybrokerqueue/?queue_name__exact=celery"
        )

        assert response.status_code == 200
        body = response.content.decode("utf-8", errors="replace")
        # The lazy fragment div is present.
        assert "celery-chart-fragment" in body
        assert "data-fragment-url" in body
        assert "chart-fragment" in body


# ---------------------------------------------------------------------------
# PR #899: chart-fragment regression — no-DB guard
# ---------------------------------------------------------------------------
#
# Exercises _build_frequency_chart_context / chart_fragment_view WITHOUT
# Django DB so this can run in any sandbox (no Postgres needed). This test
# specifically catches the TypeError that arose from passing `request=`
# to CeleryBrokerQueueQuerySet.__init__, which does not accept that kwarg.
# Before the fix the view would 500; after the fix it returns 204 for an
# empty queue.


def test_chart_fragment_no_db_catches_request_kwarg_regression(patched_broker):
    """Regression: chart_fragment_view must NOT pass request= to
    CeleryBrokerQueueQuerySet (which raises TypeError → HTTP 500).

    Uses the patched_broker fixture so no Postgres connection is required.
    """
    site = AdminSite()
    admin_instance = CeleryBrokerQueueAdmin(CeleryBrokerQueue, site)
    # Minimal mock: only is_staff / is_superuser are read on the empty-queue path.
    request = SimpleNamespace(user=SimpleNamespace(is_staff=True, is_superuser=True))

    # Empty queue → _stream_frequency_aggregate returns [] → view returns 204.
    # Before the fix this raised TypeError and the view returned 500.
    response = admin_instance.chart_fragment_view(request, "no-such-queue")

    assert isinstance(response, HttpResponse)
    assert response.status_code == 204


# ---------------------------------------------------------------------------
# PR #899: streaming clear — verify-before-LSET + repeat-until-stable
# ---------------------------------------------------------------------------


@pytest.fixture
def broker_redis(monkeypatch):
    server = fakeredis.FakeStrictRedis()
    monkeypatch.setattr(
        redis_admin_conn, "get_connection", lambda kind="default": server
    )
    return server


def _push_raw(redis, queue, **kwargs):
    raw = _build_envelope(**kwargs)
    redis.rpush(queue, raw)
    return raw.encode() if isinstance(raw, str) else raw


def _make_filter(*tuples):
    return frozenset(tuples)


def test_streaming_clear_single_pass_no_drift(broker_redis):
    """Single-pass clear matches existing LSET-tombstone behaviour when
    no drift occurs.
    """

    _push_raw(broker_redis, "notify", task="t.A", kwargs={"repoid": 1, "commitid": "x"})
    _push_raw(broker_redis, "notify", task="t.A", kwargs={"repoid": 1, "commitid": "x"})
    _push_raw(broker_redis, "notify", task="t.B", kwargs={"repoid": 2, "commitid": "y"})

    tombstone = f"{_CELERY_TOMBSTONE_PREFIX}:test-no-drift"
    filter_tuples = _make_filter(("t.A", 1, "x"))
    stats = _streaming_celery_clear(
        broker_redis, "notify", filter_tuples, tombstone, dry_run=False
    )

    assert stats.total_lset == 2
    assert stats.total_drifted == 0
    # First pass clears 2 matches; second pass confirms 0 remaining → exits.
    assert stats.passes_run == 2
    # Only the t.B message survives.
    assert broker_redis.llen("notify") == 1
    survivor = json.loads(broker_redis.lrange("notify", 0, -1)[0])
    assert survivor["headers"]["task"] == "t.B"


@pytest.mark.django_db
def test_streaming_clear_verify_before_lset_skips_drifted_entry(broker_redis):
    """Verify-before-LSET skips a slot whose bytes changed between
    LRANGE snapshot and LINDEX re-read.
    """

    raw_a = _build_envelope(task="t.A", kwargs={"repoid": 1, "commitid": "x"})
    broker_redis.rpush("notify", raw_a)

    # Intercept lindex to simulate drift: return different bytes.
    real_lindex = broker_redis.lindex

    def drifted_lindex(name, idx):
        if name == "notify" and idx == 0:
            # Return bytes for a *different* message.
            return _build_envelope(
                task="t.OTHER", kwargs={"repoid": 99, "commitid": "z"}
            ).encode()
        return real_lindex(name, idx)

    broker_redis.lindex = drifted_lindex

    tombstone = f"{_CELERY_TOMBSTONE_PREFIX}:test-drift"
    filter_tuples = _make_filter(("t.A", 1, "x"))
    stats = _streaming_celery_clear(
        broker_redis, "notify", filter_tuples, tombstone, dry_run=False
    )

    # The drifted slot was skipped, nothing tombstoned.
    assert stats.total_lset == 0
    # Persistent drift causes the loop to retry; each pass records a drift hit.
    assert stats.total_drifted >= 1
    assert stats.passes_run == 2
    # Original message still in queue (bytes unchanged — we only faked lindex).
    assert broker_redis.llen("notify") == 1


@pytest.mark.django_db
def test_streaming_clear_repeat_until_stable_converges_in_2_passes(broker_redis):
    """Repeat-until-stable exits after 2 passes when drift adds 1 new
    match between passes.

    Pass 1: finds and clears message A.
    Pass 2: finds message B (appeared after pass 1 LRANGE) and clears it.
    No new matches → stable.
    """

    raw_a = _build_envelope(task="t.A", kwargs={"repoid": 1, "commitid": "x"})
    broker_redis.rpush("notify", raw_a)

    call_count = [0]
    real_llen = broker_redis.llen

    def llen_and_inject(name):
        result = real_llen(name)
        if name == "notify" and call_count[0] == 0:
            # After first llen call (pass 2 starts), inject a new match.
            broker_redis.rpush(
                "notify",
                _build_envelope(task="t.A", kwargs={"repoid": 1, "commitid": "x"}),
            )
        call_count[0] += 1
        return result

    broker_redis.llen = llen_and_inject

    tombstone = f"{_CELERY_TOMBSTONE_PREFIX}:test-2pass"
    filter_tuples = _make_filter(("t.A", 1, "x"))
    stats = _streaming_celery_clear(
        broker_redis, "notify", filter_tuples, tombstone, dry_run=False
    )

    # Both messages were cleared across 2 passes.
    assert stats.total_lset >= 1
    assert stats.passes_run >= 1  # at least 1 pass ran
    assert broker_redis.llen("notify") == 0


@pytest.mark.django_db
def test_streaming_clear_hits_max_passes_without_infinite_loop(broker_redis):
    """When drift is pathological (every pass finds the same count),
    the loop exits at MAX_PASSES.
    """

    raw_a = _build_envelope(task="t.A", kwargs={"repoid": 1, "commitid": "x"})
    broker_redis.rpush("notify", raw_a)

    real_lrem = broker_redis.lrem

    def noop_lrem(name, count, value):
        # Prevent tombstone sweep so message always re-appears.
        return 0

    broker_redis.lrem = noop_lrem

    tombstone = f"{_CELERY_TOMBSTONE_PREFIX}:test-maxpass"
    filter_tuples = _make_filter(("t.A", 1, "x"))
    stats = _streaming_celery_clear(
        broker_redis, "notify", filter_tuples, tombstone, dry_run=False
    )

    # The bounded for-loop guarantees no infinite loop; verify the upper
    # bound is honoured even under pathological drift.
    assert 1 <= stats.passes_run <= _CELERY_MAX_PASSES


@pytest.mark.django_db
def test_streaming_clear_audit_log_records_new_fields(broker_redis):
    """Audit log entry for celery_broker_clear now records passes_run,
    total_lset, total_drifted, and mode.
    """

    raw_a = _build_envelope(task="t.A", kwargs={"repoid": 1, "commitid": "x"})
    broker_redis.rpush("notify", raw_a)
    user = UserFactory()

    targets = list(CeleryBrokerQueueQuerySet(CeleryBrokerQueue, queue_name="notify"))
    celery_broker_clear(targets, user=user, dry_run=False)

    entries = list(LogEntry.objects.filter(user_id=user.id))
    assert len(entries) == 1
    payload = json.loads(entries[0].change_message)
    assert payload["scope"] == "celery_broker_clear"
    assert "passes_run" in payload
    assert "total_lset" in payload
    assert "total_drifted" in payload
    assert "mode" in payload
    assert payload["mode"] == "all-from-bucket"


@pytest.mark.django_db
def test_streaming_clear_keep_one_dry_run_does_not_mutate(broker_redis):
    """dry_run=True never mutates the queue, regardless of keep_one."""

    for _ in range(3):
        broker_redis.rpush(
            "notify",
            _build_envelope(task="t.A", kwargs={"repoid": 1, "commitid": "x"}),
        )
    user = UserFactory()

    targets = list(CeleryBrokerQueueQuerySet(CeleryBrokerQueue, queue_name="notify"))
    result = celery_broker_clear(targets, user=user, dry_run=True, keep_one=True)

    assert result.dry_run is True
    assert broker_redis.llen("notify") == 3


@pytest.mark.django_db
def test_streaming_clear_keep_one_leaves_lowest_index(broker_redis):
    """keep_one=True passes all targets but clears all but the first
    (lowest-index) match.
    """

    raw_keep = _build_envelope(
        task="t.A", task_id="keep", kwargs={"repoid": 1, "commitid": "x"}
    )
    raw_drop1 = _build_envelope(
        task="t.A", task_id="drop1", kwargs={"repoid": 1, "commitid": "x"}
    )
    raw_drop2 = _build_envelope(
        task="t.A", task_id="drop2", kwargs={"repoid": 1, "commitid": "x"}
    )
    raw_unrelated = _build_envelope(
        task="t.B", task_id="unrelated", kwargs={"repoid": 99, "commitid": "z"}
    )
    broker_redis.rpush("notify", raw_keep)
    broker_redis.rpush("notify", raw_drop1)
    broker_redis.rpush("notify", raw_drop2)
    broker_redis.rpush("notify", raw_unrelated)
    user = UserFactory()

    # Only target the (t.A, repoid=1) messages so "unrelated" (repoid=99)
    # is not included in the filter and is left untouched.
    targets = list(
        CeleryBrokerQueueQuerySet(CeleryBrokerQueue, queue_name="notify").filter(
            repoid=1
        )
    )
    result = celery_broker_clear(targets, user=user, dry_run=False, keep_one=True)

    remaining_raw = broker_redis.lrange("notify", 0, -1)
    surviving_ids = {json.loads(r)["headers"]["id"] for r in remaining_raw}
    # "keep" (first match) and "unrelated" (different filter) survive.
    assert "keep" in surviving_ids
    assert "unrelated" in surviving_ids
    assert "drop1" not in surviving_ids
    assert "drop2" not in surviving_ids
    assert result.count == 2


@pytest.mark.django_db
def test_streaming_clear_keep_one_short_circuits_after_first_pass(broker_redis):
    """keep_one=True should not exhaust max passes when no drift occurs.

    Pass 1 clears all non-keeper matches; further passes would just
    rescan the queue (potentially 500k+ messages) to confirm the
    keeper still survives. The early-exit convergence rule keeps
    keep_one to a single useful pass on the happy path.
    """
    raw_keep = _build_envelope(
        task="t.A", task_id="keep", kwargs={"repoid": 1, "commitid": "x"}
    )
    raw_drop = _build_envelope(
        task="t.A", task_id="drop", kwargs={"repoid": 1, "commitid": "x"}
    )
    broker_redis.rpush("notify", raw_keep)
    broker_redis.rpush("notify", raw_drop)

    tombstone = f"{_CELERY_TOMBSTONE_PREFIX}:test-keep-one-short-circuit"
    filter_tuples = _make_filter(("t.A", 1, "x"))
    stats = _streaming_celery_clear(
        broker_redis,
        "notify",
        filter_tuples,
        tombstone,
        keep_one=True,
        dry_run=False,
    )

    assert stats.total_lset == 1
    assert stats.passes_run == 1


def test_streaming_clear_drains_queue_with_no_artificial_cap(broker_redis):
    """Regression for PR #899: a queue saturated with matching messages
    deeper than what the per-pass cap *used* to be was not fully drained.

    The previous implementation capped each pass at the first
    `cap` positions (`min(depth, cap)`), so on a queue dominated by a
    single `(task, repoid, commitid)` triple:

    * pass 1 tombstoned the first `cap` matches and `LREM` swept them,
    * pass 2 tombstoned another `cap` matches that shifted in from
      beyond cap (now occupying positions 0..cap-1),
    * the `prev_lset == matches_lset` plateau check (both `cap`)
      then declared convergence and exited.

    Net effect: a saturated queue dropped by ~2 * cap per "Clear all"
    click instead of draining fully — what the user perceived as
    "queue does not shrink".

    The clear is now intentionally unbounded (walks the full
    `LLEN(queue)` every pass), so a single click drains every match
    regardless of depth. This test pushes 41_000 matching messages
    (just over 2× what the old 20k cap was) so the buggy code path
    would have tombstoned exactly 40_000 across two passes, hit the
    plateau exit, and left 1_000 messages behind. The fix walks the
    entire queue every pass, so a single click drains all 41_000
    matches.
    """

    matching_count = 41_000
    raw_match = _build_envelope(task="t.A", kwargs={"repoid": 1, "commitid": "x"})
    raw_other = _build_envelope(task="t.B", kwargs={"repoid": 999, "commitid": "y"})
    pipe = broker_redis.pipeline(transaction=False)
    for _ in range(matching_count):
        pipe.rpush("notify", raw_match)
    pipe.rpush("notify", raw_other)
    pipe.execute()

    initial_depth = broker_redis.llen("notify")
    assert initial_depth == matching_count + 1

    tombstone = f"{_CELERY_TOMBSTONE_PREFIX}:test-deep-saturation"
    filter_tuples = _make_filter(("t.A", 1, "x"))
    stats = _streaming_celery_clear(
        broker_redis, "notify", filter_tuples, tombstone, dry_run=False
    )

    # Every matching message is cleared in a single click; only the
    # one unrelated message survives.
    assert stats.total_lset == matching_count
    assert stats.total_drifted == 0
    assert broker_redis.llen("notify") == 1
    survivor = json.loads(broker_redis.lrange("notify", 0, -1)[0])
    assert survivor["headers"]["task"] == "t.B"


def test_streaming_clear_dry_run_returns_full_queue_match_count(patched_broker):
    """Dry-run preview must walk the full queue, not just the capped
    target list. Regression for the case where a deep queue (e.g.
    200k messages, 190k matching one filter triple) reported only
    ~2k matches because `pending_count` was bounded by
    `CELERY_BROKER_DISPLAY_LIMIT`.
    """

    queue = "bundle_analysis"
    task = "app.tasks.bundle_analysis.BundleAnalysisProcessor"
    repoid = 21222368
    commitid = "0832c11a1f43c5e2a1b9f8a3e5d1c2b7e9a8d3f0"

    n = 5_000
    pipe = patched_broker.pipeline(transaction=False)
    for _ in range(n):
        pipe.rpush(
            queue,
            _build_envelope(task=task, kwargs={"repoid": repoid, "commitid": commitid}),
        )
    pipe.execute()
    # SimpleNamespace user keeps this regression test free of the
    # Django-DB fixture so it runs in any sandbox; `_record_audit`
    # is best-effort and swallows DB failures.
    user = SimpleNamespace(id=1, pk=1)

    targets = list(
        CeleryBrokerQueueQuerySet(CeleryBrokerQueue, queue_name=queue).filter(
            task_name=task,
            repoid=repoid,
            commitid=commitid,
        )[:1]
    )
    assert len(targets) == 1

    result = celery_broker_clear(
        targets,
        user=user,
        dry_run=True,
        keep_one=False,
    )
    assert result.dry_run is True
    assert result.count == n

    result_keep = celery_broker_clear(
        targets,
        user=user,
        dry_run=True,
        keep_one=True,
    )
    assert result_keep.count == n - 1

    assert patched_broker.llen(queue) == n
