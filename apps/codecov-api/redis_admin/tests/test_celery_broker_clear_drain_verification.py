"""Defensive end-to-end coverage for the `celery_broker_clear` /
`_streaming_celery_clear` drain semantics on deep queues.

These tests sit in their own module to insulate them from churn on
the much larger `test_celery_broker_queue.py` suite -- the
clear-queue path has regressed twice on this PR (#904 and #903's
synthetic-target fix) and we want a small, focused file that:

* asserts post-call `LLEN` arithmetic explicitly (initial - matches),
* walks the full queue post-call with `parse_celery_envelope` to
  *positively* prove no targeted envelope survives, and
* exercises queues with `LLEN > CELERY_BROKER_DISPLAY_LIMIT` so
  any future regression that re-introduces a queryset-bounded scan
  fails immediately.

All tests use `fakeredis` and `SimpleNamespace` so they run under
`-m 'not django_db'` (the `_record_audit` helper inside
`celery_broker_clear` swallows DB failures, which keeps the
audit-log write best-effort and lets these tests skip the
Postgres-bound `TestCase` machinery).
"""

from __future__ import annotations

from types import SimpleNamespace

import fakeredis
import pytest

from redis_admin import conn as redis_admin_conn
from redis_admin import settings as redis_admin_settings
from redis_admin.families import parse_celery_envelope
from redis_admin.models import CeleryBrokerQueue
from redis_admin.services import celery_broker_clear
from redis_admin.tests.test_celery_broker_queue import _build_envelope


@pytest.fixture
def patched_broker(monkeypatch) -> fakeredis.FakeStrictRedis:
    """Route both cache and broker `get_connection` to one fakeredis,
    matching the fixture used by `test_celery_broker_queue.py`.
    """

    server = fakeredis.FakeStrictRedis()
    monkeypatch.setattr(
        redis_admin_conn, "get_connection", lambda kind="default": server
    )
    return server


def _count_matches_in_queue(
    redis,
    queue: str,
    *,
    task: str,
    repoid: int,
    commitid: str,
) -> list[int]:
    """Walk the entire queue post-clear and return the indexes of
    every envelope whose `(task, repoid, commitid)` triple matches.

    Used by the deep-queue regression tests to *positively* verify
    "no match remains" rather than just inferring it from `LLEN`
    arithmetic. Catches the family of regressions where the
    streaming clear silently leaves matches behind beyond a capped
    scan window.
    """

    raw_items = redis.lrange(queue, 0, -1)
    matches: list[int] = []
    for idx, raw in enumerate(raw_items):
        meta = parse_celery_envelope(raw if isinstance(raw, bytes) else raw.encode())
        if (meta.task, meta.repoid, meta.commitid) == (task, repoid, commitid):
            matches.append(idx)
    return matches


def test_celery_broker_clear_drains_deep_queue_end_to_end(patched_broker):
    """`celery_broker_clear(dry_run=False, keep_one=False)` on a
    queue deeper than `CELERY_BROKER_DISPLAY_LIMIT` must drain every
    matching envelope.

    Reproduces the "Clear queue" admin action's exact wire-up from
    `clear_by_filter_view` (synthetic single-target with the
    operator's filter triple) and asserts both:

    * `LLEN(queue) == initial_total - matches` (here: initial_total
      == matches, so `LLEN == 0`), AND
    * walking the entire queue post-call finds zero envelopes
      whose `(task, repoid, commitid)` triple still matches the
      filter.

    This is the core regression-class test for the first failure
    mode the user got burned by on PR #903 / fixed in PR #904: the
    streaming clear walking a capped window instead of the full
    `LLEN(queue)`.
    """

    queue = "bundle_analysis"
    task = "app.tasks.bundle_analysis.BundleAnalysisProcessor"
    repoid = 21222368
    commitid = "0832c11a1f43c5e2a1b9f8a3e5d1c2b7e9a8d3f0"

    # `+ 500` so the queue is comfortably deeper than the
    # display window the queryset path would have materialised.
    matches = redis_admin_settings.CELERY_BROKER_DISPLAY_LIMIT + 500
    pipe = patched_broker.pipeline(transaction=False)
    for _ in range(matches):
        pipe.rpush(
            queue,
            _build_envelope(task=task, kwargs={"repoid": repoid, "commitid": commitid}),
        )
    pipe.execute()
    initial_total = patched_broker.llen(queue)
    assert initial_total == matches
    user = SimpleNamespace(id=1, pk=1)

    synthetic_target = CeleryBrokerQueue(
        queue_name=queue,
        task_name=task,
        repoid=repoid,
        commitid=commitid,
        index_in_queue=None,
    )

    result = celery_broker_clear(
        [synthetic_target],
        user=user,
        dry_run=False,
        keep_one=False,
    )

    assert result.dry_run is False
    assert result.count == matches
    # LLEN dropped by exactly `matches` (here: every envelope was a
    # match, so the queue is empty).
    assert patched_broker.llen(queue) == initial_total - matches == 0
    # Positive walk: zero remaining matches.
    surviving = _count_matches_in_queue(
        patched_broker, queue, task=task, repoid=repoid, commitid=commitid
    )
    assert surviving == [], f"expected zero remaining matches, got {len(surviving)}"


def test_celery_broker_clear_keep_one_deep_queue_leaves_exactly_one_at_head(
    patched_broker,
):
    """`celery_broker_clear(keep_one=True)` on a queue deeper than
    `CELERY_BROKER_DISPLAY_LIMIT` must leave *exactly one* matching
    message, at the head of the queue.

    The existing `test_streaming_clear_keep_one_leaves_lowest_index`
    runs against 4 messages, well within the display window, so it
    can't catch a regression that re-introduces a queryset-bounded
    scan -- on a deep queue, that bug would either skip the keeper
    (clearing all matches) or short-circuit before reaching the
    tail. This test pins the "deep queue + survivor at index 0"
    invariant.
    """

    queue = "bundle_analysis"
    task = "app.tasks.bundle_analysis.BundleAnalysisProcessor"
    repoid = 21222368
    commitid = "0832c11a1f43c5e2a1b9f8a3e5d1c2b7e9a8d3f0"

    matches = redis_admin_settings.CELERY_BROKER_DISPLAY_LIMIT + 100
    pipe = patched_broker.pipeline(transaction=False)
    for _ in range(matches):
        pipe.rpush(
            queue,
            _build_envelope(task=task, kwargs={"repoid": repoid, "commitid": commitid}),
        )
    pipe.execute()
    initial_total = patched_broker.llen(queue)
    assert initial_total == matches
    user = SimpleNamespace(id=1, pk=1)

    synthetic_target = CeleryBrokerQueue(
        queue_name=queue,
        task_name=task,
        repoid=repoid,
        commitid=commitid,
        index_in_queue=None,
    )

    result = celery_broker_clear(
        [synthetic_target],
        user=user,
        dry_run=False,
        keep_one=True,
    )

    # `result.count` reports cleared (not surviving) -- one match
    # is preserved per queue with at least one match.
    assert result.count == matches - 1
    # LLEN dropped by exactly the cleared count.
    assert patched_broker.llen(queue) == initial_total - (matches - 1) == 1
    # Positive walk: exactly one survivor, sitting at index 0
    # (head-of-queue, which is the next message a Celery worker
    # would BLPOP).
    surviving = _count_matches_in_queue(
        patched_broker, queue, task=task, repoid=repoid, commitid=commitid
    )
    assert surviving == [0], (
        f"expected exactly one surviving match at index 0, got {surviving}"
    )


def test_celery_broker_clear_mixed_content_deep_queue_clears_only_filter(
    patched_broker,
):
    """End-to-end clear on a deep queue with three task buckets must
    drain every envelope matching the filter triple A and leave
    every non-matching envelope (B and C) intact, regardless of
    position in the queue.

    Three buckets, interleaved through the queue:

    * A (`(task_a, repoid_a, commitid_a)`): the targeted filter
      triple. `n_a` copies (> CELERY_BROKER_DISPLAY_LIMIT) so any
      cap-bounded clear would silently leave matches in the tail.
    * B (`(task_b, repoid_a, commitid_a)`): same repoid + commitid
      as A but a different task. The synthetic target's filter
      triple includes `task_name`, so B must not be tombstoned.
    * C (`(task_a, repoid_c, commitid_c)`): same task as A but a
      different repoid + commitid. Must not be tombstoned.

    B and C are seeded both before A (head-of-queue) and after A
    (tail-of-queue, past the display window) so the clear can't
    get a free pass by stopping early or by accidentally walking
    only the head slice.
    """

    queue = "bundle_analysis"
    task_a = "app.tasks.bundle_analysis.BundleAnalysisProcessor"
    repoid_a = 21222368
    commitid_a = "0832c11a1f43c5e2a1b9f8a3e5d1c2b7e9a8d3f0"
    task_b = "app.tasks.notify.NotifyTask"
    repoid_c = 99999
    commitid_c = "ffffffffffffffffffffffffffffffffffffffff"

    n_a = redis_admin_settings.CELERY_BROKER_DISPLAY_LIMIT + 500
    head_b = head_c = 25
    tail_b = tail_c = 25
    n_b = head_b + tail_b
    n_c = head_c + tail_c

    envelope_a = _build_envelope(
        task=task_a, kwargs={"repoid": repoid_a, "commitid": commitid_a}
    )
    envelope_b = _build_envelope(
        task=task_b, kwargs={"repoid": repoid_a, "commitid": commitid_a}
    )
    envelope_c = _build_envelope(
        task=task_a, kwargs={"repoid": repoid_c, "commitid": commitid_c}
    )

    pipe = patched_broker.pipeline(transaction=False)
    for _ in range(head_b):
        pipe.rpush(queue, envelope_b)
        pipe.rpush(queue, envelope_c)
    for _ in range(n_a):
        pipe.rpush(queue, envelope_a)
    for _ in range(tail_b):
        pipe.rpush(queue, envelope_b)
        pipe.rpush(queue, envelope_c)
    pipe.execute()

    initial_total = patched_broker.llen(queue)
    assert initial_total == n_a + n_b + n_c
    user = SimpleNamespace(id=1, pk=1)

    synthetic_target = CeleryBrokerQueue(
        queue_name=queue,
        task_name=task_a,
        repoid=repoid_a,
        commitid=commitid_a,
        index_in_queue=None,
    )

    result = celery_broker_clear(
        [synthetic_target],
        user=user,
        dry_run=False,
        keep_one=False,
    )

    assert result.count == n_a
    # LLEN dropped by exactly `n_a`; every B + C remains.
    assert patched_broker.llen(queue) == initial_total - n_a == n_b + n_c
    # Positive walks: zero A survivors, every B + C survives.
    surviving_a = _count_matches_in_queue(
        patched_broker, queue, task=task_a, repoid=repoid_a, commitid=commitid_a
    )
    surviving_b = _count_matches_in_queue(
        patched_broker, queue, task=task_b, repoid=repoid_a, commitid=commitid_a
    )
    surviving_c = _count_matches_in_queue(
        patched_broker, queue, task=task_a, repoid=repoid_c, commitid=commitid_c
    )
    assert surviving_a == [], (
        f"expected zero A survivors, got {len(surviving_a)} at indexes {surviving_a[:5]}"
    )
    assert len(surviving_b) == n_b, (
        f"expected {n_b} B survivors, got {len(surviving_b)}"
    )
    assert len(surviving_c) == n_c, (
        f"expected {n_c} C survivors, got {len(surviving_c)}"
    )
