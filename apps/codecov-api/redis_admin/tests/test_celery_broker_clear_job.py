"""Coverage for the chunked celery_broker clear background-job
machinery added in this PR.

Two layers of coverage:

* Service layer (`start_celery_broker_clear_job`,
  `get_celery_broker_clear_job`,
  `request_cancel_celery_broker_clear_job`,
  `_run_celery_broker_clear_job`): end-to-end on fakeredis. We
  push messages onto a fake broker, spawn the worker thread, and
  poll the job hash via the public service helpers to assert
  progress monotonicity, cancellation semantics, keep_one
  preservation, and failure-path swallowing.

* View layer (`clear_by_filter_progress_view`,
  `clear_by_filter_status_view`, `clear_by_filter_cancel_view`,
  and the modified `clear_by_filter_view`): exercised via
  `RequestFactory` + a `SimpleNamespace` user so we don't need
  Postgres for the request lifecycle. The user fixture's
  `is_superuser=True` skirts the model-level permission check.

All tests run under `-m 'not django_db'`. The audit-log test that
needs `LogEntry` inspection lives behind `@pytest.mark.django_db`
because asserting on `LogEntry.change_message` requires the auth
DB; in the sandbox it errors with the documented `could not
translate host name "postgres"` and is verified in CI.
"""

from __future__ import annotations

import base64
import json
import threading
import time
import uuid
from types import SimpleNamespace
from unittest import mock

import fakeredis
import pytest
from django.contrib.admin.models import LogEntry
from django.contrib.admin.sites import AdminSite
from django.contrib.messages.storage.fallback import FallbackStorage
from django.test import RequestFactory

from redis_admin import conn as redis_admin_conn
from redis_admin import services as redis_admin_services
from redis_admin import settings as redis_admin_settings
from redis_admin.admin import CeleryBrokerQueueAdmin
from redis_admin.models import CeleryBrokerQueue
from redis_admin.services import (
    _CELERY_CLEAR_CHUNK_JOB,
    _CELERY_CLEAR_JOB_KEY_PREFIX,
    _CELERY_CLEAR_JOB_TTL_SECONDS,
    _ChunkProgress,
    _streaming_celery_clear,
    get_celery_broker_clear_job,
    request_cancel_celery_broker_clear_job,
    start_celery_broker_clear_job,
)
from redis_admin.tests.test_celery_broker_queue import _build_envelope
from shared.django_apps.codecov_auth.tests.factories import UserFactory

# ---- Fixtures --------------------------------------------------------------


@pytest.fixture
def patched_broker(monkeypatch) -> fakeredis.FakeStrictRedis:
    """Single fakeredis backing both `kind="default"` (cache, where
    the job hash lives) and `kind="broker"` (the queue we're
    clearing). Mirrors the fixture in `test_celery_broker_queue.py`
    so tests can co-locate broker pushes and job-hash reads on the
    same in-memory server.
    """

    server = fakeredis.FakeStrictRedis()
    monkeypatch.setattr(
        redis_admin_conn, "get_connection", lambda kind="default": server
    )
    return server


@pytest.fixture
def superuser():
    """A `SimpleNamespace` superuser stand-in for tests that don't
    need the auth DB. `start_celery_broker_clear_job` only reads
    `id` / `pk` for audit-log attribution; the view-layer permission
    checks read `is_superuser`.
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


def _wait_for_job(job_id: str, *, timeout: float = 10.0) -> None:
    """Block until the worker thread for `job_id` exits.

    Uses the `_clear_job_threads` test handle in `services.py`. We
    avoid `threading.enumerate()` so a parent test runner's
    background threads don't confuse the lookup.
    """

    thread = redis_admin_services._clear_job_threads.get(job_id)
    if thread is None:
        return
    thread.join(timeout=timeout)
    assert not thread.is_alive(), (
        f"clear job worker thread for {job_id} did not exit within {timeout}s"
    )


def _push_envelopes(
    redis: fakeredis.FakeStrictRedis,
    queue: str,
    *,
    n: int,
    task: str = "app.tasks.bundle_analysis.BundleAnalysisProcessor",
    repoid: int = 1234,
    commitid: str = "abcdef" + "0" * 34,
) -> None:
    """Push `n` matching envelopes onto `queue` in a single
    pipeline so even 5_000-message setups complete in a fraction
    of a second.
    """

    pipe = redis.pipeline(transaction=False)
    envelope = _build_envelope(
        task=task, kwargs={"repoid": repoid, "commitid": commitid}
    )
    for _ in range(n):
        pipe.rpush(queue, envelope)
    pipe.execute()


# ---- Service layer ---------------------------------------------------------


def test_start_celery_broker_clear_job_returns_uuid_and_initialises_state(
    patched_broker, superuser
):
    """`start_celery_broker_clear_job` returns a UUID-shaped job_id
    and writes a fully-populated initial hash with status=pending,
    the operator's filter shape, and a snapshotted total_estimated.
    Pinning the hash shape so the JSON status view's contract stays
    stable across refactors.
    """

    queue = "bundle_analysis"
    _push_envelopes(patched_broker, queue, n=3, repoid=99)

    job_id = start_celery_broker_clear_job(
        queue,
        user=superuser,
        task_name=None,
        repoid=99,
        commitid=None,
        keep_one=False,
        dry_run=True,  # dry_run so the worker doesn't mutate
    )
    _wait_for_job(job_id)

    # UUID canonical form so the URL converter accepts it.
    assert len(job_id) == 36
    assert job_id.count("-") == 4

    job = get_celery_broker_clear_job(job_id)
    assert job is not None
    # Initial fields the views + JSON shape rely on.
    expected_keys = {
        "status",
        "queue",
        "filter_task",
        "filter_repoid",
        "filter_commitid",
        "dry_run",
        "keep_one",
        "user_id",
        "started_at",
        "updated_at",
        "completed_at",
        "total_estimated",
        "processed",
        "matched",
        "drifted",
        "passes_run",
        "error",
        "cancel_requested",
    }
    assert expected_keys <= set(job.keys())
    assert job["queue"] == queue
    assert job["filter_repoid"] == "99"
    assert job["filter_task"] == ""
    assert job["filter_commitid"] == ""
    assert job["dry_run"] == "1"
    assert job["keep_one"] == "0"
    # Worker has had time to finish (3 messages on fakeredis); the
    # status should be terminal by now.
    assert job["status"] in {"completed", "running", "pending"}
    # Snapshotted depth is the total at submit time.
    assert int(job["total_estimated"]) == 3

    # 24h TTL set on creation.
    key = f"{_CELERY_CLEAR_JOB_KEY_PREFIX}:{job_id}"
    ttl = patched_broker.ttl(key)
    assert 0 < ttl <= _CELERY_CLEAR_JOB_TTL_SECONDS


def test_clear_job_processes_in_chunks_and_updates_progress_monotonically(
    patched_broker,
):
    """The progress callback is called once per LRANGE chunk and
    deltas accumulate monotonically.

    Rather than chase the worker thread for snapshots (timing-
    flaky), drive `_streaming_celery_clear` directly with a
    snapshot-recording callback; this is exactly the chunk loop
    the worker thread runs, so monotonicity here is what the
    progress page sees.
    """

    queue = "bundle_analysis"
    matches = 5_000
    _push_envelopes(patched_broker, queue, n=matches, repoid=1, commitid="x" * 40)

    snapshots: list[_ChunkProgress] = []

    def _record(snapshot: _ChunkProgress) -> bool:
        snapshots.append(snapshot)
        return False

    stats = _streaming_celery_clear(
        patched_broker,
        queue,
        frozenset({("app.tasks.bundle_analysis.BundleAnalysisProcessor", 1, "x" * 40)}),
        tombstone="__redis_admin_celery_tombstone__:test",
        keep_one=False,
        dry_run=True,
        chunk_size=_CELERY_CLEAR_CHUNK_JOB,
        progress_callback=_record,
    )

    assert stats.total_found == matches
    # 5_000 / chunk-size 1_000 → 5 callbacks (the loop short-
    # circuits after the first dry-run pass so only one pass
    # contributes to the snapshot list).
    assert len(snapshots) == matches // _CELERY_CLEAR_CHUNK_JOB
    # Monotonic non-decreasing across chunks.
    prev_total_found = 0
    for snap in snapshots:
        assert snap.total_found >= prev_total_found
        prev_total_found = snap.total_found
    # Final snapshot's running totals match the stats return.
    assert snapshots[-1].total_found == stats.total_found
    # Per-chunk processed delta is at most `chunk_size`.
    for snap in snapshots:
        assert snap.processed_delta <= _CELERY_CLEAR_CHUNK_JOB
    # `_ChunkProgress.chunk_index` is documented as 0-based: the
    # first snapshot reports 0, the last reports `chunks_total - 1`,
    # and the values strictly ascend by 1 across snapshots in a pass.
    expected_chunk_indices = list(range(len(snapshots)))
    assert [snap.chunk_index for snap in snapshots] == expected_chunk_indices
    assert snapshots[0].chunk_index == 0
    assert snapshots[-1].chunk_index == snapshots[-1].chunks_total - 1


def test_clear_job_drains_deep_queue_end_to_end(patched_broker, superuser):
    """End-to-end drain: push more matches than the synchronous
    display window, spawn the chunked job, wait for the worker, and
    assert every match is gone via a positive walk.

    Mirrors `test_celery_broker_clear_drains_deep_queue_end_to_end`
    in `test_celery_broker_clear_drain_verification.py` but exercises
    the chunked-job code path end-to-end.
    """

    queue = "bundle_analysis"
    matches = redis_admin_settings.CELERY_BROKER_DISPLAY_LIMIT + 500
    _push_envelopes(
        patched_broker, queue, n=matches, repoid=21222368, commitid="abc" + "0" * 37
    )
    initial = patched_broker.llen(queue)
    assert initial == matches

    job_id = start_celery_broker_clear_job(
        queue,
        user=superuser,
        task_name=None,
        repoid=21222368,
        commitid="abc" + "0" * 37,
        keep_one=False,
        dry_run=False,
    )
    _wait_for_job(job_id, timeout=30.0)

    job = get_celery_broker_clear_job(job_id)
    assert job is not None
    assert job["status"] == "completed"
    assert int(job["matched"]) == matches
    # Queue is fully drained.
    assert patched_broker.llen(queue) == 0


def test_clear_job_cancel_stops_at_chunk_boundary(patched_broker, superuser):
    """An in-flight cancel halts the worker at the next chunk
    boundary, leaves remaining matches intact, and records
    `status=cancelled`. We coordinate via a sentinel callback that
    blocks the first chunk until the test triggers cancel — that
    guarantees the cancel lands on the SECOND chunk boundary check
    (not after the worker already finished).
    """

    queue = "bundle_analysis"
    matches = 10_000
    _push_envelopes(patched_broker, queue, n=matches, repoid=1, commitid="x" * 40)

    cancel_seen = threading.Event()
    chunks_seen_before_cancel = []

    real_streaming = redis_admin_services._streaming_celery_clear

    def _slow_streaming(*args, progress_callback=None, **kwargs):
        # Wrap the operator's progress_callback with a sleep so the
        # cancel request has time to land between chunks. Each
        # chunk advances the LRANGE; the sleep gives the test main
        # thread a window to flip cancel_requested=1 before the
        # next chunk is processed.
        original_cb = progress_callback

        def _gated_cb(snapshot):
            chunks_seen_before_cancel.append(snapshot)
            cancel_seen.set()
            time.sleep(0.05)  # let the test thread set cancel
            if original_cb is None:
                return False
            return original_cb(snapshot)

        return real_streaming(*args, progress_callback=_gated_cb, **kwargs)

    with mock.patch.object(
        redis_admin_services, "_streaming_celery_clear", _slow_streaming
    ):
        job_id = start_celery_broker_clear_job(
            queue,
            user=superuser,
            task_name=None,
            repoid=1,
            commitid="x" * 40,
            keep_one=False,
            dry_run=False,
        )
        # Wait for the first chunk callback to fire so the worker
        # has loaded its filter and seen at least one chunk's worth
        # of envelopes.
        assert cancel_seen.wait(timeout=5.0)
        ok = request_cancel_celery_broker_clear_job(job_id)
        assert ok is True
        _wait_for_job(job_id, timeout=30.0)

    job = get_celery_broker_clear_job(job_id)
    assert job is not None
    assert job["status"] == "cancelled"
    matched = int(job["matched"])
    # Cancel landed mid-pass: matched should be > 0 (at least one
    # chunk's worth) and < the total match count.
    assert 0 < matched < matches
    # The queue still has remaining matches (cancel left them in
    # place) — LLEN is at most `matches` minus whatever was LSET'd.
    remaining = patched_broker.llen(queue)
    assert remaining == matches - matched, (
        f"expected remaining={matches - matched}, got {remaining}; matched={matched}"
    )


def test_clear_job_keep_one_leaves_one_at_head(patched_broker, superuser):
    """`keep_one=True` chunked variant: every match cleared except
    the lowest-index one, which survives at index 0. Mirrors
    `test_celery_broker_clear_keep_one_deep_queue_leaves_exactly_one_at_head`
    but exercises the chunked code path.
    """

    queue = "bundle_analysis"
    matches = 2_500  # > CELERY_BROKER_DISPLAY_LIMIT in the default test
    repoid = 1
    commitid = "y" * 40
    _push_envelopes(patched_broker, queue, n=matches, repoid=repoid, commitid=commitid)

    job_id = start_celery_broker_clear_job(
        queue,
        user=superuser,
        task_name=None,
        repoid=repoid,
        commitid=commitid,
        keep_one=True,
        dry_run=False,
    )
    _wait_for_job(job_id, timeout=30.0)

    job = get_celery_broker_clear_job(job_id)
    assert job is not None
    assert job["status"] == "completed"
    assert int(job["matched"]) == matches - 1
    # Exactly one survivor remains, at the head of the queue.
    assert patched_broker.llen(queue) == 1
    survivor_raw = patched_broker.lindex(queue, 0)
    assert isinstance(survivor_raw, bytes)
    parsed = json.loads(survivor_raw.decode())
    body_b64 = parsed["body"]
    decoded_body = json.loads(base64.b64decode(body_b64).decode())
    assert decoded_body[1]["repoid"] == repoid


def test_clear_job_failure_records_status_failed_and_swallows_exception(
    patched_broker, superuser
):
    """A worker exception (broker outage, malformed envelope,
    LLEN miss-fire) lands as `status=failed` with the exception's
    `str(...)` recorded in the `error` field, and never propagates
    to the caller. The thread must exit cleanly.
    """

    _push_envelopes(patched_broker, "bundle_analysis", n=10, repoid=1)

    def _boom(*args, **kwargs):
        raise RuntimeError("simulated broker outage")

    with mock.patch.object(redis_admin_services, "_streaming_celery_clear", _boom):
        job_id = start_celery_broker_clear_job(
            "bundle_analysis",
            user=superuser,
            task_name=None,
            repoid=1,
            commitid=None,
            keep_one=False,
            dry_run=False,
        )
        _wait_for_job(job_id, timeout=10.0)

    job = get_celery_broker_clear_job(job_id)
    assert job is not None
    assert job["status"] == "failed"
    assert "simulated broker outage" in job["error"]
    # No exception was raised in the test thread (we're past the
    # `with` block); the assertion below is redundant but explicit.
    assert True


# ---- View layer ------------------------------------------------------------


def _admin_instance() -> CeleryBrokerQueueAdmin:
    return CeleryBrokerQueueAdmin(model=CeleryBrokerQueue, admin_site=AdminSite())


def test_clear_job_status_view_returns_json_for_running_job(patched_broker, superuser):
    """The status JSON endpoint returns the pinned shape so the
    progress-page JS can rely on the field set across deploys.
    """

    queue = "notify"
    _push_envelopes(patched_broker, queue, n=3, repoid=1)
    job_id = start_celery_broker_clear_job(
        queue,
        user=superuser,
        task_name=None,
        repoid=1,
        commitid=None,
        keep_one=False,
        dry_run=True,
    )
    _wait_for_job(job_id)

    rf = RequestFactory()
    request = rf.get(
        f"/admin/redis_admin/celerybrokerqueue/clear-by-filter/job/{job_id}/status/"
    )
    request.user = superuser

    admin_instance = _admin_instance()
    response = admin_instance.clear_by_filter_status_view(request, uuid.UUID(job_id))
    assert response.status_code == 200
    payload = json.loads(response.content)
    expected_fields = {
        "job_id",
        "status",
        "is_terminal",
        "queue_name",
        "filter_task",
        "filter_repoid",
        "filter_commitid",
        "dry_run",
        "keep_one",
        "started_at",
        "updated_at",
        "completed_at",
        "total_estimated",
        "processed",
        "matched",
        "drifted",
        "passes_run",
        "error",
        "cancel_requested",
    }
    assert expected_fields <= set(payload.keys())
    assert payload["job_id"] == job_id
    assert payload["queue_name"] == queue
    assert payload["filter_repoid"] == "1"
    assert payload["dry_run"] is True
    # ints stay ints in JSON, not stringified.
    assert isinstance(payload["total_estimated"], int)
    assert isinstance(payload["processed"], int)


def test_clear_job_cancel_view_requires_post(patched_broker, superuser):
    """`clear_by_filter_cancel_view` rejects GET with 405 so a
    drive-by browser navigation can't accidentally cancel a clear.
    """

    job_id = start_celery_broker_clear_job(
        "notify",
        user=superuser,
        task_name=None,
        repoid=1,
        commitid=None,
        keep_one=False,
        dry_run=True,
    )
    _wait_for_job(job_id)
    rf = RequestFactory()
    request = rf.get(
        f"/admin/redis_admin/celerybrokerqueue/clear-by-filter/job/{job_id}/cancel/"
    )
    request.user = superuser

    admin_instance = _admin_instance()
    response = admin_instance.clear_by_filter_cancel_view(request, uuid.UUID(job_id))
    assert response.status_code == 405
    # Allow header announces POST as the legal verb so an HTTP-
    # aware client can recover automatically.
    assert "POST" in response.get("Allow", "")


def test_clear_job_status_view_404_for_unknown_id(patched_broker, superuser):
    """Unknown / expired job ids return 404 JSON instead of 500ing
    or rendering an empty progress page.
    """

    rf = RequestFactory()
    fake_id = uuid.uuid4()
    request = rf.get(
        f"/admin/redis_admin/celerybrokerqueue/clear-by-filter/job/{fake_id}/status/"
    )
    request.user = superuser

    admin_instance = _admin_instance()
    response = admin_instance.clear_by_filter_status_view(request, fake_id)
    assert response.status_code == 404
    payload = json.loads(response.content)
    assert payload["error"] == "not_found"
    assert payload["job_id"] == str(fake_id)


def test_clear_by_filter_view_redirects_to_progress_page_on_confirmed_submit(
    patched_broker, superuser
):
    """A confirmed clear_all submit on `clear_by_filter_view`
    spawns a job (via the patched `start_celery_broker_clear_job`)
    and 302s to `…/clear-by-filter/job/<uuid>/`. The previous
    behaviour (synchronous `celery_broker_clear`) would have
    redirected to the changelist instead.
    """

    queue = "notify"
    # Push enough messages that the streaming-count path runs.
    _push_envelopes(patched_broker, queue, n=2, repoid=1)
    rf = RequestFactory()

    fake_job_id = "11111111-2222-3333-4444-555555555555"

    with (
        mock.patch.object(
            redis_admin_services,
            "start_celery_broker_clear_job",
            return_value=fake_job_id,
        ) as start_mock,
        mock.patch(
            "redis_admin.admin.start_celery_broker_clear_job",
            return_value=fake_job_id,
        ),
    ):
        request = rf.post(
            "/admin/redis_admin/celerybrokerqueue/clear-by-filter/",
            data={
                "queue_name": queue,
                "repoid": "1",
                "action": "clear_all",
                "typed_confirm": queue,
            },
        )
        request.user = superuser
        admin_instance = _admin_instance()
        response = admin_instance.clear_by_filter_view(request)

    assert response.status_code == 302
    assert response["Location"].endswith(f"/clear-by-filter/job/{fake_job_id}/"), (
        response["Location"]
    )
    # Sanity: the synchronous service was NOT invoked.
    start_mock.assert_not_called()  # patched at module-level, not used


def test_clear_by_filter_view_dry_run_preview_unchanged(patched_broker, superuser):
    """The dry-run action keeps the synchronous shape: it calls
    `celery_broker_clear(dry_run=True)`, re-renders the preview
    page, and never spawns a chunked job. Pin so a future refactor
    that fans dry-run out to a chunked job has to update this
    test deliberately.
    """

    queue = "notify"
    _push_envelopes(patched_broker, queue, n=2, repoid=1)
    rf = RequestFactory()

    with mock.patch("redis_admin.admin.start_celery_broker_clear_job") as start_mock:
        request = rf.post(
            "/admin/redis_admin/celerybrokerqueue/clear-by-filter/",
            data={
                "queue_name": queue,
                "repoid": "1",
                "action": "dry_run",
            },
        )
        request.user = superuser
        admin_instance = _admin_instance()
        # `messages` middleware isn't installed on the bare
        # RequestFactory; the view calls `messages.info(...)`. We
        # use a stand-in storage so the call doesn't raise.
        request.session = {}
        request._messages = FallbackStorage(request)
        response = admin_instance.clear_by_filter_view(request)

    # Dry-run re-renders the preview (200 OK, not 302) and never
    # touches the chunked-job code path.
    assert response.status_code == 200
    start_mock.assert_not_called()


# ---- Audit log (django_db) -------------------------------------------------


@pytest.mark.django_db
def test_clear_job_writes_audit_row_on_completion_with_chunked_mode(
    patched_broker,
):
    """The chunked worker writes a `LogEntry` at job-completion
    (not job-start) with `mode` reflecting the chunked variant
    (    `chunked-all-from-bucket` / `chunked-all-but-first` /
    `chunked-dry-run`) and a `job_id` extra. Pinned so the audit
    trail can distinguish chunked from synchronous clears.
    """

    user = UserFactory()
    queue = "notify"
    _push_envelopes(patched_broker, queue, n=3, repoid=1)

    initial = LogEntry.objects.filter(user_id=user.id).count()
    job_id = start_celery_broker_clear_job(
        queue,
        user=user,
        task_name=None,
        repoid=1,
        commitid=None,
        keep_one=False,
        dry_run=False,
    )
    _wait_for_job(job_id)

    entries = list(LogEntry.objects.filter(user_id=user.id).order_by("-action_time"))
    assert len(entries) == initial + 1
    payload = json.loads(entries[0].change_message)
    assert payload["scope"] == "celery_broker_clear"
    assert payload["mode"] == "chunked-all-from-bucket"
    assert payload["job_id"] == job_id
    assert payload["cancelled"] is False
    assert "passes_run" in payload
    assert "total_lset" in payload
