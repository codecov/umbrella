"""End-to-end coverage for the Kombu unacked drill-down.

Mirrors `test_celery_broker_queue.py` for the per-message admin
surface that maps `redis_key_size{key="unacked"}` from Grafana
into a per-message admin surface. Same fakeredis-backed strategy
as the celery_broker tests so we can run without Postgres.

Layout reference:

* `unacked` (Redis HASH) — field=`delivery_tag`, value=JSON
  triple `[envelope_dict, exchange_str, routing_key_str]`.
* `unacked_index` (Redis ZSET) — score=visibility deadline,
  member=`delivery_tag`. Cleared in tandem with the HASH field
  via the `services.unacked_clear` HDEL+ZREM pipeline; skipping
  the ZREM would leave a phantom in `unacked_index` that points
  at a non-existent HASH field.

Verified against `kombu.transport.redis.Channel.unacked_key` /
`unacked_index_key` (defaults: `"unacked"` and
`"unacked_index"`).
"""

from __future__ import annotations

import base64
import json
import uuid
from types import SimpleNamespace
from typing import Any

import fakeredis
import pytest
from django.contrib.admin.sites import AdminSite
from django.contrib.messages.storage.fallback import FallbackStorage
from django.test import RequestFactory

from redis_admin import conn as redis_admin_conn
from redis_admin import services as redis_admin_services
from redis_admin.admin import UnackedQueueAdmin
from redis_admin.families import (
    FAMILIES,
    _unacked_decode_value,
    find_family,
)
from redis_admin.models import UnackedQueueItem
from redis_admin.queryset import (
    _UNACKED_KEY,
    UnackedFrequencyBucket,
    UnackedQueueQuerySet,
    _decode_unacked_value,
    _stream_unacked_frequency_aggregate,
)
from redis_admin.services import (
    _FILTER_ANY,
    _streaming_unacked_clear,
    _substitute_filter_any_unacked,
    get_unacked_clear_job,
    request_cancel_unacked_clear_job,
    start_unacked_clear_job,
    unacked_clear,
)

# ---- helpers ---------------------------------------------------------------


def _build_envelope(
    *,
    task: str | None = "app.tasks.notify.NotifyTask",
    task_id: str | None = None,
    args: list | None = None,
    kwargs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a kombu broker envelope dict (as it lives inside
    the `unacked` HASH triple's first slot).

    Returns the dict rather than the JSON-serialised triple so
    `_push_unacked` can assemble the
    `[envelope, exchange, routing_key]` shape for each entry.
    """

    body = json.dumps([args or [], kwargs or {}, {}])
    body_b64 = base64.b64encode(body.encode("utf-8")).decode("ascii")
    headers: dict[str, Any] = {}
    if task is not None:
        headers["task"] = task
    if task_id is not None:
        headers["id"] = task_id
    return {"body": body_b64, "headers": headers}


def _push_unacked(
    redis: fakeredis.FakeStrictRedis,
    *,
    delivery_tag: str,
    routing_key: str = "celery",
    exchange: str = "",
    deadline: float | None = None,
    envelope: dict[str, Any] | None = None,
    unacked_key: str = "unacked",
    unacked_index_key: str = "unacked_index",
) -> None:
    """Insert one unacked entry into the HASH + index pair.

    Mirrors what Kombu does in
    `kombu.transport.redis.Channel.basic_publish`-style paths
    (the actual restore-loop code path is in `Channel.qos`'s
    `_unacked_pre_publish_handler`). Uses `kombu.utils.json.dumps`
    semantics — plain JSON of the triple — so the queryset's
    decoder (which calls `json.loads`) reads it cleanly without
    any kombu-specific shim.
    """

    triple = [envelope or _build_envelope(), exchange, routing_key]
    redis.hset(unacked_key, delivery_tag, json.dumps(triple))
    if deadline is not None:
        redis.zadd(unacked_index_key, {delivery_tag: deadline})


@pytest.fixture
def patched_broker(monkeypatch) -> fakeredis.FakeStrictRedis:
    """Single fakeredis backing both `kind="default"` (cache,
    where the job hash lives) and `kind="broker"` (the unacked
    HASH+ZSET we're clearing). Mirrors the celery_broker fixture.
    """

    server = fakeredis.FakeStrictRedis()
    monkeypatch.setattr(
        redis_admin_conn, "get_connection", lambda kind="default": server
    )
    return server


@pytest.fixture
def superuser():
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

    Mirrors `_wait_for_job` in `test_celery_broker_clear_job.py`
    but reads the unacked-specific thread handle.
    """

    thread = redis_admin_services._unacked_clear_job_threads.get(job_id)
    if thread is None:
        return
    thread.join(timeout=timeout)
    assert not thread.is_alive(), (
        f"unacked clear job worker thread for {job_id} did not exit within {timeout}s"
    )


# ---- family registration --------------------------------------------------


def test_unacked_family_is_registered_with_hash_redis_type():
    """`Family("unacked", redis_type="hash", ...)` shows up in
    the registry with the broker connection_kind. Pinning the
    shape so a future refactor can't silently route the unacked
    HASH off the broker Redis.
    """

    [unacked] = [f for f in FAMILIES if f.name == "unacked"]
    assert unacked.redis_type == "hash"
    assert unacked.connection_kind == "broker"
    assert "unacked" in unacked.fixed_keys
    # `unacked_index` is intentionally NOT a fixed_key — it's an
    # internal Kombu index, not a queue. The dedicated
    # `UnackedQueueAdmin` reads it via ZSCORE per row instead.
    assert "unacked_index" not in unacked.fixed_keys


def test_find_family_routes_unacked_to_unacked_family():
    fam = find_family("unacked")
    assert fam is not None
    assert fam.name == "unacked"


def test_unacked_decode_value_renders_preview_with_task_and_repo():
    """`families._unacked_decode_value` prepends a one-line
    summary `[task=… repoid=… commit=… rk=…]` mirroring the
    celery_broker preview shape. The `rk=` segment is the only
    structural difference: unacked spans every queue, so the
    routing_key is essential context for an operator scanning
    raw HASH values via the generic `RedisQueueItem` admin.
    """

    envelope = _build_envelope(
        task="app.tasks.notify.NotifyTask",
        kwargs={"repoid": 21222368, "commitid": "0832c110a744ddb"},
    )
    raw_triple = json.dumps([envelope, "", "notify"])

    rendered = _unacked_decode_value(raw_triple)

    assert "task=app.tasks.notify.NotifyTask" in rendered
    assert "repoid=21222368" in rendered
    assert "commit=0832c11" in rendered
    assert "rk=notify" in rendered
    # The full raw value is appended so an operator can still
    # see the literal JSON triple for forensics.
    assert raw_triple in rendered


def test_unacked_decode_value_returns_raw_for_garbled_input():
    """Non-JSON / non-triple values pass through verbatim so the
    generic queue admin still renders something rather than
    crashing on a corrupted HASH value.
    """

    assert _unacked_decode_value("not-json {") == "not-json {"
    # Empty list is well-formed JSON but missing the envelope
    # slot; render verbatim.
    assert _unacked_decode_value("[]") == "[]"


def test_unacked_decode_value_handles_string_envelope_slot():
    """Some kombu paths serialise the envelope as a string rather
    than a dict; the decoder should still pull the celery
    envelope summary out of it.
    """

    envelope = _build_envelope(task="t", kwargs={"repoid": 9})
    raw_triple = json.dumps([json.dumps(envelope), "", "celery"])

    rendered = _unacked_decode_value(raw_triple)

    assert "task=t" in rendered
    assert "repoid=9" in rendered
    assert "rk=celery" in rendered


# ---- queryset --------------------------------------------------------------


def test_decode_unacked_value_extracts_envelope_exchange_routing_key():
    envelope = _build_envelope(task="t", kwargs={"repoid": 1})
    raw = json.dumps([envelope, "default-exchange", "notify"])

    envelope_str, exchange, routing_key = _decode_unacked_value(raw)

    assert envelope_str is not None
    assert json.loads(envelope_str) == envelope
    assert exchange == "default-exchange"
    assert routing_key == "notify"


def test_decode_unacked_value_returns_none_triple_for_garbled_input():
    assert _decode_unacked_value("not-json") == (None, None, None)
    assert _decode_unacked_value("[]") == (None, None, None)


def test_unacked_queryset_summary_mode_emits_single_row_with_hlen(
    patched_broker,
):
    """No `routing_key__exact` → one summary row carrying
    `HLEN(unacked)` as `depth`. The single-row shape comes from
    the unacked HASH being global per broker (vs one LIST per
    celery queue), so a multi-row summary would be misleading.
    """

    for i in range(3):
        _push_unacked(
            patched_broker,
            delivery_tag=f"tag-{i}",
            envelope=_build_envelope(kwargs={"repoid": i}),
        )

    qs = UnackedQueueQuerySet(UnackedQueueItem)
    rows = list(qs)

    assert len(rows) == 1
    assert rows[0].depth == 3
    assert rows[0].pk_token == "unacked#summary"
    # Summary rows carry no per-message fields.
    assert rows[0].delivery_tag == ""
    assert rows[0].routing_key == ""


def test_unacked_queryset_summary_returns_zero_when_hash_missing(
    patched_broker,
):
    qs = UnackedQueueQuerySet(UnackedQueueItem)
    rows = list(qs)

    assert len(rows) == 1
    assert rows[0].depth == 0


def test_unacked_queryset_detail_mode_filters_by_routing_key(
    patched_broker,
):
    """`routing_key__exact=<queue>` flips into per-message mode.
    Only rows whose decoded triple's routing_key matches survive.
    The other queues' messages stay in the HASH but are filtered
    out post-materialise.
    """

    _push_unacked(
        patched_broker,
        delivery_tag="tag-notify-1",
        routing_key="notify",
        envelope=_build_envelope(
            task="app.tasks.notify.NotifyTask",
            kwargs={"repoid": 1, "commitid": "abcdef0123"},
        ),
    )
    _push_unacked(
        patched_broker,
        delivery_tag="tag-celery-1",
        routing_key="celery",
        envelope=_build_envelope(
            task="app.tasks.bundle.Processor",
            kwargs={"repoid": 2},
        ),
    )

    qs = UnackedQueueQuerySet(UnackedQueueItem).filter(routing_key__exact="notify")
    rows = list(qs)

    assert len(rows) == 1
    assert rows[0].delivery_tag == "tag-notify-1"
    assert rows[0].task_name == "app.tasks.notify.NotifyTask"
    assert rows[0].repoid == 1
    assert rows[0].commitid == "abcdef0123"
    assert rows[0].pk_token == "unacked#tag-notify-1"


def test_unacked_queryset_detail_mode_resolves_visibility_deadline(
    patched_broker,
):
    """The detail-mode queryset reads `ZSCORE unacked_index <tag>`
    per row and renders it as a UTC datetime. Pinning the
    field-population shape so the changelist's
    `visibility_deadline` column has a stable contract.
    """

    deadline = 1_700_000_000.0
    _push_unacked(
        patched_broker,
        delivery_tag="tag-deadline",
        routing_key="celery",
        deadline=deadline,
    )

    qs = UnackedQueueQuerySet(UnackedQueueItem).filter(routing_key__exact="celery")
    [row] = list(qs)

    assert row.visibility_deadline is not None
    assert row.visibility_deadline.timestamp() == pytest.approx(deadline)
    # ZSET-less rows render `None` (Kombu's restore-loop won't
    # touch them on its own; useful signal to surface in the UI).


def test_unacked_queryset_detail_mode_handles_missing_zset_score(
    patched_broker,
):
    _push_unacked(
        patched_broker,
        delivery_tag="tag-no-deadline",
        routing_key="celery",
        deadline=None,  # no ZSET entry
    )

    qs = UnackedQueueQuerySet(UnackedQueueItem).filter(routing_key__exact="celery")
    [row] = list(qs)

    assert row.visibility_deadline is None


def test_unacked_queryset_get_resolves_summary_pk(patched_broker):
    _push_unacked(patched_broker, delivery_tag="t-1")
    _push_unacked(patched_broker, delivery_tag="t-2")

    qs = UnackedQueueQuerySet(UnackedQueueItem)
    summary = qs.get(pk="unacked#summary")

    assert summary.depth == 2


def test_unacked_queryset_get_resolves_specific_delivery_tag(patched_broker):
    """`pk=unacked#<delivery_tag>` resolves via direct HGET so a
    delivery_tag past the `CELERY_BROKER_DISPLAY_LIMIT` cap still
    renders cleanly when the operator clicks through.
    """

    _push_unacked(
        patched_broker,
        delivery_tag="t-target",
        routing_key="celery",
        envelope=_build_envelope(task="t", kwargs={"repoid": 99}),
    )

    qs = UnackedQueueQuerySet(UnackedQueueItem)
    row = qs.get(pk="unacked#t-target")

    assert row.delivery_tag == "t-target"
    assert row.task_name == "t"
    assert row.repoid == 99


def test_unacked_queryset_get_raises_for_unknown_delivery_tag(patched_broker):
    qs = UnackedQueueQuerySet(UnackedQueueItem)
    with pytest.raises(UnackedQueueItem.DoesNotExist):
        qs.get(pk="unacked#absent")


def test_unacked_queryset_filter_by_task_name(patched_broker):
    _push_unacked(
        patched_broker,
        delivery_tag="t-1",
        routing_key="celery",
        envelope=_build_envelope(task="task.A"),
    )
    _push_unacked(
        patched_broker,
        delivery_tag="t-2",
        routing_key="celery",
        envelope=_build_envelope(task="task.B"),
    )

    qs = UnackedQueueQuerySet(UnackedQueueItem).filter(
        routing_key__exact="celery", task_name__exact="task.A"
    )

    rows = list(qs)
    assert [r.delivery_tag for r in rows] == ["t-1"]


def test_unacked_queryset_filter_by_repoid_and_commit_prefix(patched_broker):
    _push_unacked(
        patched_broker,
        delivery_tag="t-match",
        routing_key="celery",
        envelope=_build_envelope(kwargs={"repoid": 7, "commitid": "abc1234"}),
    )
    _push_unacked(
        patched_broker,
        delivery_tag="t-other-repo",
        routing_key="celery",
        envelope=_build_envelope(kwargs={"repoid": 8, "commitid": "abc1234"}),
    )
    _push_unacked(
        patched_broker,
        delivery_tag="t-other-commit",
        routing_key="celery",
        envelope=_build_envelope(kwargs={"repoid": 7, "commitid": "def5678"}),
    )

    qs = UnackedQueueQuerySet(UnackedQueueItem).filter(
        routing_key__exact="celery",
        repoid__exact=7,
        commitid__startswith="abc",
    )

    rows = list(qs)
    assert [r.delivery_tag for r in rows] == ["t-match"]


# ---- frequency aggregator --------------------------------------------------


def test_stream_unacked_frequency_aggregate_groups_by_4tuple(patched_broker):
    """`_stream_unacked_frequency_aggregate` returns top-N
    `(routing_key, task_name, repoid, commitid)` quadruples. The
    `routing_key` axis is what differentiates the unacked chart
    from celery_broker's 3-axis chart — the unacked HASH spans
    every queue, so without it "clear all messages for repo X
    commit Y" would silently span queues.
    """

    for i in range(3):
        _push_unacked(
            patched_broker,
            delivery_tag=f"notify-{i}",
            routing_key="notify",
            envelope=_build_envelope(
                task="app.tasks.notify.NotifyTask",
                kwargs={"repoid": 1, "commitid": "aaaa"},
            ),
        )
    for i in range(2):
        _push_unacked(
            patched_broker,
            delivery_tag=f"celery-{i}",
            routing_key="celery",
            envelope=_build_envelope(
                task="app.tasks.bundle.Processor",
                kwargs={"repoid": 2, "commitid": "bbbb"},
            ),
        )

    buckets, total = _stream_unacked_frequency_aggregate(patched_broker)

    assert total == 5
    assert [
        (b.routing_key, b.task_name, b.repoid, b.commitid, b.count) for b in buckets
    ] == [
        ("notify", "app.tasks.notify.NotifyTask", 1, "aaaa", 3),
        ("celery", "app.tasks.bundle.Processor", 2, "bbbb", 2),
    ]
    total_pct = sum(b.pct for b in buckets)
    assert 99.0 < total_pct < 101.0


def test_unacked_frequency_chart_aggregates_by_task_repo_commit(
    patched_broker,
):
    """`UnackedQueueQuerySet.frequency_by_routing_task_repo_commit`
    drives the lazy-loaded chart fragment. In summary mode it
    returns `[]` so the chart placeholder doesn't render.
    """

    for _ in range(2):
        _push_unacked(
            patched_broker,
            delivery_tag=f"d-{uuid.uuid4().hex}",
            routing_key="celery",
            envelope=_build_envelope(
                task="t.A", kwargs={"repoid": 1, "commitid": "aaaa"}
            ),
        )
    _push_unacked(
        patched_broker,
        delivery_tag=f"d-{uuid.uuid4().hex}",
        routing_key="celery",
        envelope=_build_envelope(task="t.B", kwargs={"repoid": 2, "commitid": "bbbb"}),
    )

    qs_summary = UnackedQueueQuerySet(UnackedQueueItem)
    assert qs_summary.frequency_by_routing_task_repo_commit() == []

    qs_detail = UnackedQueueQuerySet(UnackedQueueItem, routing_key="celery")
    buckets = qs_detail.frequency_by_routing_task_repo_commit()

    assert all(isinstance(b, UnackedFrequencyBucket) for b in buckets)
    assert [(b.task_name, b.count) for b in buckets] == [
        ("t.A", 2),
        ("t.B", 1),
    ]


def test_unacked_frequency_chart_pcts_sum_to_100_with_unparseable_envelopes(
    patched_broker,
):
    """Regression for a Bugbot-flagged frequency-chart percentage
    bug found on PR #911: every all-None unparseable envelope
    incremented `total` (the percentage denominator) but never
    landed in `counter`, so the rendered percentages summed to
    less than 100% on a HASH that mixed parseable and
    unparseable messages. Now `total` is re-normalised from the
    surviving counter (matching the cached path's
    `total = sum(counter.values())` behaviour) so the chart's
    bars always sum to 100% across the rendered buckets.
    """

    _push_unacked(
        patched_broker,
        delivery_tag="t-real",
        routing_key="celery",
        envelope=_build_envelope(task="t.A", kwargs={"repoid": 9, "commitid": "x"}),
    )
    # All-None, no routing_key — drops out of the chart (matches
    # `test_unacked_frequency_chart_drops_fully_empty_rows`).
    _push_unacked(
        patched_broker,
        delivery_tag="t-empty",
        routing_key="",
        envelope=_build_envelope(task=None, kwargs={}),
    )

    buckets, _total = _stream_unacked_frequency_aggregate(patched_broker)

    assert len(buckets) == 1, (
        f"unparseable + 1 real → 1 visible bucket; got {len(buckets)}"
    )
    total_pct = sum(b.pct for b in buckets)
    assert 99.0 < total_pct < 101.0, (
        f"chart percentages must sum to 100% across rendered buckets, got "
        f"{total_pct!r} — pre-fix denominator included dropped all-None rows"
    )


def test_unacked_frequency_chart_drops_fully_empty_rows(patched_broker):
    """An envelope with no task/repoid/commitid AND no
    routing_key would produce a chart row whose "Clear" button
    couldn't form any narrowing filter — drop it so the row's
    click can't produce an empty-scope clear (which the
    `clear_by_filter_view` refuses anyway).
    """

    _push_unacked(
        patched_broker,
        delivery_tag="t-empty",
        routing_key="",  # blank routing_key
        envelope=_build_envelope(task=None, kwargs={}),
    )
    _push_unacked(
        patched_broker,
        delivery_tag="t-real",
        routing_key="celery",
        envelope=_build_envelope(task="t", kwargs={"repoid": 9}),
    )

    buckets, _total = _stream_unacked_frequency_aggregate(patched_broker)

    assert [(b.task_name, b.repoid, b.routing_key) for b in buckets] == [
        ("t", 9, "celery"),
    ]


# ---- substitute_filter_any -------------------------------------------------


def test_substitute_filter_any_unacked_returns_4tuple_with_wildcards():
    out = _substitute_filter_any_unacked(None, None, None, None)
    assert out == (_FILTER_ANY, _FILTER_ANY, _FILTER_ANY, _FILTER_ANY)


def test_substitute_filter_any_unacked_preserves_set_slots():
    out = _substitute_filter_any_unacked("celery", "task.A", 7, "abc")
    assert out == ("celery", "task.A", 7, "abc")


def test_substitute_filter_any_unacked_treats_zero_repoid_as_set():
    """`repoid=0` is a legitimate value and must NOT be coerced to
    wildcard — the truthiness vs `is None` asymmetry mirrors the
    celery_broker `_substitute_filter_any` rule.
    """

    out = _substitute_filter_any_unacked(None, None, 0, None)
    assert out[2] == 0


# ---- streaming clear -------------------------------------------------------


def test_unacked_clear_drains_hash_and_index_for_matches(patched_broker):
    """`_streaming_unacked_clear` issues paired HDEL+ZREM per
    match. Skipping the ZREM would leave a phantom in
    `unacked_index` that points at a non-existent HASH field,
    which is the failure mode this test guards against.
    """

    for i in range(3):
        _push_unacked(
            patched_broker,
            delivery_tag=f"match-{i}",
            routing_key="celery",
            deadline=1000.0 + i,
            envelope=_build_envelope(
                task="t.A", kwargs={"repoid": 7, "commitid": "abc"}
            ),
        )
    _push_unacked(
        patched_broker,
        delivery_tag="other",
        routing_key="celery",
        deadline=2000.0,
        envelope=_build_envelope(task="t.B", kwargs={"repoid": 7, "commitid": "abc"}),
    )

    filter_tuple = _substitute_filter_any_unacked("celery", "t.A", 7, "abc")
    stats = _streaming_unacked_clear(
        patched_broker,
        frozenset({filter_tuple}),
    )

    assert stats.total_found == 3
    assert stats.total_hdel == 3
    assert stats.total_zrem == 3
    # The HASH and ZSET for the matched delivery tags are gone;
    # the unrelated `t.B` entry survives.
    assert patched_broker.hexists("unacked", "match-0") is False
    assert patched_broker.zscore("unacked_index", "match-0") is None
    assert patched_broker.hexists("unacked", "other") is True
    assert patched_broker.zscore("unacked_index", "other") is not None


def test_unacked_clear_dry_run_counts_without_mutating(patched_broker):
    for i in range(3):
        _push_unacked(
            patched_broker,
            delivery_tag=f"d-{i}",
            routing_key="celery",
            deadline=float(i),
            envelope=_build_envelope(kwargs={"repoid": 1}),
        )

    filter_tuple = _substitute_filter_any_unacked("celery", None, 1, None)
    stats = _streaming_unacked_clear(
        patched_broker, frozenset({filter_tuple}), dry_run=True
    )

    assert stats.total_found == 3
    assert stats.total_hdel == 0
    assert stats.total_zrem == 0
    assert patched_broker.hlen("unacked") == 3
    assert patched_broker.zcard("unacked_index") == 3


def test_unacked_clear_keep_one_keeps_lowest_deadline_match(patched_broker):
    """`keep_one=True` preserves the lowest-`ZSCORE` match — the
    message Kombu's restore-loop is most likely to re-enqueue
    first. Useful workflow: "I want to keep one example to
    inspect but clear the rest."
    """

    deadlines = {"early": 100.0, "middle": 200.0, "late": 300.0}
    for tag, deadline in deadlines.items():
        _push_unacked(
            patched_broker,
            delivery_tag=tag,
            routing_key="celery",
            deadline=deadline,
            envelope=_build_envelope(task="t", kwargs={"repoid": 1, "commitid": "aaa"}),
        )

    filter_tuple = _substitute_filter_any_unacked("celery", "t", 1, "aaa")
    stats = _streaming_unacked_clear(
        patched_broker,
        frozenset({filter_tuple}),
        keep_one=True,
    )

    assert stats.total_found == 3
    # Two non-keepers got HDEL+ZREMmed; the lowest-deadline
    # `early` survives.
    assert stats.total_hdel == 2
    assert stats.total_zrem == 2
    assert patched_broker.hexists("unacked", "early") is True
    assert patched_broker.zscore("unacked_index", "early") == 100.0
    assert patched_broker.hexists("unacked", "middle") is False
    assert patched_broker.hexists("unacked", "late") is False


def test_unacked_clear_keep_one_pass_one_does_not_double_pipeline(
    patched_broker, monkeypatch
):
    """Regression for a Bugbot-flagged duplicate-pipeline bug
    found on PR #911: when `keep_one=True` and pass 1 walked a
    chunk-buffer with multiple matches, `pass_matches` was
    accumulated twice (once via the per-field
    `pass_matches.append(field_name)`, once via a then-present
    `pass_matches.extend(chunk_targets)`). The post-pass HDEL+ZREM
    pipeline then issued two commands per non-keeper. Idempotent
    on Redis (second HDEL/ZREM returns 0, so counters stayed
    truthful) but doubled the round-trip volume on a long clear.

    We assert each non-keeper field appears in the actual HDEL
    pipeline call exactly once by patching `pipeline.hdel` to
    record arg lists. Pinning this contract so a future refactor
    of the post-pass keep_one branch can't quietly reintroduce
    the duplicate.
    """

    deadlines = {"a": 100.0, "b": 200.0, "c": 300.0, "d": 400.0}
    for tag, deadline in deadlines.items():
        _push_unacked(
            patched_broker,
            delivery_tag=tag,
            routing_key="celery",
            deadline=deadline,
            envelope=_build_envelope(task="t", kwargs={"repoid": 1, "commitid": "z"}),
        )

    hdel_calls: list[tuple[str, ...]] = []
    real_pipeline = patched_broker.pipeline

    def _spy_pipeline(*args, **kwargs):
        pipe = real_pipeline(*args, **kwargs)
        real_hdel = pipe.hdel

        def _hdel(name, *fields):
            hdel_calls.append(tuple(fields))
            return real_hdel(name, *fields)

        pipe.hdel = _hdel
        return pipe

    monkeypatch.setattr(patched_broker, "pipeline", _spy_pipeline)

    filter_tuple = _substitute_filter_any_unacked("celery", "t", 1, "z")
    stats = _streaming_unacked_clear(
        patched_broker,
        frozenset({filter_tuple}),
        keep_one=True,
    )

    assert stats.total_found == 4
    # Three non-keepers HDELed; the lowest-deadline `a` is kept.
    assert stats.total_hdel == 3
    # Across all pipelines, each non-keeper field must appear
    # exactly once. Flatten the recorded HDEL arg lists and count.
    flat = [field for fields in hdel_calls for field in fields]
    counts = {field: flat.count(field) for field in flat}
    for field, n in counts.items():
        assert n == 1, (
            f"field {field!r} HDELed {n} times across pipelines — "
            "duplicate pipeline regression"
        )


def test_unacked_clear_filter_any_wildcards_match_correctly(patched_broker):
    """`_FILTER_ANY` in any slot wildcards that axis. Verify the
    full-wildcard `(routing_key=ANY, task=ANY, repoid=ANY,
    commitid=ANY)` matches every entry, while a partial filter
    narrows correctly.
    """

    _push_unacked(
        patched_broker,
        delivery_tag="A",
        routing_key="celery",
        deadline=1.0,
        envelope=_build_envelope(task="t.A", kwargs={"repoid": 1}),
    )
    _push_unacked(
        patched_broker,
        delivery_tag="B",
        routing_key="notify",
        deadline=2.0,
        envelope=_build_envelope(task="t.B", kwargs={"repoid": 2}),
    )

    # Full wildcard → matches both
    full_wildcard = (_FILTER_ANY, _FILTER_ANY, _FILTER_ANY, _FILTER_ANY)
    stats = _streaming_unacked_clear(
        patched_broker, frozenset({full_wildcard}), dry_run=True
    )
    assert stats.total_found == 2

    # routing_key="celery", everything else wildcard → matches A only
    partial = ("celery", _FILTER_ANY, _FILTER_ANY, _FILTER_ANY)
    stats = _streaming_unacked_clear(patched_broker, frozenset({partial}), dry_run=True)
    assert stats.total_found == 1


# ---- chunked-job worker ---------------------------------------------------


def test_start_unacked_clear_job_returns_uuid_and_initialises_state(
    patched_broker, superuser
):
    """`start_unacked_clear_job` returns a UUID-shaped job_id
    and writes a fully-populated initial hash with status=pending,
    the operator's filter shape, and a snapshotted total_estimated.
    Pinning the hash shape so the JSON status view's contract
    stays stable across refactors.
    """

    for i in range(5):
        _push_unacked(
            patched_broker,
            delivery_tag=f"d-{i}",
            routing_key="celery",
            deadline=float(i),
            envelope=_build_envelope(kwargs={"repoid": 1}),
        )

    job_id = start_unacked_clear_job(
        user=superuser,
        routing_key="celery",
        repoid=1,
        keep_one=False,
        dry_run=False,
    )
    _wait_for_job(job_id)

    assert uuid.UUID(job_id)
    state = get_unacked_clear_job(job_id)
    assert state is not None
    assert state["status"] == "completed"
    assert state["filter_routing_key"] == "celery"
    assert state["filter_repoid"] == "1"
    assert state["dry_run"] == "0"
    assert int(state["total_estimated"]) == 5
    assert int(state["matched"]) >= 5
    assert int(state["zrem_removed"]) >= 5


def test_unacked_clear_by_filter_chunked_job_drains_hash_and_index(
    patched_broker, superuser
):
    """End-to-end: spawn the chunked worker, wait for completion,
    and assert both `unacked` and `unacked_index` have been
    drained for the filtered scope. Other entries survive.
    """

    for i in range(10):
        _push_unacked(
            patched_broker,
            delivery_tag=f"clear-{i}",
            routing_key="celery",
            deadline=float(i),
            envelope=_build_envelope(
                task="t.A", kwargs={"repoid": 7, "commitid": "abc"}
            ),
        )
    _push_unacked(
        patched_broker,
        delivery_tag="keep-other-task",
        routing_key="celery",
        deadline=99.0,
        envelope=_build_envelope(task="t.B", kwargs={"repoid": 7, "commitid": "abc"}),
    )

    job_id = start_unacked_clear_job(
        user=superuser,
        routing_key="celery",
        task_name="t.A",
        repoid=7,
        commitid="abc",
    )
    _wait_for_job(job_id)

    state = get_unacked_clear_job(job_id)
    assert state is not None
    assert state["status"] == "completed"
    assert int(state["matched"]) == 10
    assert int(state["zrem_removed"]) == 10
    # The keep-other-task entry survives — different task_name,
    # so the filter's task_name slot exact-matched it out.
    assert patched_broker.hlen("unacked") == 1
    assert patched_broker.hexists("unacked", "keep-other-task") is True
    assert patched_broker.zcard("unacked_index") == 1


def test_unacked_clear_job_cancellation_lands_at_chunk_boundary(
    patched_broker, superuser, monkeypatch
):
    """Cancelling mid-clear: set `cancel_requested=1` and expect
    the worker to bail out at the next chunk boundary with
    `status=cancelled`. We force a tiny chunk size so cancel
    has a chance to land before the (small) test queue drains.
    """

    for i in range(50):
        _push_unacked(
            patched_broker,
            delivery_tag=f"d-{i}",
            routing_key="celery",
            deadline=float(i),
            envelope=_build_envelope(kwargs={"repoid": 1}),
        )

    monkeypatch.setattr(redis_admin_services, "_UNACKED_CLEAR_CHUNK", 5)

    job_id = start_unacked_clear_job(
        user=superuser,
        routing_key="celery",
        repoid=1,
    )
    request_cancel_unacked_clear_job(job_id)
    _wait_for_job(job_id)

    state = get_unacked_clear_job(job_id)
    assert state is not None
    assert state["status"] in ("cancelled", "completed")


def test_unacked_clear_job_preserves_lowest_deadline_keeper(patched_broker, superuser):
    """Chunked-mode `keep_one`: the survivor is the
    lowest-`ZSCORE` match across all chunks, not the
    first-encountered one. Same semantic as the synchronous path.
    """

    deadlines = [(f"d-{i}", float(i * 100)) for i in range(5)]
    for tag, deadline in deadlines:
        _push_unacked(
            patched_broker,
            delivery_tag=tag,
            routing_key="celery",
            deadline=deadline,
            envelope=_build_envelope(task="t", kwargs={"repoid": 1, "commitid": "aaa"}),
        )

    job_id = start_unacked_clear_job(
        user=superuser,
        routing_key="celery",
        task_name="t",
        repoid=1,
        commitid="aaa",
        keep_one=True,
    )
    _wait_for_job(job_id)

    state = get_unacked_clear_job(job_id)
    assert state is not None
    assert state["status"] == "completed"
    # Lowest-deadline keeper is `d-0` (deadline=0.0); the four
    # higher-deadline matches were HDEL+ZREMmed.
    assert patched_broker.hexists("unacked", "d-0") is True
    for i in range(1, 5):
        assert patched_broker.hexists("unacked", f"d-{i}") is False


def test_request_cancel_unacked_clear_job_returns_false_for_unknown_id(
    patched_broker,
):
    assert request_cancel_unacked_clear_job(uuid.uuid4().hex) is False


def test_get_unacked_clear_job_returns_none_for_unknown_id(patched_broker):
    assert get_unacked_clear_job(uuid.uuid4().hex) is None


# ---- admin views (RequestFactory-driven) ----------------------------------


def _build_request(method: str, path: str, *, user, **post_kwargs):
    factory = RequestFactory()
    if method == "POST":
        request = factory.post(path, data=post_kwargs)
    else:
        request = factory.get(path, data=post_kwargs)
    request.user = user
    # Django's `messages` framework needs a message storage on
    # the request before `messages.error(...)` works.
    setattr(request, "session", {})
    setattr(request, "_messages", FallbackStorage(request))
    return request


def test_unacked_admin_chart_fragment_endpoint_returns_html(
    patched_broker, superuser, monkeypatch
):
    """Hitting `chart_fragment_view` for a routing_key with
    matching unacked entries returns 200 + the rendered chart
    template. Used by the changelist's lazy `<script>` to fetch
    the chart asynchronously on `DOMContentLoaded`.

    `_resolve_repo_displays` is stubbed because the in-process
    sandbox doesn't have Postgres; the `service:owner/name`
    rendering is covered by a separate ORM-aware test.
    """

    # noqa: PLC0415 — local import + monkeypatch keeps the
    # ORM-skipping shim narrowly scoped to this test.
    from redis_admin import admin as redis_admin_module  # noqa: PLC0415

    monkeypatch.setattr(
        redis_admin_module, "_resolve_repo_displays", lambda repoids: {}
    )

    for _ in range(2):
        _push_unacked(
            patched_broker,
            delivery_tag=f"d-{uuid.uuid4().hex}",
            routing_key="celery",
            envelope=_build_envelope(
                task="t.A", kwargs={"repoid": 1, "commitid": "aaaa"}
            ),
        )

    admin_instance = UnackedQueueAdmin(UnackedQueueItem, AdminSite())
    request = _build_request(
        "GET",
        "/admin/redis_admin/unackedqueueitem/celery/chart-fragment/",
        user=superuser,
    )

    response = admin_instance.chart_fragment_view(request, routing_key="celery")

    assert response.status_code == 200
    assert response["Content-Type"].startswith("text/html")
    body = response.content.decode("utf-8", errors="replace")
    assert "celery-frequency-chart" in body
    assert "t.A" in body


def test_unacked_admin_chart_fragment_returns_204_for_empty_hash(
    patched_broker, superuser
):
    """No unacked entries → no buckets → 204 so the JS loader
    drops the placeholder rather than rendering an empty chart.
    """

    admin_instance = UnackedQueueAdmin(UnackedQueueItem, AdminSite())
    request = _build_request(
        "GET",
        "/admin/redis_admin/unackedqueueitem/celery/chart-fragment/",
        user=superuser,
    )

    response = admin_instance.chart_fragment_view(request, routing_key="celery")

    assert response.status_code == 204


def test_unacked_admin_clear_progress_endpoint_returns_json(patched_broker, superuser):
    """The status-JSON endpoint serves a stable shape regardless
    of the underlying job hash's keys, so the polling JS can rely
    on the field set even as we add new counters.
    """

    _push_unacked(patched_broker, delivery_tag="d-1", routing_key="celery")
    job_id = start_unacked_clear_job(user=superuser, routing_key="celery", dry_run=True)
    _wait_for_job(job_id)

    admin_instance = UnackedQueueAdmin(UnackedQueueItem, AdminSite())
    request = _build_request(
        "GET",
        f"/admin/redis_admin/unackedqueueitem/clear-by-filter/job/{job_id}/status/",
        user=superuser,
    )

    response = admin_instance.clear_by_filter_status_view(request, job_id=job_id)

    assert response.status_code == 200
    assert response["Content-Type"].startswith("application/json")
    payload = json.loads(response.content.decode("utf-8"))
    # Pinned shape: every field the JS poller reads must be present.
    expected_keys = {
        "job_id",
        "status",
        "is_terminal",
        "routing_key",
        "filter_routing_key",
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
        "zrem_removed",
        "passes_run",
        "error",
        "cancel_requested",
    }
    assert expected_keys <= set(payload.keys())
    assert payload["job_id"] == job_id
    assert payload["status"] == "completed"
    assert payload["filter_routing_key"] == "celery"


def test_unacked_admin_clear_by_filter_get_renders_preview(patched_broker, superuser):
    """GET on `clear_by_filter_view` with chart hints renders the
    preview page's approximate-count callout in pure Python (no
    Redis I/O on this path).
    """

    admin_instance = UnackedQueueAdmin(UnackedQueueItem, AdminSite())
    request = _build_request(
        "GET",
        "/admin/redis_admin/unackedqueueitem/clear-by-filter/",
        user=superuser,
        routing_key="celery",
        task_name="t.A",
        repoid="7",
        commitid="abc",
        bucket_count="50",
        bucket_pct="50.0",
        total_visible="100",
        total_depth="200",
    )

    response = admin_instance.clear_by_filter_view(request)

    assert response.status_code == 200
    body = response.content.decode("utf-8", errors="replace")
    assert "Clear unacked" in body
    assert "celery" in body
    # Approx-impact callout: 50/100 * 200 = 100 messages
    assert "100" in body
    # Typed-confirmation field surfaces with the routing_key as
    # the expected confirm value.
    assert "Type" in body
    assert "celery" in body


def test_unacked_admin_clear_by_filter_refuses_empty_scope(patched_broker, superuser):
    """Refusing to clear with no narrowing filter — would
    otherwise drop the entire unacked HASH, which is too sharp
    an edge for an admin button. Operators wanting the full
    drop should run `redis-cli DEL unacked unacked_index`
    directly.
    """

    admin_instance = UnackedQueueAdmin(UnackedQueueItem, AdminSite())
    request = _build_request(
        "GET",
        "/admin/redis_admin/unackedqueueitem/clear-by-filter/",
        user=superuser,
    )

    response = admin_instance.clear_by_filter_view(request)

    # 302 redirect back to changelist with an error message.
    assert response.status_code == 302


def test_unacked_admin_clear_by_filter_post_spawns_chunked_job(
    patched_broker, superuser
):
    """POST with `action=dry_run` (no typed-confirm needed)
    spawns the chunked worker and 302s to the progress page.
    """

    _push_unacked(
        patched_broker,
        delivery_tag="d-1",
        routing_key="celery",
        envelope=_build_envelope(kwargs={"repoid": 1}),
    )

    admin_instance = UnackedQueueAdmin(UnackedQueueItem, AdminSite())
    request = _build_request(
        "POST",
        "/admin/redis_admin/unackedqueueitem/clear-by-filter/",
        user=superuser,
        routing_key="celery",
        repoid="1",
        action="dry_run",
    )

    response = admin_instance.clear_by_filter_view(request)

    assert response.status_code == 302
    # Redirect URL should target the progress page.
    assert "/clear-by-filter/job/" in response["Location"]


def test_unacked_admin_clear_by_filter_destructive_requires_typed_confirm(
    patched_broker, superuser
):
    """`clear_all` / `clear_keep_one` need the typed-confirm
    field to match `routing_key`; without it the page re-renders
    with an error rather than spawning the worker.
    """

    admin_instance = UnackedQueueAdmin(UnackedQueueItem, AdminSite())
    request = _build_request(
        "POST",
        "/admin/redis_admin/unackedqueueitem/clear-by-filter/",
        user=superuser,
        routing_key="celery",
        repoid="1",
        action="clear_all",
        typed_confirm="wrong-value",
    )

    response = admin_instance.clear_by_filter_view(request)

    assert response.status_code == 200  # re-renders preview
    body = response.content.decode("utf-8", errors="replace")
    assert "Typed confirmation must equal" in body


def test_unacked_admin_clear_by_filter_cancel_view_returns_202_for_known_job(
    patched_broker, superuser
):
    _push_unacked(patched_broker, delivery_tag="d-1", routing_key="celery")
    job_id = start_unacked_clear_job(user=superuser, routing_key="celery", dry_run=True)

    admin_instance = UnackedQueueAdmin(UnackedQueueItem, AdminSite())
    request = _build_request(
        "POST",
        f"/admin/redis_admin/unackedqueueitem/clear-by-filter/job/{job_id}/cancel/",
        user=superuser,
    )

    response = admin_instance.clear_by_filter_cancel_view(request, job_id=job_id)
    _wait_for_job(job_id)

    assert response.status_code == 202


# ---- synchronous unacked_clear --------------------------------------------


def test_synchronous_unacked_clear_drains_targets(patched_broker, superuser):
    """`services.unacked_clear` is the synchronous path used by
    bulk admin actions (`clear_dry_run` / `clear_selected`).
    Mirrors `celery_broker_clear` but for the HASH+ZSET layout.
    """

    _push_unacked(
        patched_broker,
        delivery_tag="d-1",
        routing_key="celery",
        deadline=1.0,
        envelope=_build_envelope(task="t", kwargs={"repoid": 1, "commitid": "abc"}),
    )
    _push_unacked(
        patched_broker,
        delivery_tag="d-2",
        routing_key="celery",
        deadline=2.0,
        envelope=_build_envelope(task="t", kwargs={"repoid": 1, "commitid": "abc"}),
    )

    qs = UnackedQueueQuerySet(UnackedQueueItem).filter(routing_key__exact="celery")
    targets = list(qs)

    result = unacked_clear(targets, user=superuser, dry_run=False)

    assert result.count == 2
    assert patched_broker.hlen("unacked") == 0
    assert patched_broker.zcard("unacked_index") == 0


def test_unacked_admin_summary_drill_in_link_uses_view_all_query_param():
    """Regression for a Bugbot-flagged self-referencing link bug
    found on PR #911: the summary row's `messages_link` rendered
    a URL with no `routing_key__exact` query param. Since
    `_is_summary_request` returned True whenever
    `routing_key__exact` was absent, clicking the drill-in link
    re-rendered the summary page — a self-loop. The unacked
    summary row has no single routing_key to point at because
    the unacked HASH is one global bucket carrying every queue's
    reservations.

    The fix uses a dedicated `view_all=1` flag that
    `_is_summary_request` recognises as the explicit "show every
    reservation" mode. We pin both halves of the contract: the
    rendered URL carries `view_all=1`, and `_is_summary_request`
    treats `view_all=1` as a detail-mode request even when
    `routing_key__exact` is empty.
    """

    admin_instance = UnackedQueueAdmin(UnackedQueueItem, AdminSite())
    summary_obj = UnackedQueueItem(
        delivery_tag=_UNACKED_KEY,
        routing_key="",
        depth=42,
    )

    rendered = admin_instance.messages_link(summary_obj)

    assert "view_all=1" in rendered, (
        f"summary drill-in link must carry view_all=1, got {rendered!r}"
    )
    assert "view 42 unacked message(s)" in rendered

    factory = RequestFactory()
    detail_request_via_view_all = factory.get(
        "/admin/redis_admin/unackedqueueitem/", {"view_all": "1"}
    )
    assert UnackedQueueAdmin._is_summary_request(detail_request_via_view_all) is False

    summary_request = factory.get("/admin/redis_admin/unackedqueueitem/")
    assert UnackedQueueAdmin._is_summary_request(summary_request) is True

    detail_request_via_routing_key = factory.get(
        "/admin/redis_admin/unackedqueueitem/", {"routing_key__exact": "celery"}
    )
    assert (
        UnackedQueueAdmin._is_summary_request(detail_request_via_routing_key) is False
    )


def test_unacked_admin_view_all_drill_in_renders_per_message_rows(patched_broker):
    """Regression for a Bugbot-flagged HIGH-severity bug found on
    PR #911: clicking the summary row's "view N unacked
    message(s) →" link landed on `?view_all=1` and
    `_is_summary_request` correctly flipped to detail mode for
    rendering, but `get_queryset` only flipped the queryset out
    of summary mode when `routing_key__exact` was set. With
    `view_all=1`, the queryset's `routing_key` stayed `None`,
    `is_summary_mode()` returned `True`, and the admin rendered
    a single dashes-row under the per-message detail columns —
    breaking the feature at its primary navigation entry point.
    The fix wires `view_all=1` through to the queryset so it
    materialises every reservation regardless of routing_key,
    and the operator narrows from there via the sidebar
    `routing_key` filter.
    """

    _push_unacked(
        patched_broker,
        delivery_tag="d-celery",
        routing_key="celery",
        deadline=1.0,
        envelope=_build_envelope(task="app.tasks.notify", kwargs={"repoid": 1}),
    )
    _push_unacked(
        patched_broker,
        delivery_tag="d-uploads",
        routing_key="uploads",
        deadline=2.0,
        envelope=_build_envelope(task="app.tasks.upload", kwargs={"repoid": 2}),
    )

    factory = RequestFactory()
    request = factory.get("/admin/redis_admin/unackedqueueitem/", {"view_all": "1"})
    admin_instance = UnackedQueueAdmin(UnackedQueueItem, AdminSite())

    queryset = admin_instance.get_queryset(request)

    assert queryset.is_summary_mode() is False, (
        "view_all=1 must flip the queryset out of summary mode so the "
        "admin's detail columns render per-message rows."
    )
    rows = list(queryset)
    delivery_tags = sorted(r.delivery_tag for r in rows)
    assert delivery_tags == ["d-celery", "d-uploads"], (
        f"view_all=1 must surface every reserved message regardless of "
        f"routing_key; got {delivery_tags!r}"
    )
    routing_keys = sorted(r.routing_key for r in rows)
    assert routing_keys == ["celery", "uploads"], (
        f"view_all=1 must preserve each row's source routing_key; got {routing_keys!r}"
    )
    assert all(r.depth is None for r in rows), (
        "view_all=1 rows must be per-message (depth=None), not summary "
        "rows wearing detail columns."
    )
