"""Unit tests for the M3 key parsers and SCAN-pattern pushdown.

`ParsedKey` and the `pattern_for` helpers are pure functions so these
tests don't need a Redis (fake or real) at all.
"""

from __future__ import annotations

import base64
import json

import fakeredis
import pytest

from redis_admin import conn as redis_admin_conn
from redis_admin.families import (
    FAMILIES,
    _celery_decode_value,
    _celery_pattern,
    _intermediate_report_pattern,
    _lock_attempts_pattern,
    _lock_pattern_factory,
    _parse_coordination_lock_key,
    _parse_intermediate_report_key,
    _parse_latest_upload_key,
    _parse_lock_attempts_key,
    _parse_ta_flake_key,
    _parse_ta_flake_lock_key,
    _parse_ta_notifier_fence_key,
    _parse_upload_finisher_gate_key,
    _parse_upload_processing_state_key,
    _parse_uploads_key,
    _ta_flake_pattern,
    _uploads_pattern,
    find_family,
    iter_keys,
)


@pytest.fixture
def patched_redis(monkeypatch) -> fakeredis.FakeStrictRedis:
    server = fakeredis.FakeStrictRedis()
    monkeypatch.setattr(redis_admin_conn, "get_connection", lambda: server)
    return server


def test_parse_uploads_coverage_default():
    parsed = _parse_uploads_key("uploads/1234/abcdef0")
    assert parsed.family == "uploads"
    assert parsed.repoid == 1234
    assert parsed.commitid == "abcdef0"
    assert parsed.report_type == "coverage"


def test_parse_uploads_with_typed_report_type():
    parsed = _parse_uploads_key("uploads/42/sha/test_results")
    assert parsed.repoid == 42
    assert parsed.commitid == "sha"
    assert parsed.report_type == "test_results"


def test_parse_uploads_handles_extra_slashes_in_report_type():
    # `split('/', 3)` keeps everything past the third slash in report_type
    # so unexpected nesting is preserved as-is for inspection.
    parsed = _parse_uploads_key("uploads/42/sha/bundle_analysis/extra")
    assert parsed.report_type == "bundle_analysis/extra"


def test_parse_uploads_non_int_repoid_is_none():
    parsed = _parse_uploads_key("uploads/notanint/sha")
    assert parsed.repoid is None
    assert parsed.commitid == "sha"


def test_parse_ta_flake_key_extracts_repoid():
    parsed = _parse_ta_flake_key("ta_flake_key:99")
    assert parsed.family == "ta_flake_key"
    assert parsed.repoid == 99
    assert parsed.commitid is None


def test_parse_ta_flake_key_non_int_repoid_is_none():
    parsed = _parse_ta_flake_key("ta_flake_key:bogus")
    assert parsed.repoid is None


def test_uploads_pattern_pushdown_repoid_only():
    assert _uploads_pattern({"repoid": 1234}) == "uploads/1234/*"


def test_uploads_pattern_pushdown_with_commitid_prefix():
    assert (
        _uploads_pattern({"repoid": 1234, "commitid_prefix": "abc"})
        == "uploads/1234/abc*"
    )


def test_uploads_pattern_no_filters_falls_back_to_wildcard():
    assert _uploads_pattern({}) == "uploads/*"


def test_ta_flake_pattern_pushdown_is_exact_when_repoid_known():
    assert _ta_flake_pattern({"repoid": 7}) == "ta_flake_key:7"
    assert _ta_flake_pattern({}) == "ta_flake_key:*"


def test_find_family_classifies_known_keys():
    assert find_family("uploads/1/abc").name == "uploads"
    assert find_family("ta_flake_key:9").name == "ta_flake_key"


def test_find_family_returns_none_for_unknown_keys():
    assert find_family("celery/some-queue") is None


def test_iter_keys_pushdown_skips_other_repos(patched_redis):
    """A `repoid` filter should narrow the SCAN pattern so unrelated
    repos are never scanned at all."""

    patched_redis.rpush("uploads/1/abc", "x")
    patched_redis.rpush("uploads/2/def", "x")

    keys = [k for k, _ in iter_keys(patched_redis, repoid=1)]
    assert keys == ["uploads/1/abc"]


def test_iter_keys_pushdown_skips_other_families(patched_redis):
    patched_redis.rpush("uploads/1/abc", "x")
    patched_redis.rpush("ta_flake_key:1", "x")

    keys = [k for k, fam in iter_keys(patched_redis, family="uploads")]
    assert keys == ["uploads/1/abc"]


def test_iter_keys_pushdown_combines_repoid_and_commit_prefix(patched_redis):
    patched_redis.rpush("uploads/1/abcd", "x")
    patched_redis.rpush("uploads/1/abcd/test_results", "x")
    patched_redis.rpush("uploads/1/zzzz", "x")
    patched_redis.rpush("uploads/2/abcd", "x")

    keys = sorted(
        k for k, _ in iter_keys(patched_redis, repoid=1, commitid_prefix="abcd")
    )
    assert keys == ["uploads/1/abcd", "uploads/1/abcd/test_results"]


def test_families_registry_exposes_uploads_and_ta_flake_key():
    names = {f.name for f in FAMILIES}
    assert "uploads" in names
    assert "ta_flake_key" in names


# ---- M4.1: latest_upload / upload-processing-state / intermediate-report ----


def test_parse_latest_upload_default_report_type():
    parsed = _parse_latest_upload_key("latest_upload/1234/abcdef0")
    assert parsed.family == "latest_upload"
    assert parsed.repoid == 1234
    assert parsed.commitid == "abcdef0"
    assert parsed.report_type == "coverage"


def test_parse_latest_upload_typed_report_type():
    parsed = _parse_latest_upload_key("latest_upload/42/sha/test_results")
    assert parsed.report_type == "test_results"


def test_parse_upload_processing_state_extracts_state_segment():
    parsed = _parse_upload_processing_state_key(
        "upload-processing-state/123/abc/in_progress"
    )
    assert parsed.family == "upload-processing-state"
    assert parsed.repoid == 123
    assert parsed.commitid == "abc"
    assert parsed.state == "in_progress"


def test_parse_intermediate_report_extracts_upload_id():
    parsed = _parse_intermediate_report_key("intermediate-report/upload-99")
    assert parsed.family == "intermediate-report"
    assert parsed.upload_id == "upload-99"
    assert parsed.repoid is None  # not derivable from the key alone


def test_intermediate_report_pattern_drops_when_repoid_filter_set():
    """Filtering by repoid against intermediate-report can never match,
    so the pushdown returns None and `iter_keys` skips the family.
    """

    assert _intermediate_report_pattern({}) == "intermediate-report/*"
    assert _intermediate_report_pattern({"repoid": 5}) is None
    assert _intermediate_report_pattern({"commitid_prefix": "abc"}) is None


def test_iter_keys_classifies_new_m4_families(patched_redis):
    patched_redis.set("latest_upload/1/abc", "1")
    patched_redis.sadd("upload-processing-state/1/abc/started", "x")
    patched_redis.hset("intermediate-report/u1", mapping={"f": "v"})

    classified = {k: f.name for k, f in iter_keys(patched_redis)}
    assert classified["latest_upload/1/abc"] == "latest_upload"
    assert (
        classified["upload-processing-state/1/abc/started"] == "upload-processing-state"
    )
    assert classified["intermediate-report/u1"] == "intermediate-report"


# ---- M4.2: celery_broker family + kombu envelope decoder -------------------


def test_celery_pattern_skips_when_repoid_filter_set():
    """Celery queue *keys* don't carry repoid; filtering by repoid means
    every celery queue would post-scan-drop, so skip the SCAN entirely.
    """

    assert _celery_pattern({}) == "enterprise_*"
    assert _celery_pattern({"repoid": 5}) is None
    assert _celery_pattern({"commitid_prefix": "abc"}) is None


def test_celery_broker_family_resolves_known_queue_names():
    """`fixed_keys` for `celery_broker` includes the BaseCeleryConfig
    defaults so `iter_keys` finds the well-known queues without needing
    an `enterprise_*` SCAN to discover them.
    """
    family = next(f for f in FAMILIES if f.name == "celery_broker")
    assert "celery" in family.fixed_keys
    assert "healthcheck" in family.fixed_keys


def test_iter_keys_yields_celery_queues_via_fixed_keys(patched_redis):
    patched_redis.rpush("celery", '{"task": "x"}')
    patched_redis.rpush("healthcheck", '{"task": "y"}')
    patched_redis.rpush("enterprise_uploadcoverage", '{"task": "z"}')
    patched_redis.rpush("uploads/1/abc", "x")  # different family

    classified = {k: f.name for k, f in iter_keys(patched_redis)}
    assert classified["celery"] == "celery_broker"
    assert classified["healthcheck"] == "celery_broker"
    assert classified["enterprise_uploadcoverage"] == "celery_broker"
    assert classified["uploads/1/abc"] == "uploads"


def test_celery_decode_value_extracts_task_repoid_commit():
    """The kombu envelope decoder pulls task / repoid / commitid out of
    a real-shaped JSON+base64 envelope and prefixes the raw payload so
    operators can scan a queue without re-decoding by hand.
    """

    body = json.dumps([[], {"repoid": 7, "commitid": "abcdef0123"}, {}])
    body_b64 = base64.b64encode(body.encode("utf-8")).decode("ascii")
    envelope = json.dumps(
        {
            "body": body_b64,
            "headers": {"task": "app.tasks.upload.Upload", "id": "task-uuid-1"},
        }
    )

    decoded = _celery_decode_value(envelope)
    assert decoded.startswith("[task=app.tasks.upload.Upload repoid=7 commit=abcdef0]")
    assert envelope in decoded  # raw payload is preserved after the prefix


def test_celery_decode_value_falls_back_for_non_json_payload():
    """Anything that doesn't look like a kombu envelope is rendered
    unchanged so we don't 500 the admin on an unexpected message.
    """

    assert _celery_decode_value("not-json") == "not-json"


# ---- M4.3: lock families ---------------------------------------------------


def test_parse_coordination_lock_extracts_repoid_commit_report_type():
    parsed = _parse_coordination_lock_key("upload_lock_123_abcdef0")
    assert parsed.family == "coordination_lock"
    assert parsed.repoid == 123
    assert parsed.commitid == "abcdef0"
    assert parsed.report_type is None


def test_parse_coordination_lock_with_report_type_suffix():
    """`bundle_analysis_processing_lock_123_abc_bundle_analysis` shape
    keeps the report_type suffix. Multi-word report types stay joined."""

    parsed = _parse_coordination_lock_key(
        "bundle_analysis_processing_lock_123_abc_bundle_analysis"
    )
    assert parsed.repoid == 123
    assert parsed.commitid == "abc"
    assert parsed.report_type == "bundle_analysis"


def test_parse_lock_attempts_unwraps_embedded_lock_name():
    parsed = _parse_lock_attempts_key("lock_attempts:upload_lock_42_abcdef")
    assert parsed.family == "lock_attempts"
    assert parsed.repoid == 42
    assert parsed.commitid == "abcdef"


def test_parse_ta_flake_lock_extracts_repoid():
    parsed = _parse_ta_flake_lock_key("ta_flake_lock:99")
    assert parsed.repoid == 99


def test_parse_upload_finisher_gate_extracts_repoid_commit():
    parsed = _parse_upload_finisher_gate_key("upload_finisher_gate_123_abcdef0")
    assert parsed.repoid == 123
    assert parsed.commitid == "abcdef0"


def test_parse_ta_notifier_fence_extracts_repoid_commit():
    parsed = _parse_ta_notifier_fence_key("ta_notifier_fence:123_abcdef0")
    assert parsed.repoid == 123
    assert parsed.commitid == "abcdef0"


def test_find_family_routes_lock_attempts_before_coordination_lock():
    """`lock_attempts:upload_lock_…` keys contain `_lock_` so the broad
    `*_lock_*` family would also match them; `find_family` must respect
    registration order and return the more-specific `lock_attempts:*`
    family.
    """
    fam = find_family("lock_attempts:upload_lock_1_abc")
    assert fam is not None and fam.name == "lock_attempts"


def test_lock_families_are_marked_undeletable_and_lock_category():
    """Defence in depth: every lock-shaped family must have
    `is_deletable=False` and `category="lock"` so M5's delete service
    refuses to touch them and they don't leak into the queue admin.
    """
    lock_names = {
        "lock_attempts",
        "ta_flake_lock",
        "ta_notifier_fence",
        "upload_finisher_gate",
        "coordination_lock",
    }
    for fam in FAMILIES:
        if fam.name in lock_names:
            assert fam.is_deletable is False, fam.name
            assert fam.category == "lock", fam.name
        else:
            assert fam.category == "queue", fam.name


def test_iter_keys_with_category_lock_only_returns_locks(patched_redis):
    patched_redis.rpush("uploads/1/abc", "x")
    patched_redis.set("upload_lock_1_abc", "owned")
    patched_redis.set("ta_flake_lock:1", "owned")

    queue_keys = {k for k, _ in iter_keys(patched_redis, category="queue")}
    lock_keys = {k for k, _ in iter_keys(patched_redis, category="lock")}

    assert queue_keys == {"uploads/1/abc"}
    assert lock_keys == {"upload_lock_1_abc", "ta_flake_lock:1"}


def test_iter_keys_does_not_double_yield_lock_attempts(patched_redis):
    """`lock_attempts:upload_lock_…` matches both `lock_attempts:*` and
    the broader `*_lock_*`; `iter_keys` must dedupe so the row appears
    once.
    """

    patched_redis.set("lock_attempts:upload_lock_1_abc", "1")

    rows = [
        (k, f.name)
        for k, f in iter_keys(patched_redis, category="lock")
        if k == "lock_attempts:upload_lock_1_abc"
    ]
    assert rows == [("lock_attempts:upload_lock_1_abc", "lock_attempts")]


# ---- Lock SCAN pattern pushdown regressions (Bugbot review on PR #888) ----


def test_ta_notifier_fence_pattern_pushes_down_commitid():
    """Bugbot PR #888: the previous `_lock_pattern_factory(sep=":")` set
    `has_commit = (sep == "_") = False`, so any `commitid_prefix`
    filter caused `pattern_for` to return None and skip the family
    entirely — even though `ta_notifier_fence:{repoid}_{commitid}`
    actually does encode a commit. Now `has_commit=True` is explicit.
    """

    pattern_for = _lock_pattern_factory(
        "ta_notifier_fence", prefix_sep=":", has_commit=True
    )

    assert pattern_for({}) == "ta_notifier_fence:*"
    assert pattern_for({"repoid": 42}) == "ta_notifier_fence:42_*"
    assert (
        pattern_for({"repoid": 42, "commitid_prefix": "abc"})
        == "ta_notifier_fence:42_abc*"
    )
    # commitid-only without repoid still falls back to the family
    # wildcard (Redis MATCH globs can't anchor "skip the repoid").
    assert pattern_for({"commitid_prefix": "abc"}) == "ta_notifier_fence:*"


def test_ta_flake_lock_pattern_drops_when_filtered_by_commitid():
    """`ta_flake_lock:{repoid}` truly has no commit segment, so
    `has_commit=False` is correct here — and a `commitid_prefix`
    filter must drop the family from the SCAN entirely.
    """

    pattern_for = _lock_pattern_factory(
        "ta_flake_lock", prefix_sep=":", has_commit=False
    )

    assert pattern_for({}) == "ta_flake_lock:*"
    assert pattern_for({"repoid": 7}) == "ta_flake_lock:7*"
    assert pattern_for({"commitid_prefix": "abc"}) is None
    assert pattern_for({"repoid": 7, "commitid_prefix": "abc"}) is None


def test_upload_finisher_gate_pattern_uses_underscore_prefix_sep():
    """`upload_finisher_gate_{repoid}_{commitid}` keeps the same
    `_` separator on both boundaries; explicit `prefix_sep="_"`,
    `has_commit=True` documents that.
    """

    pattern_for = _lock_pattern_factory(
        "upload_finisher_gate", prefix_sep="_", has_commit=True
    )

    assert pattern_for({"repoid": 99}) == "upload_finisher_gate_99_*"
    assert (
        pattern_for({"repoid": 99, "commitid_prefix": "deadbeef"})
        == "upload_finisher_gate_99_deadbeef*"
    )


def test_lock_attempts_pattern_uses_embedded_repoid_position():
    """Bugbot PR #888: the previous factory call generated
    `lock_attempts:{repoid}*`, but actual keys are
    `lock_attempts:{coordination_lock_name}` where the coord-lock-
    name is `<type>_lock_<repoid>_<commit>...`. The repoid lives
    deep inside the suffix, so we pushdown the embedded structure
    via `*_lock_<repoid>_*` instead.
    """

    assert _lock_attempts_pattern({}) == "lock_attempts:*"
    assert _lock_attempts_pattern({"repoid": 42}) == "lock_attempts:*_lock_42_*"
    assert (
        _lock_attempts_pattern({"repoid": 42, "commitid_prefix": "abc"})
        == "lock_attempts:*_lock_42_abc*"
    )
    # commitid-only is unsafe to push down (commitid digits could
    # collide with repoid digits at any position) — fall back to
    # the family wildcard and let the post-scan parse_key filter
    # finish the job.
    assert _lock_attempts_pattern({"commitid_prefix": "abc"}) == "lock_attempts:*"


def test_iter_keys_finds_lock_attempts_filtered_by_repoid(patched_redis):
    """End-to-end regression for bugbot bug #4: searching the locks
    admin by `repoid:42` must surface `lock_attempts:upload_lock_42_*`
    keys (previously the pushdown SCAN-MATCH excluded them).
    """

    patched_redis.set("lock_attempts:upload_lock_42_abc", "1")
    patched_redis.set("lock_attempts:upload_lock_99_xyz", "1")

    rows = {k for k, _ in iter_keys(patched_redis, family="lock_attempts", repoid=42)}

    assert rows == {"lock_attempts:upload_lock_42_abc"}


def test_iter_keys_finds_ta_notifier_fence_filtered_by_repoid_and_commit(
    patched_redis,
):
    """End-to-end regression for bugbot bug #1: a `repoid:42
    commitid:abc` filter on the locks admin must include
    `ta_notifier_fence` keys (previously the family was skipped
    entirely because `_lock_pattern_factory` returned `None` whenever
    `commitid_prefix` was set against a `sep=":"` family).
    """

    patched_redis.set("ta_notifier_fence:42_abcdef", "1")
    patched_redis.set("ta_notifier_fence:42_other00", "1")
    patched_redis.set("ta_notifier_fence:99_abcdef", "1")

    rows = {
        k
        for k, _ in iter_keys(
            patched_redis,
            family="ta_notifier_fence",
            repoid=42,
            commitid_prefix="abc",
        )
    }

    assert rows == {"ta_notifier_fence:42_abcdef"}
