"""Unit tests for the M3 key parsers and SCAN-pattern pushdown.

`ParsedKey` and the `pattern_for` helpers are pure functions so these
tests don't need a Redis (fake or real) at all.
"""

from __future__ import annotations

import fakeredis
import pytest

from redis_admin import conn as redis_admin_conn
from redis_admin.families import (
    FAMILIES,
    _parse_ta_flake_key,
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
