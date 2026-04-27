"""Unit tests for `RedisQueueQuerySet` against an in-process fakeredis.

These do not touch Django auth or the admin URL routes, so they run without
the Postgres test database. The end-to-end admin smoke tests live in
`test_admin_smoke.py` and are gated on the test DB being available.
"""

from __future__ import annotations

import fakeredis
import pytest

from redis_admin import conn as redis_admin_conn
from redis_admin import families as redis_admin_families
from redis_admin import settings as redis_admin_settings
from redis_admin.models import RedisQueue


@pytest.fixture
def patched_redis(monkeypatch) -> fakeredis.FakeStrictRedis:
    server = fakeredis.FakeStrictRedis()
    monkeypatch.setattr(redis_admin_conn, "get_connection", lambda: server)
    return server


def test_queryset_lists_uploads_and_flake_keys(patched_redis):
    patched_redis.rpush("uploads/1/abc", '{"upload_id": 1}')
    patched_redis.rpush("uploads/1/abc", '{"upload_id": 2}')
    patched_redis.rpush("ta_flake_key:9", "abc")

    rows = list(RedisQueue.objects.all())

    by_name = {row.name: row for row in rows}
    assert set(by_name) == {"uploads/1/abc", "ta_flake_key:9"}
    assert by_name["uploads/1/abc"].family == "uploads"
    assert by_name["uploads/1/abc"].redis_type == "LIST"
    assert by_name["uploads/1/abc"].depth == 2
    assert by_name["ta_flake_key:9"].family == "ta_flake_key"
    assert by_name["ta_flake_key:9"].depth == 1


def test_queryset_count_matches_iter(patched_redis):
    patched_redis.rpush("uploads/1/aa", "x")
    patched_redis.rpush("uploads/2/bb", "x")
    patched_redis.rpush("ta_flake_key:5", "x")

    assert RedisQueue.objects.all().count() == 3


def test_queryset_supports_slicing_for_admin_pagination(patched_redis):
    for i in range(5):
        patched_redis.rpush(f"uploads/1/c{i}", "x")

    qs = RedisQueue.objects.all().order_by("name")
    page = qs[0:2]
    assert len(page) == 2
    assert all(isinstance(item, RedisQueue) for item in page)


def test_queryset_orders_by_depth_descending(patched_redis):
    patched_redis.rpush("ta_flake_key:1", "x")
    for i in range(4):
        patched_redis.rpush("uploads/2/deep", f"x{i}")

    rows = list(RedisQueue.objects.all().order_by("-depth"))

    assert rows[0].name == "uploads/2/deep"
    assert rows[0].depth == 4


def test_queryset_filter_with_no_args_is_a_clone(patched_redis):
    patched_redis.rpush("uploads/1/aa", "x")
    assert RedisQueue.objects.filter().count() == 1


def test_queryset_filter_unknown_kwarg_raises_not_implemented(patched_redis):
    """Unrecognised filter kwargs should fail loudly so missing M3+ work
    is obvious instead of silently returning everything."""
    patched_redis.rpush("uploads/1/aa", "x")
    with pytest.raises(NotImplementedError):
        list(RedisQueue.objects.filter(repo_owner="not-a-real-field"))


def test_queryset_delete_raises_until_milestone5(patched_redis):
    patched_redis.rpush("uploads/1/aa", "x")
    with pytest.raises(NotImplementedError):
        RedisQueue.objects.all().delete()


def test_queryset_returns_none_ttl_when_no_ttl_set(patched_redis):
    patched_redis.rpush("uploads/1/no-ttl", "x")
    [row] = list(RedisQueue.objects.all())
    assert row.ttl_seconds is None


def test_queryset_returns_positive_ttl_when_set(patched_redis):
    patched_redis.rpush("uploads/1/with-ttl", "x")
    patched_redis.expire("uploads/1/with-ttl", 600)
    [row] = list(RedisQueue.objects.all())
    assert row.ttl_seconds is not None
    assert 0 < row.ttl_seconds <= 600


def test_queryset_filter_by_repoid_pushes_into_scan(patched_redis):
    patched_redis.rpush("uploads/1/abc", "x")
    patched_redis.rpush("uploads/2/def", "x")
    patched_redis.rpush("ta_flake_key:1", "x")

    rows = list(RedisQueue.objects.filter(repoid=1))
    names = {r.name for r in rows}
    assert names == {"uploads/1/abc", "ta_flake_key:1"}
    for row in rows:
        assert row.repoid == 1


def test_queryset_filter_by_family_only_returns_that_family(patched_redis):
    patched_redis.rpush("uploads/1/abc", "x")
    patched_redis.rpush("ta_flake_key:1", "x")

    rows = list(RedisQueue.objects.filter(family__exact="uploads"))
    assert {r.name for r in rows} == {"uploads/1/abc"}


def test_queryset_filter_by_commitid_prefix_post_scan(patched_redis):
    patched_redis.rpush("uploads/1/abcdef", "x")
    patched_redis.rpush("uploads/1/abczzz", "x")
    patched_redis.rpush("uploads/1/zzzzzz", "x")

    rows = list(RedisQueue.objects.filter(commitid__startswith="abc"))
    assert {r.name for r in rows} == {"uploads/1/abcdef", "uploads/1/abczzz"}


def test_queryset_filter_by_depth_gte_drops_shallow_rows(patched_redis):
    patched_redis.rpush("uploads/1/shallow", "x")
    for i in range(5):
        patched_redis.rpush("uploads/1/deep", f"x{i}")

    rows = list(RedisQueue.objects.filter(depth__gte=3))
    assert {r.name for r in rows} == {"uploads/1/deep"}


def test_queryset_filter_by_report_type_post_scan(patched_redis):
    patched_redis.rpush("uploads/1/sha", "x")  # coverage default
    patched_redis.rpush("uploads/1/sha/test_results", "x")
    patched_redis.rpush("uploads/1/sha/bundle_analysis", "x")

    rows = list(RedisQueue.objects.filter(report_type="test_results"))
    assert [r.name for r in rows] == ["uploads/1/sha/test_results"]


def test_queryset_filter_with_non_int_repoid_returns_empty(patched_redis):
    patched_redis.rpush("uploads/1/abc", "x")
    rows = list(RedisQueue.objects.filter(repoid="not-a-number"))
    assert rows == []


def test_queryset_filter_by_name_icontains_post_scan(patched_redis):
    patched_redis.rpush("uploads/1/abc", "x")
    patched_redis.rpush("ta_flake_key:1", "x")

    rows = list(RedisQueue.objects.filter(name__icontains="flake"))
    assert {r.name for r in rows} == {"ta_flake_key:1"}


def test_queryset_populates_repoid_commitid_report_type(patched_redis):
    patched_redis.rpush("uploads/77/somesha/test_results", "x")
    patched_redis.rpush("ta_flake_key:88", "x")

    rows = {r.name: r for r in RedisQueue.objects.all()}
    uploads_row = rows["uploads/77/somesha/test_results"]
    assert uploads_row.repoid == 77
    assert uploads_row.commitid == "somesha"
    assert uploads_row.report_type == "test_results"
    flake_row = rows["ta_flake_key:88"]
    assert flake_row.repoid == 88
    assert flake_row.commitid is None


def test_max_scan_keys_limits_result_set(patched_redis, settings, monkeypatch):
    settings.REDIS_ADMIN_MAX_SCAN_KEYS = 3
    # `families.iter_keys` reads MAX_SCAN_KEYS off the redis_admin settings
    # module, which captured `getattr(settings, "REDIS_ADMIN_MAX_SCAN_KEYS")`
    # at import time. Patch the captured value directly for this test.
    monkeypatch.setattr(redis_admin_settings, "MAX_SCAN_KEYS", 3)

    for i in range(5):
        patched_redis.rpush(f"uploads/1/k{i}", "x")

    keys = list(redis_admin_families.iter_keys(patched_redis))
    assert len(keys) == 3
