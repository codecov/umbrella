"""Unit tests for `RedisItemQuerySet` against fakeredis (no DB needed)."""

from __future__ import annotations

import fakeredis
import pytest

from redis_admin import conn as redis_admin_conn
from redis_admin import settings as redis_admin_settings
from redis_admin.models import RedisQueueItem


@pytest.fixture
def patched_redis(monkeypatch) -> fakeredis.FakeStrictRedis:
    server = fakeredis.FakeStrictRedis()
    monkeypatch.setattr(redis_admin_conn, "get_connection", lambda: server)
    return server


def test_unfiltered_queryset_is_empty(patched_redis):
    patched_redis.rpush("uploads/1/abc", "x")
    assert list(RedisQueueItem.objects.all()) == []
    assert RedisQueueItem.objects.all().count() == 0


def test_filter_by_queue_name_exact_returns_list_items(patched_redis):
    patched_redis.rpush("uploads/1/abc", '{"upload_id": 1}')
    patched_redis.rpush("uploads/1/abc", '{"upload_id": 2}')
    patched_redis.rpush("uploads/1/abc", '{"upload_id": 3}')

    qs = RedisQueueItem.objects.filter(queue_name__exact="uploads/1/abc")

    rows = list(qs)
    assert len(rows) == 3
    assert [row.queue_name for row in rows] == ["uploads/1/abc"] * 3
    assert [row.index_or_field for row in rows] == ["0", "1", "2"]
    assert [row.raw_value for row in rows] == [
        '{"upload_id": 1}',
        '{"upload_id": 2}',
        '{"upload_id": 3}',
    ]
    assert [row.pk_token for row in rows] == [
        "uploads/1/abc#0",
        "uploads/1/abc#1",
        "uploads/1/abc#2",
    ]


def test_count_returns_real_llen_for_lists(patched_redis):
    for i in range(7):
        patched_redis.rpush("uploads/1/abc", f"x{i}")
    assert RedisQueueItem.objects.filter(queue_name__exact="uploads/1/abc").count() == 7


def test_slice_pagination_uses_lrange(patched_redis):
    for i in range(10):
        patched_redis.rpush("uploads/1/abc", f"x{i}")
    qs = RedisQueueItem.objects.filter(queue_name__exact="uploads/1/abc")

    page = qs[3:6]

    assert [row.index_or_field for row in page] == ["3", "4", "5"]
    assert [row.raw_value for row in page] == ["x3", "x4", "x5"]
    assert [row.pk_token for row in page] == [
        "uploads/1/abc#3",
        "uploads/1/abc#4",
        "uploads/1/abc#5",
    ]


def test_iter_caps_at_item_page_size(patched_redis, monkeypatch):
    monkeypatch.setattr(redis_admin_settings, "ITEM_PAGE_SIZE", 5)
    for i in range(20):
        patched_redis.rpush("uploads/1/abc", f"x{i}")

    qs = RedisQueueItem.objects.filter(queue_name__exact="uploads/1/abc")

    # __iter__ honors ITEM_PAGE_SIZE so naive `list(qs)` cannot blow up.
    rows = list(qs)
    assert len(rows) == 5
    # count() still reports the real length so paginator can navigate.
    assert qs.count() == 20


def test_long_value_is_truncated_with_suffix(patched_redis, monkeypatch):
    monkeypatch.setattr(redis_admin_settings, "MAX_DECODE_BYTES", 16)
    patched_redis.rpush("uploads/1/abc", "0123456789ABCDEFGHIJ")  # 20 chars

    [row] = list(RedisQueueItem.objects.filter(queue_name__exact="uploads/1/abc"))

    assert row.raw_value.startswith("0123456789ABCDEF")
    assert "+4 chars truncated" in row.raw_value


def test_unsupported_filter_kwarg_raises(patched_redis):
    with pytest.raises(NotImplementedError):
        RedisQueueItem.objects.filter(
            queue_name__exact="uploads/1/abc", redis_type="LIST"
        )


def test_filter_by_unknown_queue_name_returns_empty(patched_redis):
    assert list(RedisQueueItem.objects.filter(queue_name__exact="missing")) == []
    assert RedisQueueItem.objects.filter(queue_name__exact="missing").count() == 0


def test_set_and_hash_queues_report_zero_until_milestone_4(patched_redis):
    # M2 only handles LIST. SET/HASH/STRING families arrive in M4; until then
    # the item view reports zero so the changelist renders cleanly without
    # blowing up.
    patched_redis.sadd("upload-processing-state/1/abc", "x", "y")
    patched_redis.hset("intermediate-report/1/abc", mapping={"a": "1", "b": "2"})

    set_qs = RedisQueueItem.objects.filter(
        queue_name__exact="upload-processing-state/1/abc"
    )
    hash_qs = RedisQueueItem.objects.filter(
        queue_name__exact="intermediate-report/1/abc"
    )
    assert set_qs.count() == 0
    assert hash_qs.count() == 0
    assert list(set_qs) == []
    assert list(hash_qs) == []


def test_delete_raises_until_milestone_5(patched_redis):
    patched_redis.rpush("uploads/1/abc", "x")
    qs = RedisQueueItem.objects.filter(queue_name__exact="uploads/1/abc")
    with pytest.raises(NotImplementedError):
        qs.delete()
