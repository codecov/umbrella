"""Unit tests for `redis_admin.services.redis_delete`.

Covers the matrix that's invariant across admin entry points:

- DEL on whole keys for queue families
- LREM/SREM/HDEL on items (services-only path; the admin doesn't wire
  per-item delete in M5 but the service is the single chokepoint, so
  unit-test it directly)
- Lock families refuse deletion
- dry_run=True performs zero mutations
- Audit `LogEntry` is written on every call
- Pipelining batches don't blow up on > batch_size targets
"""

from __future__ import annotations

import json

import fakeredis
import pytest
from django.contrib.admin.models import DELETION, LogEntry
from django.test import TestCase

from redis_admin import conn as redis_admin_conn
from redis_admin import settings as redis_admin_settings
from redis_admin.models import RedisLock, RedisQueue, RedisQueueItem
from redis_admin.services import redis_delete
from shared.django_apps.codecov_auth.tests.factories import UserFactory


@pytest.fixture
def patched_redis(monkeypatch) -> fakeredis.FakeStrictRedis:
    server = fakeredis.FakeStrictRedis()
    monkeypatch.setattr(
        redis_admin_conn, "get_connection", lambda kind="default": server
    )
    return server


def _make_queue(name: str, family: str = "uploads", redis_type: str = "LIST"):
    return RedisQueue(name=name, family=family, redis_type=redis_type, depth=0)


def _make_lock(name: str, family: str):
    return RedisLock(name=name, family=family, redis_type="STRING", depth=0)


@pytest.mark.django_db
class TestRedisDeleteService(TestCase):
    def setUp(self):
        self.user = UserFactory(is_staff=True, is_superuser=True)
        self.server = fakeredis.FakeStrictRedis()
        self._patcher = self._patch_connection()

    def _patch_connection(self):
        # Module-level monkeypatch so the service sees our fakeredis.
        original = redis_admin_conn.get_connection
        redis_admin_conn.get_connection = (  # type: ignore[assignment]
            lambda kind="default": self.server
        )
        self.addCleanup(setattr, redis_admin_conn, "get_connection", original)
        return None

    def test_dry_run_performs_no_mutations(self):
        self.server.rpush("uploads/1/abc", "x")
        self.server.rpush("uploads/2/def", "y")

        result = redis_delete(
            [_make_queue("uploads/1/abc"), _make_queue("uploads/2/def")],
            user=self.user,
            dry_run=True,
        )

        assert result.dry_run is True
        assert result.count == 2
        assert set(result.sample) == {"uploads/1/abc", "uploads/2/def"}
        assert "uploads" in result.families
        # Both keys still in redis.
        assert self.server.exists("uploads/1/abc") == 1
        assert self.server.exists("uploads/2/def") == 1

    def test_real_run_deletes_keys(self):
        self.server.rpush("uploads/1/abc", "x")
        self.server.rpush("uploads/2/def", "y")

        result = redis_delete(
            [_make_queue("uploads/1/abc"), _make_queue("uploads/2/def")],
            user=self.user,
            dry_run=False,
        )

        assert result.dry_run is False
        assert result.count == 2
        assert self.server.exists("uploads/1/abc") == 0
        assert self.server.exists("uploads/2/def") == 0

    def test_locks_are_refused_even_on_real_run(self):
        """Lock-flagged families MUST NOT be deletable from this service.
        This is the last line of defence against an operator mistake;
        the lock admin already hides the delete UI but the service
        refuses regardless of how the row was constructed.
        """

        self.server.set("upload_lock_1_abc", "owned")
        self.server.set("ta_flake_lock:1", "owned")

        result = redis_delete(
            [
                _make_lock("upload_lock_1_abc", family="coordination_lock"),
                _make_lock("ta_flake_lock:1", family="ta_flake_lock"),
            ],
            user=self.user,
            dry_run=False,
        )

        assert result.count == 0
        assert set(result.refused) == {"upload_lock_1_abc", "ta_flake_lock:1"}
        # Locks remain in redis.
        assert self.server.exists("upload_lock_1_abc") == 1
        assert self.server.exists("ta_flake_lock:1") == 1

    def test_unknown_families_are_refused(self):
        """Sanity: a key that doesn't match any family (operator typed
        a name into a URL) must refuse rather than `DEL` blindly.
        """
        self.server.set("totally-random-key", "v")

        result = redis_delete(
            [_make_queue("totally-random-key", family="???")],
            user=self.user,
            dry_run=False,
        )

        assert result.count == 0
        assert "totally-random-key" in result.refused
        assert self.server.exists("totally-random-key") == 1

    def test_audit_log_entry_written_on_dry_run(self):
        self.server.rpush("uploads/1/abc", "x")

        redis_delete([_make_queue("uploads/1/abc")], user=self.user, dry_run=True)

        entries = LogEntry.objects.filter(user=self.user, action_flag=DELETION)
        assert entries.count() == 1
        message = json.loads(entries.first().change_message)
        assert message["dry_run"] is True
        assert message["count"] == 1
        assert message["scope"] == "queue"
        assert "uploads" in message["families"]

    def test_audit_log_entry_written_on_real_run(self):
        self.server.rpush("uploads/1/abc", "x")

        redis_delete([_make_queue("uploads/1/abc")], user=self.user, dry_run=False)

        entries = LogEntry.objects.filter(user=self.user, action_flag=DELETION)
        assert entries.count() == 1
        message = json.loads(entries.first().change_message)
        assert message["dry_run"] is False
        assert message["count"] == 1
        assert "uploads/1/abc" in message["sample"]

    def test_pipeline_batching_handles_more_than_one_batch(self):
        """A single redis_delete call > DELETE_BATCH_SIZE keys must still
        complete; we run smaller batches to avoid one giant MULTI.
        """
        # Force batch size = 5 via monkeypatch so we don't have to add
        # 500+ keys in the test.
        original = redis_admin_settings.DELETE_BATCH_SIZE
        redis_admin_settings.DELETE_BATCH_SIZE = 5
        try:
            for i in range(12):
                self.server.rpush(f"uploads/1/sha{i:02d}", "x")
            queues = [_make_queue(f"uploads/1/sha{i:02d}") for i in range(12)]

            result = redis_delete(queues, user=self.user, dry_run=False)
            assert result.count == 12
            for i in range(12):
                assert self.server.exists(f"uploads/1/sha{i:02d}") == 0
        finally:
            redis_admin_settings.DELETE_BATCH_SIZE = original

    def test_item_lrem_deletes_specific_value(self):
        """Service-level item delete is functional (admin doesn't wire
        it in M5, but the service is the chokepoint and must work).
        """
        self.server.rpush("uploads/1/abc", "keep-1", "drop-me", "keep-2")
        item = RedisQueueItem(
            pk_token="uploads/1/abc#1",
            queue_name="uploads/1/abc",
            index_or_field="1",
            raw_value="drop-me",
        )

        result = redis_delete([item], user=self.user, dry_run=False)

        assert result.count == 1
        remaining = [v.decode() for v in self.server.lrange("uploads/1/abc", 0, -1)]
        assert remaining == ["keep-1", "keep-2"]

    def test_item_hdel_uses_index_or_field_as_selector(self):
        """HASH items use the field name as selector, not the value."""

        self.server.hset("intermediate-report/u1", mapping={"alpha": "1", "beta": "2"})
        item = RedisQueueItem(
            pk_token="intermediate-report/u1#f:alpha",
            queue_name="intermediate-report/u1",
            index_or_field="alpha",
            raw_value="1",
        )

        result = redis_delete([item], user=self.user, dry_run=False)

        assert result.count == 1
        assert self.server.hget("intermediate-report/u1", "alpha") is None
        assert self.server.hget("intermediate-report/u1", "beta") == b"2"

    def test_audit_log_written_when_only_refused(self):
        """Even a fully-refused call writes an audit row so an attempt
        to clear locks shows up in the operator history.
        """
        self.server.set("upload_lock_1_abc", "owned")
        redis_delete(
            [_make_lock("upload_lock_1_abc", family="coordination_lock")],
            user=self.user,
            dry_run=False,
        )

        entries = LogEntry.objects.filter(user=self.user, action_flag=DELETION)
        assert entries.count() == 1
        message = json.loads(entries.first().change_message)
        assert message["count"] == 0
        assert "upload_lock_1_abc" in message["refused"]

    def test_mixed_family_delete_routes_to_per_kind_redis(self):
        """A single redis_delete that spans cache + broker families
        must dispatch each batch against the Redis instance that owns
        those keys. We model the production split with two fakeredis
        servers and verify keys are deleted on the matching side and
        untouched on the other.
        """

        cache_server = fakeredis.FakeStrictRedis()
        broker_server = fakeredis.FakeStrictRedis()

        cache_server.rpush("uploads/1/abc", "x")
        broker_server.rpush("enterprise_test", "y")
        # Cross-server "ghosts" — keys with a name that *would* belong
        # to the other family but living on the wrong Redis. They must
        # be left untouched, since the service should never try to
        # DEL a cache-family key against the broker (and vice versa).
        cache_server.rpush("enterprise_ghost", "ghost-1")
        broker_server.rpush("uploads/2/ghost", "ghost-2")

        original = redis_admin_conn.get_connection
        redis_admin_conn.get_connection = (  # type: ignore[assignment]
            lambda kind="default": (
                broker_server if kind == "broker" else cache_server
            )
        )
        try:
            result = redis_delete(
                [
                    _make_queue("uploads/1/abc"),
                    _make_queue(
                        "enterprise_test",
                        family="celery_broker",
                        redis_type="LIST",
                    ),
                ],
                user=self.user,
                dry_run=False,
            )
        finally:
            redis_admin_conn.get_connection = original  # type: ignore[assignment]

        assert result.count == 2
        # Targeted keys gone from their respective servers...
        assert cache_server.exists("uploads/1/abc") == 0
        assert broker_server.exists("enterprise_test") == 0
        # ...and the cross-server ghosts are still intact, proving the
        # delete didn't fan out into the wrong instance.
        assert cache_server.exists("enterprise_ghost") == 1
        assert broker_server.exists("uploads/2/ghost") == 1

    def test_item_delete_refuses_celery_lists(self):
        """Bugbot PR #888: `_classify_items` used `item.raw_value` as
        the LREM selector. For families with `decode_value` (today:
        `celery_broker`), `raw_value` carries the
        `[task=… repoid=… commit=…]` display prefix that's NOT in
        Redis, so LREM matches zero rows. Refuse item-level
        LIST/SET delete for those families and let operators clear
        the whole queue from `RedisQueueAdmin` instead.
        """

        self.server.rpush("celery", b"raw-payload-bytes")
        # Simulate what `_build_list_items` produces for a celery
        # row: raw_value carries the decode-prefix.
        item = RedisQueueItem(
            pk_token="celery#0",
            queue_name="celery",
            index_or_field="0",
            raw_value="[task=app.foo repoid=42 commit=abc] raw-payload-bytes",
        )

        result = redis_delete([item], user=self.user, dry_run=False)

        assert result.count == 0
        assert "celery#0" in result.refused
        # Most importantly: the original Redis payload is still
        # there. Without the refusal, LREM would silently no-op
        # against the prefixed selector and report success.
        assert self.server.llen("celery") == 1
