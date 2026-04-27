# `redis_admin`

A Django app that surfaces Redis-backed queues and locks in the
existing `/admin/` site so on-call operators can spot backed-up
queues, drill into their contents, and (with `is_superuser`) clear
subsets of them — all without shelling into Redis.

Inspired by [`wolph/django-redis-admin`][wolph]: instead of a real
database table, the registered models are unmanaged
(`Meta.managed = False`) and back themselves onto a custom
`QuerySet` that translates Django admin's calls (`iterator`,
`count`, `__getitem__`, `filter`, `delete`) into `SCAN` / `TYPE` /
`LLEN` / `LRANGE` / `SSCAN` / `HSCAN` / `DEL` / `LREM` / `SREM` /
`HDEL`. From the admin's perspective the rows look exactly like
ORM-backed rows — list_display, search, filters, pagination, and
`delete_selected` all work without custom views or templates.

[wolph]: https://github.com/wolph/django-redis-admin

## What's surfaced

| Model | Audience | Mutable? |
|---|---|---|
| `RedisQueue` | Queue families: `uploads/*`, `ta_flake_key:*`, `latest_upload/*`, `upload-processing-state/*`, `intermediate-report/*`, Celery broker queues (`celery`, `healthcheck`, `enterprise_*`, `task_routes` values) | Yes (superuser only) |
| `RedisQueueItem` | Items inside a single queue (LIST entries / SET members / HASH fields / STRING value) | Yes (superuser only) |
| `RedisLock` | Lock families: `*_lock_*`, `upload_finisher_gate_*`, `ta_flake_lock:*`, `lock_attempts:*`, `ta_notifier_fence:*`, `coordination_lock:*` | Read-only (always) |

Lock families are flagged `is_deletable=False` in the registry and
are refused by `services.redis_delete()` even if a future change
accidentally surfaces them under `RedisQueue`.

## URLs

| Path | Purpose |
|---|---|
| `/admin/redis_admin/redisqueue/` | Browse Redis queue keys |
| `/admin/redis_admin/redisqueue/<key>/change/` | Inspect a single queue, with a tabular-inline preview of its items |
| `/admin/redis_admin/redisqueueitem/?queue_name__exact=<key>` | Browse the full items of a queue |
| `/admin/redis_admin/redisqueueitem/<token>/change/` | Inspect a single item |
| `/admin/redis_admin/redislock/` | Browse Redis locks (read-only) |
| `/admin/redis_admin/redisqueue/clear-by-scope/` | M6 — cross-family clear-by-scope (superuser only) |

## Search and filters

The queues changelist's search box accepts both bare substrings and
`key:value` tokens, joined implicitly with AND:

- `repoid:1234` &mdash; every backed-up queue/key for repo 1234
- `commitid:abc` &mdash; every backed-up queue/key for commits whose SHA starts with `abc`
- `family:uploads` &mdash; only the `uploads/*` family
- `report_type:test_results` &mdash; per-report-type filter for `uploads/*` keys
- `repoid:1234 commitid:abc` &mdash; combined
- bare substring &mdash; any key whose name contains the text

When `repoid:` or `commitid:` is in the query the filter is pushed
down into a tighter `SCAN MATCH` pattern (e.g. `uploads/1234/*`)
rather than a full-keyspace scan.

The sidebar adds standalone `family`, `min depth`, and
`report_type` filters.

## Clearing subsets of Redis

Three deletion entry points, all funnel through the audited
`services.redis_delete()` and all gated on `request.user.is_superuser`:

1. **Per-queue / per-item `Delete selected`** — the standard admin
   bulk action. `delete_queryset` is overridden to dispatch through
   `redis_delete`, which issues `DEL` for queue rows and
   `LREM` / `SREM` / `HDEL` for item rows.
2. **`Dry-run: count what 'delete selected' would clear`** — sibling
   action visible to staff users. Same code path with `dry_run=True`,
   no mutations, but still writes a `LogEntry`.
3. **Clear by scope** — single endpoint at
   `/admin/redis_admin/redisqueue/clear-by-scope/`. Aggregates every
   deletable key tied to a `repoid` and/or `commitid` across *all*
   families and clears them in one operation. Required-scope guard
   refuses an empty form. Typed-confirmation guard requires the
   operator to re-type the repoid/commitid before the destructive
   button arms. Both dry-run and confirm runs leave an audit trail.

Every call writes a `django.contrib.admin.LogEntry` (action_flag =
`DELETION`) with a JSON `change_message` of the form:

```json
{
  "scope": "queue|item|queue+item",
  "dry_run": false,
  "count": 17,
  "families": ["uploads", "latest_upload"],
  "sample": ["uploads/1234/abc", "..."],
  "refused": []
}
```

so an admin can reconstruct who cleared what and when from
`/admin/admin/logentry/`.

## Settings

All configurable via `REDIS_ADMIN_*` Django settings; defaults shown.

| Setting | Default | Purpose |
|---|---|---|
| `REDIS_ADMIN_MAX_SCAN_KEYS` | `10_000` | Hard cap on keys visited per `iter_keys()` sweep. |
| `REDIS_ADMIN_SCAN_COUNT` | `500` | `COUNT` hint for each `SCAN` / `SSCAN` / `HSCAN`. |
| `REDIS_ADMIN_ITEM_PAGE_SIZE` | `100` | Page size for the items-in-a-queue view. |
| `REDIS_ADMIN_MAX_ITEMS_PER_KEY` | `5_000` | Hard cap on members materialised from a single SET/HASH for browsing. |
| `REDIS_ADMIN_MAX_DECODE_BYTES` | `4_096` | Per-value display truncation. |
| `REDIS_ADMIN_DELETE_BATCH_SIZE` | `500` | Pipeline batch size for `DEL` / `LREM` / `SREM` / `HDEL`. |
| `REDIS_ADMIN_CONNECTION_FACTORY` | unset | Dotted path to a callable returning a `redis.Redis`. Defaults to `shared.helpers.redis.get_redis_connection`. |

## Adding a new family

A "family" is a Redis key pattern owned by some part of the system,
described declaratively in `families.py::FAMILIES`. To add a new
one:

1. **Add a `Family(...)`** entry to the `FAMILIES` tuple. Required
   fields:
   - `name` — short identifier shown in `list_display` and accepted
     in `family:<name>` search.
   - `scan_pattern` — Redis glob (e.g. `"my_thing/*"`).
   - `redis_type` — one of `"list" | "set" | "hash" | "string"`.
   - `parse_key(key) -> ParsedKey | None` — extracts `repoid`,
     `commitid`, `report_type` (any may be `None`).
   - `pattern_for(filters) -> str | None` — given the active
     `repoid` / `commitid_prefix` filters, return a tightened
     `SCAN MATCH` pattern, or `None` if the family can't possibly
     match those filters (skips the SCAN entirely).
   - `is_deletable: bool` — set `False` for locks and any
     append-only auditing surface.
   - `category: "queue" | "lock"` — locks land in `RedisLock`
     instead of `RedisQueue`.
   - `decode_value(raw) -> str` (optional) — per-item display
     transform (used by `celery_broker` to unwrap kombu envelopes).
   - `fixed_keys: tuple[str, ...]` (optional) — well-known names
     that aren't discoverable via SCAN (used for the static Celery
     queue names from `BaseCeleryConfig`).

2. **Register a parser** in `families.py` returning a `ParsedKey`
   so `repoid:` / `commitid:` searches hit your family.

3. **Order matters**. `find_family()` walks `FAMILIES` in order and
   returns the first matching pattern, so put more-specific
   prefixes (e.g. `lock_attempts:*`) before broader ones (e.g.
   `*_lock_*`).

4. **Add tests** in `tests/test_families.py` covering both the
   parser (`parse_key`) and the SCAN-pattern pushdown
   (`pattern_for` returning the right tightened pattern for each
   filter shape).

## Observability

Every Redis-touching path opens a Sentry span:

| op | Span name | Where |
|---|---|---|
| `redis.admin.iter_keys` | `<category>.<family>` | `families.iter_keys` (the SCAN sweep) |
| `redis.admin.delete` | `dry_run` / `execute` | `services.redis_delete` |
| function-name (`@sentry_sdk.trace`) | per call | item materialisation (`_list_slice`, `_materialize_set`, `_materialize_hash`, `_materialize_string`) |

so a slow admin page surfaces as a discrete unit in Sentry traces
rather than disappearing into raw `redis.cmd` spans.

## Tests

```
cd apps/codecov-api
pytest redis_admin/tests
```

The full suite uses `fakeredis` so it doesn't require a running
Redis server.
