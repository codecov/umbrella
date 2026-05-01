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
| `RedisQueue` | Queue families excluding celery: `uploads/*`, `ta_flake_key:*`, `latest_upload/*`, `upload-processing-state/*`, `intermediate-report/*`. Celery broker queues are hidden here (see `CeleryBrokerQueue` row below). | Yes (superuser only) |
| `RedisQueueItem` | Items inside a single non-celery queue (LIST entries / SET members / HASH fields / STRING value). | Yes (superuser only) |
| `CeleryBrokerQueue` | Two-mode admin for `celery_broker`. **Summary mode** (no `queue_name__exact`): one row per known queue with `LLEN` and a drill-in link. **Drill-down mode** (`?queue_name__exact=<queue>`): one row per kombu message, with the envelope parsed into `task_name` / `repoid` / `commitid` / `task_id` / `ownerid` / `pullid` columns + a `(repoid, commitid)` frequency chart above the table. The default discovery surface for celery queues. | Yes (superuser only) |
| `RedisLock` | Lock families: `*_lock_*`, `upload_finisher_gate_*`, `ta_flake_lock:*`, `lock_attempts:*`, `ta_notifier_fence:*`, `coordination_lock:*` | Read-only (always) |

Lock families are flagged `is_deletable=False` in the registry and
are refused by `services.redis_delete()` even if a future change
accidentally surfaces them under `RedisQueue`.

## URLs

| Path | Purpose |
|---|---|
| `/admin/redis_admin/redisqueue/` | Browse non-celery queue families (celery_broker is hidden via `family_exclude`) |
| `/admin/redis_admin/redisqueue/<key>/change/` | Inspect a single queue, with a tabular-inline preview of its items |
| `/admin/redis_admin/redisqueueitem/?queue_name__exact=<key>` | Browse the full items of a queue (non-celery families) |
| `/admin/redis_admin/redisqueueitem/<token>/change/` | Inspect a single item |
| `/admin/redis_admin/celerybrokerqueue/` | **Celery summary** — one row per known celery_broker queue with current `LLEN` and a drill-in link |
| `/admin/redis_admin/celerybrokerqueue/?queue_name__exact=<queue>` | **Celery drill-down** — messages inside a celery broker queue with structured task / repoid / commitid columns, a `(repoid, commitid)` frequency chart, and per-message clear |
| `/admin/redis_admin/celerybrokerqueue/clear-by-filter/` | Preview + clear messages in `<queue>` matching a `(task_name?, repoid?, commitid?)` filter (POST-only, superuser-only). Wired up by the frequency chart's per-row "Clear queue" button; the preview page exposes three explicit actions (dry-run, clear all but first, clear all). |
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

## Permissions

The "deletion is superuser-only" invariant is enforced at four
layers, redundantly on purpose, so a misconfiguration in any one
layer still leaves the others in place:

1. `RedisQueueAdmin.has_delete_permission` returns `is_superuser`.
   `RedisLockAdmin.has_delete_permission` returns `False` (locks are
   blanket-disabled — even superusers can't delete them; the
   worker's `LockManager` is the only legitimate releaser).
   `RedisQueueItemAdmin.has_delete_permission` returns `False`
   (item-level mutation isn't exposed; clear the whole queue
   instead).
2. Every bulk action (`clear_selected`, `clear_dry_run`) declares
   `permissions=("delete",)`, so Django strips them from the action
   dropdown for non-superusers and refuses raw POSTs.
3. `clear_by_scope_view` raises `PermissionDenied` early if
   `request.user.is_superuser` is false.
4. `services.redis_delete()` is the single mutation choke point;
   everything destructive funnels through it, including dry-runs,
   so there's a single source of audit-log truth.

Layer 1 is the "buttons aren't even rendered" guard; layers 2-3 are
the "raw URL/POST is refused" guard; layer 4 keeps the audit log
honest.

## Clearing subsets of Redis

Three deletion entry points, all funnel through the audited
`services.redis_delete()` and all gated on `request.user.is_superuser`:

1. **`Clear selected (dry-run preview, then confirm)`** — the bulk
   action that replaces Django's stock `delete_selected`. Two-stage
   flow: stage 1 runs `redis_delete(dry_run=True)` and renders a
   confirmation page with the dry-run's count, families, refused
   keys, and a sample list rendered inline; stage 2 (when the form
   re-posts with `confirm=yes`) runs `redis_delete(dry_run=False)`.
   Operators cannot reach the destructive button without first seeing
   the dry-run output. The stock `delete_selected` is stripped in
   `RedisQueueAdmin.get_actions` so there's no no-dry-run path at
   all.
2. **`Dry-run: count what 'clear selected' would clear`** — sibling
   action that's also superuser-only (`permissions=("delete",)`).
   Standalone dry-run with no destructive button on the same page;
   useful for "let me see what would happen" workflows. Still writes
   a `LogEntry`. Originally staff-allowed; tightened to superuser
   because (a) it lives next to the destructive button, and "anything
   adjacent to a delete button is superuser-only" is the cleaner
   invariant, and (b) it writes audit log rows, which is the kind of
   thing you don't want unprivileged staff users spamming.
3. **Clear by scope** — single endpoint at
   `/admin/redis_admin/redisqueue/clear-by-scope/`. Aggregates every
   deletable key tied to a `repoid`, `commitid`, and/or explicit
   `family` list and clears them in one operation. Scope is the
   cross-product of those three optional dimensions — at least one
   must be set; a fully blank form is refused. Family is a multi-
   select; leaving it blank sweeps every deletable family. Typed-
   confirmation guard requires the operator to re-type the primary
   scope value (repoid &gt; commitid &gt; joined family names) before
   the destructive button arms. Both dry-run and confirm runs leave
   an audit trail.

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
| `REDIS_ADMIN_MAX_ITEMS_PER_KEY` | `20_000` | Hard cap on members materialised from a single SET/HASH for admin browsing (M4). LIST keys use bounded LRANGE windows so they aren't constrained here. |
| `REDIS_ADMIN_CELERY_BROKER_DISPLAY_LIMIT` | `2_000` | Number of per-message rows materialised on the `celery_broker` changelist drill-down. Smaller than the scan limit because each row keeps a parsed envelope in the per-request cache. |
| `REDIS_ADMIN_CELERY_BROKER_SCAN_LIMIT` | `100_000` | Sample window used by the `celery_broker` frequency chart aggregator. The streaming clear (Clear queue → Clear all) intentionally is *not* bounded by this — it walks the full `LLEN(queue)` so a clear action always drains the entire queue regardless of the chart's sample size. |
| `REDIS_ADMIN_MAX_DECODE_BYTES` | `4_096` | Per-value display truncation. |
| `REDIS_ADMIN_DELETE_BATCH_SIZE` | `500` | Pipeline batch size for `DEL` / `LREM` / `SREM` / `HDEL`. |
| `REDIS_ADMIN_CONNECTION_FACTORY` | unset | Dotted path to a callable returning a `redis.Redis`. Defaults to `shared.helpers.redis.get_redis_connection`. |
| `REDIS_ADMIN_BROKER_CONNECTION_FACTORY` | unset | Dotted path for the Celery broker connection. Defaults to building a client from `services.celery_broker` (falls back to `services.redis_url` for single-Redis dev/enterprise). |

The `celery_broker` family also reads one shared-config knob (not a
Django setting) to learn which Celery queue names to enumerate
beyond the `BaseCeleryConfig` defaults:

| Config key | Form | Purpose |
|---|---|---|
| `setup.redis_admin.celery_queues` | comma-separated string or list | Operator-supplied list of broker queue names (e.g. `uploads,notify,sync,...`). Reads via `shared.config.get_config("setup", "redis_admin", "celery_queues")`, so an env var like `SETUP__REDIS_ADMIN__CELERY_QUEUES=...` works on the API pod. The actual production list lives in private deployment config (it varies per environment), so this isn't shipped with defaults. Each name is `EXISTS`-checked before being yielded — listing a queue that doesn't exist is harmless. |

Without this set the admin's `celery_broker` family only sees
`celery`, `healthcheck`, and whatever the `enterprise_*` SCAN
matches; queues like `uploads` / `notify` / `sync` stay invisible
even when the broker has them, because the API pod doesn't carry
the worker-side `setup.tasks.*.queue` env vars and so
`BaseCeleryConfig.task_routes` resolves every route to
`task_default_queue`.

## Celery broker admin

`CeleryBrokerQueueAdmin` is a two-mode polymorphic admin: the same
URL serves a queue-summary landing page or a per-message drill-
down depending on whether the request carries a
`?queue_name__exact=<queue>` filter. Other families keep using
`RedisQueue` / `RedisQueueItem` unchanged; `RedisQueueAdmin`
calls `family_exclude("celery_broker")` so the celery queues
don't double up between the two admins.

### Summary mode (`/admin/redis_admin/celerybrokerqueue/`)

The default landing page when an on-call engineer clicks through
from Grafana. One row per known celery queue:

| Column | Source |
|---|---|
| `queue_name` | `_resolve_celery_queue_names()` (`BaseCeleryConfig.task_routes` + `celery`/`healthcheck` defaults + operator-supplied `setup.redis_admin.celery_queues`) |
| `depth` | `LLEN(<queue>)` |
| `messages` | Drill-in link "view N message(s) →" that re-renders the same admin with `?queue_name__exact=<queue>` |

`get_list_filter` returns `()` and `get_actions` strips the bulk
clear actions in summary mode — the page is intentionally a
read-only "pick a queue" surface; clears live on the drill-down.

### Drill-down mode (`?queue_name__exact=<queue>`)

One row per kombu message inside the selected queue:

| Column | Source |
|---|---|
| `index_in_queue` | LRANGE position |
| `task_name` | `headers.task` (e.g. `app.tasks.bundle_analysis.BundleAnalysisProcessor`) |
| `repoid` / `commitid` | body kwargs |
| `ownerid` / `pullid` | body kwargs (for owner / pull-shaped tasks) |
| `task_id` | `headers.id` (kombu UUID; useful for cross-referencing flower / Sentry) |
| `payload_preview` | the body kwargs dict, rendered as one line. Known-large keys (`commit_yaml`, `processing_results`, `current_yaml`, `report_json`, `raw_upload`, `commit_yaml_dict`) are replaced with a `<truncated: N chars>` placeholder so a single 97 KB envelope can't blow past `MAX_DECODE_BYTES`. |

Search bar tokens (in addition to bare substrings, which match
`task_name__icontains`):

- `repoid:1234` &mdash; exact repoid
- `commit:abc` / `commitid:abc` &mdash; commit prefix (works with the 7-char abbrev or the full SHA)
- `task:Notify` / `task_name:Notify` &mdash; task substring
- `task_id:<uuid>` &mdash; full kombu task id
- `ownerid:N` / `pullid:N` &mdash; for owner / pull-shaped tasks
- `queue:notify` &mdash; switch the active queue without leaving the page

Sidebar filters: queue (well-known celery queue names), celery
task (dynamic dropdown built from the `task_name` values present
in the currently filtered queue), repoid, commit prefix.

### Frequency chart

Above the message table on the drill-down view, the admin renders
a `(task_name, repoid, commitid)` frequency chart computed from
the same `LRANGE` snapshot the changelist iterated. One row per
top-N bucket, sorted by `(count desc, task_name asc, repoid asc,
commitid asc)` for a stable order across reloads. Each row shows
(in column order):

- `repoid` rendered as a `service:owner/name` link to the standard
  `admin:core_repository_change` page (matches the
  `repo_display` column on `RedisQueueAdmin`); falls back to the
  bare repoid when the repo isn't in the DB.
- the 7-char `commitid` prefix (full SHA on hover)
- `task` (the kombu `headers.task` value, e.g.
  `app.tasks.notify.NotifyTask`; truncated with full path on
  hover)
- the bucket's `count` and percentage share of the visible window
- a single per-bucket **Clear queue…** button (superuser-only).
  Clicking it POSTs the bucket's
  `(queue, task_name, repoid, commitid)` to `clear-by-filter/`
  and lands on the preview page, where the operator picks one of
  three explicit actions (dry-run / clear all but first / clear
  all). The chart row itself is intentionally non-destructive —
  no count in the label, no `deletelink` styling — because the
  click only opens a page; the destructive choice is made there.

Grouping by `task_name` matters on shared queues like the default
`celery` queue where multiple task classes coexist — without it,
"clear all messages for repo X commit Y" would silently drop
unrelated tasks routed through the same queue.

The chart's percentages are computed against the visible
`LRANGE 0 CELERY_BROKER_SCAN_LIMIT-1` window, not against `LLEN`.
When `LLEN > CELERY_BROKER_SCAN_LIMIT` the chart surfaces a
"showing top N of M sampled messages (queue depth: K)" banner so
operators know the share is over the visible window.

### `clear-by-filter/` (chart-driven targeted clear)

`POST /admin/redis_admin/celerybrokerqueue/clear-by-filter/` —
superuser-only. Required form fields: `queue_name`, plus at
least one of `task_name` / `repoid` / `commitid` (refusing the
empty-narrowing case keeps the surface from overlapping
`clear-by-scope/`).

The chart's per-row button POSTs without an `action`, which
lands on a preview page listing the matched messages. The
preview page then exposes three explicit submit buttons; the
view dispatches on the form's `action` value:

- `action=dry_run` — neutral / `default`-styled button. Audited
  via `celery_broker_clear(dry_run=True)`; queue untouched.
  Re-renders the preview with an info banner. No
  typed-confirmation required.
- `action=clear_keep_one` — destructive (red text). Only
  rendered when `match_count >= 2`. Clears every match EXCEPT
  the one with the lowest `index_in_queue` (the next message a
  Celery worker would pop), so a single representative stays in
  flight. Intent: "drop the duplicate retries but leave one
  running."
- `action=clear_all` — destructive (red text). Clears every
  matching message; the broadest of the three.

Both destructive buttons gate on the same typed-confirmation
field (re-type the queue name); they share a common
`celery-destructive-button` class that paints the text red and
keeps them visually distinct from the dry-run button on the
left. Server-side, both ultimately funnel through
`services.celery_broker_clear`, so the LSET-tombstone path runs
and the audit log captures the operation under
`scope="celery_broker_clear"`.

For backward compatibility, the older `action=confirm` form
shape (paired with `mode=all` / `mode=keep_one`) is aliased to
the new actions so stale tabs and scripts keep working.

### Per-message clear (LSET-tombstone)

Item-level deletion of celery messages goes through
`services.celery_broker_clear`, not `redis_delete`. The reason is
that `redis_delete`'s `_classify_items` deliberately refuses
celery_broker items because the family's `decode_value`
transformer rewrites the raw bytes into a
`[task=… repoid=… commit=…]` summary, and the decorated string
won't round-trip back to `LREM` (which matches on exact bytes).

`celery_broker_clear` solves this with the canonical LSET-
tombstone idiom:

1. For each selected message, `LSET <queue> <idx> <sentinel>`
   (sentinel = `__redis_admin_celery_tombstone__:<uuid>`).
2. Once all sentinels are written, one `LREM <queue> 0 <sentinel>`
   sweeps them all out of the list.

Per-call UUID means simultaneous clears can't step on each
other's sentinels. Race-safe under concurrent celery consumers:
even if a worker `BLPOP`s from the head of the list between our
LSETs and our LREM, the popped (sentinel) message is recognised at
task-decode time as garbage and discarded, and we still get a
consistent count from LREM.

The same dry-run + audit log apparatus as `redis_delete` is
reused: `LogEntry.change_message` with `scope =
"celery_broker_clear"`, count, sample, families. Superuser-only
(`has_delete_permission`) and dry-run-required (`clear_dry_run`
must run before the destructive `clear_selected` arms).

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
