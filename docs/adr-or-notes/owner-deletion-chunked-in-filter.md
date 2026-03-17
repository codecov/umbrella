# Owner deletion: chunked IN filter

## Summary

When deleting an owner with tens of thousands of child rows (uploads, commits, repositories, etc.), the cleanup service was generating `DELETE ... WHERE id IN (...)` SQL statements with the full list of IDs embedded directly in the query text. For large owners this could mean hundreds of kilobytes of SQL in a single statement, which caused connection drops and broke the cleanup job mid-run.

**Fix:** [PR #731](https://github.com/codecov/umbrella/pull/731) splits those materialized ID lists into chunks (default: 10,000 IDs each) so each individual DELETE statement stays well within safe size limits.

---

## Background: how cleanup works

Deleting an `Owner` requires also deleting everything associated with it across many tables — repositories, commits, uploads, reports, etc. The cleanup service builds a dependency graph of all related models and issues DELETE statements in the correct order (children before parents).

To know *which rows* to delete in each child table, it needs to pass along the IDs from the parent query. There are two ways to do this:

**Subquery** — the DB resolves IDs at query time:
```sql
DELETE FROM repositories WHERE owner_id IN (SELECT id FROM owners WHERE ...)
```

**Materialized list** — Python fetches the IDs first, then embeds them directly:
```sql
DELETE FROM repositories WHERE owner_id IN (1, 2, 3, 42, ...)
```

The materialized approach (`simplified_lookup`) was added previously because it produces better query plans — the DB knows exactly how many rows it's dealing with instead of having to estimate. For typical owners this works well.

---

## The problem

For large owners, `simplified_lookup` can materialize tens of thousands of IDs. Those IDs all end up as a literal list inside the SQL string:

```sql
DELETE FROM uploads WHERE id IN (1, 2, 3, 4, ... 50000 values ...)
```

A statement with 50,000 IDs can be ~250–450 KB of text (each integer ID plus separators). Statements that large caused the database connection to drop, leaving cleanup in a partially-completed state.

---

## The fix

PR #731 adds a chunking step (`_chunked_in_filter`) between materializing the ID list and building the DELETE querysets.

**Before:** one queryset (and therefore one DELETE statement) per field/filter combination, regardless of list size.

**After:** if the materialized list exceeds `delete_chunk_size`, it's split into multiple querysets — one per chunk:

```
-- chunk 1
DELETE FROM uploads WHERE id IN (1, ..., 10000)
-- chunk 2
DELETE FROM uploads WHERE id IN (10001, ..., 20000)
-- etc.
```

The chunk size defaults to 10,000 and is configurable via `cleanup.delete_chunk_size` in `codecov.yml` without requiring a code change.

Subquery-based filters are passed through unchanged — chunking only applies to the materialized list path.

**Key files:**
- `apps/worker/services/cleanup/relations.py` — `_chunked_in_filter`, `_get_delete_chunk_size`, and the updated `build_relation_graph` callsite
- `apps/worker/services/cleanup/tests/test_relations.py` — new tests for passthrough, empty list, split behavior, and integration

---

## Trade-offs

**Pros**
- Eliminates connection drops on large owner deletions
- Chunk size is tunable via config
- Only affects the materialized list path; subquery path is unchanged
- Debug logging when a split occurs, so it's visible in logs

**Cons**
- More database round-trips per cleanup run — 50,000 rows that were one DELETE are now five. For very large owners this makes cleanup slower.
- Deletes within a single model are no longer atomic — if the process crashes between chunks, some rows will be deleted and others won't. This was already a property of the broader cleanup design, not a new regression.

The trade-off is acceptable: cleanup is a background job where slower-but-reliable is preferable to fast-but-crashes.
