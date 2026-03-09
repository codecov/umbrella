# Production bug: Lock attempt counter INCR/EXPIRE non-atomicity

## Summary

When lock acquisition fails, `LockManager` increments a Redis counter (`INCR`) and, when the counter is new (`attempts == 1`), sets a TTL (`EXPIRE`). These are two separate, non-atomic operations. If `INCR` succeeds but `EXPIRE` fails (e.g. Redis timeout or connection error), the counter key is left **without a TTL**. Because we only clear the counter on **successful** lock acquisition, that key can remain in Redis indefinitely. Every future attempt for the same lock will see `attempts >= max_retries` and immediately raise `LockRetry(max_retries_exceeded=True)`, effectively **poisoning** that lock until the key is manually removed or Redis is restarted.

**Severity:** High (lock permanently unusable for that repo/commit until remediation).

**Source:** [PR #712 review – Sentry bot](https://github.com/codecov/umbrella/pull/712#pullrequestreview-3821240104).

---

## Affected code

**File:** `apps/worker/services/lock_manager.py`  
**Location:** `LockManager.locked()`, in the `except LockError` block (lines 179–181 in current code):

```python
attempts = self.redis_connection.incr(attempt_key)
if attempts == 1:
    self.redis_connection.expire(attempt_key, LOCK_ATTEMPTS_TTL_SECONDS)
```

- `attempt_key` = `lock_attempts:{lock_name}` (e.g. `lock_attempts:bundle_analysis_processing_lock_19963992_7d279e5f..._bundle_analysis`).
- `LOCK_ATTEMPTS_TTL_SECONDS` = 86400 (24 hours).

---

## Root cause

1. `INCR` and `EXPIRE` are two separate Redis commands with no atomicity guarantee.
2. If the process or connection fails between `INCR` and `EXPIRE`, or if `EXPIRE` raises (e.g. `RedisConnectionError` / `RedisTimeoutError`), the key already exists with no TTL.
3. We only delete the key in `_clear_lock_attempt_counter`, which is called from the `finally` block **inside** the successful `with self.redis_connection.lock(...):` branch. So after any failure we never clear the key.
4. A key with no TTL never expires. The next time any task tries to acquire that lock, `INCR` returns 11, 12, … and we immediately hit `attempts >= max_retries` and raise. No task can ever acquire that lock again until the key is deleted.

---

## Observed / possible impact

- **Observed:** Sentry review flagged this as HIGH severity; no confirmed production incident yet, but the failure mode is clear.
- **Possible:** Under Redis timeouts or restarts during lock contention, one or more `lock_attempts:*` keys could be left without TTL, causing specific repo/commit locks to appear “stuck” (every attempt fails with “Lock acquisition failed after N retries (max: 10)” with N never resetting).

---

## Fix plan

### Goal

Ensure that for the lock attempt counter we never leave a key in Redis without a TTL. Either both “increment (or init)” and “set TTL” succeed together, or we don’t leave a long-lived key.

### Option A: Redis transaction (MULTI/EXEC) — recommended

- Start a transaction.
- `INCR attempt_key`.
- If the design allows, set TTL in the same transaction. Note: in a strict MULTI/EXEC, the result of `INCR` isn’t available until EXEC, so “only set TTL when attempts == 1” requires either:
  - **Variant 1:** Always `EXPIRE attempt_key LOCK_ATTEMPTS_TTL_SECONDS` in the same transaction (safe: existing key’s TTL is refreshed; no key is left without TTL).
  - **Variant 2:** Use a Lua script (see Option B) if we must set TTL only when the key was just created.

**Recommended variant:** In the transaction, run `INCR attempt_key` and then `EXPIRE attempt_key LOCK_ATTEMPTS_TTL_SECONDS` every time (not only when `attempts == 1`). That keeps the key’s TTL refreshed on every failure and avoids leaving the key without TTL. Cost: one extra EXPIRE per failed attempt (acceptable).

**Steps:**

1. In `lock_manager.py`, in the `except LockError` block, replace the current `incr` + conditional `expire` with a pipeline/transaction:
   - `pipe = self.redis_connection.pipeline()`
   - `pipe.incr(attempt_key)`
   - `pipe.expire(attempt_key, LOCK_ATTEMPTS_TTL_SECONDS)`
   - `results = pipe.execute()`
   - `attempts = results[0]`
2. Handle pipeline execution errors (e.g. Redis connection/timeout) so we don’t leave a half-applied state; if the pipeline fails, the key may or may not exist—consider whether to retry or fail the task (current behavior on Redis errors is to let the exception propagate).
3. Add a unit test that mocks the Redis pipeline and verifies both INCR and EXPIRE are called and that we read `attempts` from the pipeline result.
4. (Optional) Add an integration test that simulates EXPIRE failing after INCR and verifies we don’t leave a key without TTL (e.g. with a fake Redis or a real Redis and a hook that fails the second command).

### Option B: Lua script

- One script that runs on the server: “INCR key; EXPIRE key TTL; return INCR result.” Atomic and no key is left without TTL.
- Slightly more moving parts (script registration/caching); use if the team prefers script-based atomicity over pipelines.

### Option C: EXPIRE on every failure (minimal change)

- Keep `INCR` as is; always call `EXPIRE attempt_key LOCK_ATTEMPTS_TTL_SECONDS` after every `INCR` (not only when `attempts == 1`). If EXPIRE fails, we still have the same risk (key without TTL). So this **reduces** the window (we set TTL on every failure) but does **not** make the two operations atomic. Only adopt as a quick mitigation if Option A is delayed; still fix properly with A or B.

---

## Recommendation

- **Implement Option A (pipeline with INCR + EXPIRE every time)** so that we never create or update the attempt counter without setting TTL in the same logical step.
- Add a unit test for the new pipeline and for “attempts” read from the pipeline result.
- After deploy, monitor for any `lock_attempts:*` keys without TTL (e.g. Redis CLI or a small admin script) to confirm no poisoning.

---

## References

- [PR #712 – Cap bundle analysis processor at 10 attempts](https://github.com/codecov/umbrella/pull/712)
- [Sentry bot review – INCR/EXPIRE atomicity](https://github.com/codecov/umbrella/pull/712#pullrequestreview-3821240104)
- Redis pipeline: [redis-py pipeline](https://redis-py.readthedocs.io/en/stable/advanced_features.html#pipelines)
