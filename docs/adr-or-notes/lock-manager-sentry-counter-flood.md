# Production issue: Lock max-retries Sentry event flood

## Summary

After deploying the bundle analysis lock cap (PR #712), Sentry issue WORKER-WYP showed **LockRetry: Lock acquisition failed after N retries (max: 10)** with N climbing (27, 33, 36, …) and **occurrences in the millions**. It looked like the task was not stopping at 10 attempts. In reality the cap was working per message, but **many distinct Celery messages** for the same repo/commit share one Redis attempt counter, and we were **reporting to Sentry on every run** when `attempts >= max_retries`. So each of those runs (11th, 12th, … 36th attempt on the shared counter) sent a new Sentry event, flooding the issue.

**Severity:** Medium (no incorrect retry behavior; Sentry noise and confusion).

**Sentry issue:** [WORKER-WYP](https://codecov.sentry.io/issues/WORKER-WYP) (e.g. `release:production-release-247740b`).

---

## What we saw in Sentry

- **Error message:** `LockRetry: Lock acquisition failed after 27 retries (max: 10)` (and 32, 33, 34, 35, 36 in recent events).
- **Context:** `lock_acquisition.attempts` = 27, 33, etc.; `max_retries` = 10; `error_type` = `lock_max_retries_exceeded`.
- **Same lock, different task IDs:** Events had different `celery_task_id` (e.g. `c774cbff-...`, `9c4dae38-...`, `0256e333-...`, `f9d07a86-...`) but same `lock_name`, `repo_id`, `commitid` (e.g. repo 19963992, commit `7d279e5f...`).
- **Volume:** Occurrences very high; “Last Seen” kept updating as new events arrived.

So in Sentry it looked like a single task was retrying way past 10, and the “counter” (attempts) kept going up.

---

## Root cause

1. **Shared Redis counter**  
   The lock attempt key is per lock name: `lock_attempts:{lock_name}` (e.g. `lock_attempts:bundle_analysis_processing_lock_19963992_7d279e5f..._bundle_analysis`). So **all** Celery tasks trying to acquire **that** lock (same repo, same commit) share one counter. This is intentional so we cap total contention for that lock, not per message.

2. **Many messages for one lock**  
   For some repos (e.g. square-web), many bundle analysis processor messages are enqueued for the same commit (e.g. many bundles/uploads). Each run that fails to get the lock calls `INCR` on the same key, so the counter goes 1, 2, … 10, 11, 12, … 36 across those messages.

3. **Reporting on every run when over cap**  
   When `attempts >= max_retries`, we raised `LockRetry(max_retries_exceeded=True)` and called `sentry_sdk.capture_exception(error)` **every time**. So the first run that saw 10 reported once; every later run (11, 12, … 36) also reported. With many messages sharing the counter, that produced a flood of Sentry events and made it look like “the counter” (and the task) never stopped.

4. **Per-message behavior was correct**  
   Each message that saw `attempts >= max_retries` was caught in the processor; we returned (e.g. `previous_result`) and acked, so each message did stop. The issue was only Sentry reporting and the shared counter climbing past 10—not tasks retrying forever.

---

## Fix (implemented)

- **Only report to Sentry when we first hit the cap:** call `sentry_sdk.capture_exception(error)` only when `attempts == max_retries`, not when `attempts > max_retries`.
- **Location:** `apps/worker/services/lock_manager.py`, in the `except LockError` block where we raise `LockRetry(max_retries_exceeded=True)`.
- **Effect:** One Sentry event per lock that hits the cap, instead of one per run when the shared counter is already ≥ 10. The task still raises and returns the same way; only Sentry volume changes.

---

## Why it wasn’t caught in the PR

- Tests model a **single** task flow (one message, one LockManager, mocked Redis 1→2→…→10). They don’t simulate many messages for the same lock all incrementing the same key (11, 12, …).
- Sentry reporting frequency (“report only when attempts == max_retries”) was not under test.
- The shared-counter design was for re-deliveries and multiple contenders; the consequence (many messages → many Sentry events if we report every time) wasn’t documented or tested.

---

## Optional follow-up

- **Product/backend:** Understand why some customers (e.g. square-web) have so many BA processor messages per commit; whether that’s expected (many bundles) or something to throttle/dedupe.
- **Observability:** Rely on the single Sentry event per lock at cap; use logs/metrics for “how often we hit the cap” and “how many messages contended” rather than Sentry occurrence count.

---

## References

- Sentry issue: [WORKER-WYP](https://codecov.sentry.io/issues/WORKER-WYP)
- Cap + return behavior: PR #712 (bundle analysis processor at 10 attempts)
- Lock attempt key: `LOCK_ATTEMPTS_KEY_PREFIX + lock_name` in `apps/worker/services/lock_manager.py`
