# Per-Task Session Implementation

## Summary

This implementation moves database sessions from worker-level (shared) to task-level (isolated), eliminating transaction contamination issues.

## Changes Made

### 1. `apps/worker/database/engine.py`

**Added:**
- `create_task_session()` function - Creates a new isolated session per task
- `_get_task_session_maker()` - Internal function to get/create sessionmaker
- Caching of sessionmaker to avoid recreation overhead

**Key Features:**
- Uses same engine/connection pool (efficient)
- Supports timeseries routing (same as shared session)
- Creates fresh session each time (no contamination)

### 2. `apps/worker/tasks/base.py`

**Modified `run()` method:**
- Changed from `get_db_session()` to `create_task_session()`
- Added transaction state validation at start
- Added automatic commit on success (if transaction open)
- Changed cleanup to use `wrap_up_task_session()`

**Added `wrap_up_task_session()` method:**
- New method for per-task session cleanup
- Always rolls back remaining transactions
- Always closes session
- Handles errors gracefully

**Kept `wrap_up_dbsession()` method:**
- Legacy method for backward compatibility
- Still used by routing and other non-task code

**Modified `apply_async()` method:**
- Now uses temporary session for routing lookups
- Ensures routing session is cleaned up

### 3. `apps/worker/celery_task_router.py`

**Modified `route_task()` function:**
- Changed from shared `get_db_session()` to temporary `create_task_session()`
- Ensures routing session is cleaned up after use
- Prevents routing from leaving transactions open

## How It Works

### Task Execution Flow

1. **Task Starts**: `BaseCodecovTask.run()` is called
2. **Session Creation**: `create_task_session()` creates new isolated session
3. **Validation**: Checks for inherited transactions (should be none)
4. **Task Execution**: `run_impl()` executes with isolated session
5. **Success**: Commits if transaction is open
6. **Cleanup**: `wrap_up_task_session()` rolls back and closes session

### Routing Flow

1. **Routing Called**: `route_task()` or `apply_async()` called
2. **Temporary Session**: Creates temporary session for lookup
3. **Lookup**: Queries user plan and ownerid
4. **Cleanup**: Rolls back and closes temporary session
5. **No Contamination**: Routing doesn't affect task sessions

## Benefits

✅ **Eliminates Transaction Contamination**: Each task gets isolated session
✅ **Automatic Cleanup**: Session always closed in `finally` block
✅ **Backward Compatible**: Existing task code doesn't need changes
✅ **Efficient**: Uses shared connection pool (no connection overhead)
✅ **Safe**: Handles errors and timeouts gracefully

## Testing Checklist

- [ ] Run a simple task and verify it works
- [ ] Verify session is created per task (check logs)
- [ ] Test task timeout scenarios (soft and hard)
- [ ] Verify routing still works correctly
- [ ] Test tasks that use `commit.get_db_session()` (should work - returns object's session)
- [ ] Monitor connection pool usage
- [ ] Check for any transaction contamination issues

## Rollback Plan

If issues arise, you can revert by:
1. Change `run()` back to use `get_db_session()`
2. Change cleanup back to `wrap_up_dbsession()`
3. Revert routing changes

The shared session infrastructure is still in place, so rollback is straightforward.

## Notes

- Tasks that call `commit.get_db_session()` or `pull.get_db_session()` will still work
  - These methods return the session the object is bound to
  - If object was loaded in task session, it returns task session
  - This is the correct behavior
  
- Connection pool usage should be similar
  - Still uses shared pool
  - Sessions are lightweight
  - Only concurrent transactions need separate connections

