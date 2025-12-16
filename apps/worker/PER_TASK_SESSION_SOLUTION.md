# Per-Task Session Solution

## Overview

This document provides a code solution for moving database connections from Celery worker level to task level, eliminating transaction contamination while handling Celery's routing needs.

## Current Architecture

**Worker-Level Session:**
```python
# database/engine.py
session = session_factory.create_session()  # scoped_session at module level
get_db_session = session  # Shared across all tasks

# tasks/base.py
def run(self, *args, **kwargs):
    db_session = get_db_session()  # Gets shared session
    # ... task execution
    finally:
        self.wrap_up_dbsession(db_session)  # Commits/closes shared session
```

**Problem**: Shared session causes transaction contamination when tasks fail.

## Solution: Per-Task Sessions

### Architecture

**Key Changes:**
1. Keep engine at worker level (shared connection pool)
2. Create new session per task (not shared)
3. Ensure proper cleanup in `finally` block
4. Handle Celery routing separately (routing happens before task execution)

### Implementation

#### Step 1: Modify `database/engine.py`

```python
# database/engine.py

# Keep engine at module level (shared connection pool)
session_factory = SessionFactory(...)
engine = session_factory.main_engine  # Expose engine for per-task sessions

# Keep get_db_session for backward compatibility (routing, etc.)
# But mark as deprecated for task use
get_db_session = session_factory.create_session()  # Still used for routing

# New function for per-task sessions
def create_task_session():
    """Create a new session for a task. Caller is responsible for cleanup."""
    return sessionmaker(bind=engine)()
```

#### Step 2: Modify `tasks/base.py`

```python
# tasks/base.py

from database.engine import create_task_session, get_db_session
from sqlalchemy.orm import Session

class BaseCodecovTask(celery_app.Task):
    @sentry_sdk.trace
    def run(self, *args, **kwargs):
        with self.task_full_runtime.time():
            # Create NEW session for this task
            db_session = create_task_session()
            
            # Validate session is clean (no inherited transaction)
            if db_session.in_transaction():
                log.warning(
                    "New session has open transaction, rolling back",
                    extra={"task": self.name, "task_id": self.request.id}
                )
                db_session.rollback()
            
            log_context = LogContext(...)
            # ... existing log context setup ...
            
            try:
                with self.task_core_runtime.time():
                    return self.run_impl(db_session, *args, **kwargs)
            except InterfaceError as ex:
                # ... existing error handling ...
            except (DataError, IntegrityError):
                db_session.rollback()
                # ... existing retry logic ...
            except SQLAlchemyError as ex:
                db_session.rollback()
                # ... existing error handling ...
            except MaxRetriesExceededError as ex:
                # ... existing handling ...
            finally:
                # Always cleanup task session
                self.wrap_up_task_session(db_session)
    
    def wrap_up_task_session(self, db_session: Session):
        """Clean up task-specific session.
        
        This replaces wrap_up_dbsession() for task sessions.
        Always rolls back on exception, commits on success.
        """
        try:
            # Check if we're in a transaction
            if db_session.in_transaction():
                # If there was an exception, rollback
                # If successful, commit
                # We can't easily detect exception state here, so be safe
                # The run() method handles commits for success cases
                db_session.rollback()
        except Exception as e:
            log.warning(
                "Error during task session cleanup",
                extra={"error": str(e), "task": self.name},
                exc_info=True
            )
        finally:
            try:
                db_session.close()
            except Exception as e:
                log.warning(
                    "Error closing task session",
                    extra={"error": str(e), "task": self.name},
                    exc_info=True
                )
    
    # Keep old method for backward compatibility (routing, etc.)
    def wrap_up_dbsession(self, db_session):
        """Legacy method for shared sessions. Use wrap_up_task_session for tasks."""
        # ... existing implementation ...
```

#### Step 3: Update Task Implementations

**Before:**
```python
def run_impl(self, db_session, *args, **kwargs):
    # Work with db_session
    commit = db_session.query(Commit).filter_by(...).first()
    # ... do work ...
    db_session.commit()  # Explicit commit
    return result
```

**After:**
```python
def run_impl(self, db_session, *args, **kwargs):
    # Work with db_session (same API)
    commit = db_session.query(Commit).filter_by(...).first()
    # ... do work ...
    db_session.commit()  # Explicit commit (now per-task, safe)
    return result
```

**Note**: Task implementations don't need to change! The session API is the same.

#### Step 4: Handle Celery Routing (Special Case)

**Problem**: `celery_task_router.py` uses `get_db_session()` for routing, which happens BEFORE task execution.

**Solution**: Keep shared session for routing, but make it read-only and short-lived.

```python
# celery_task_router.py

def route_task(name, args, kwargs, options, task=None, **kw):
    """Function to dynamically route tasks to the proper queue."""
    user_plan = options.get("user_plan")
    ownerid = options.get("ownerid")
    
    if user_plan is None or ownerid is None:
        # Use shared session for routing (short-lived, read-only)
        db_session = get_db_session()
        try:
            if user_plan is None:
                user_plan = _get_user_plan_from_task(db_session, name, kwargs)
            if ownerid is None:
                ownerid = _get_ownerid_from_task(db_session, name, kwargs)
        finally:
            # Routing is read-only, but ensure no transaction left open
            if db_session.in_transaction():
                db_session.rollback()
            # Don't close - it's a shared scoped_session
    
    return route_tasks_based_on_user_plan(name, user_plan, ownerid)
```

**Alternative**: Use a separate routing session:

```python
# celery_task_router.py

from database.engine import create_task_session

def route_task(name, args, kwargs, options, task=None, **kw):
    """Function to dynamically route tasks to the proper queue."""
    user_plan = options.get("user_plan")
    ownerid = options.get("ownerid")
    
    if user_plan is None or ownerid is None:
        # Create temporary session just for routing
        routing_session = create_task_session()
        try:
            if user_plan is None:
                user_plan = _get_user_plan_from_task(routing_session, name, kwargs)
            if ownerid is None:
                ownerid = _get_ownerid_from_task(routing_session, name, kwargs)
        finally:
            # Always cleanup routing session
            routing_session.rollback()
            routing_session.close()
    
    return route_tasks_based_on_user_plan(name, user_plan, ownerid)
```

### Complete Example: Modified `tasks/base.py`

```python
import logging
from contextlib import contextmanager
from celery._state import get_current_task
from celery.exceptions import MaxRetriesExceededError, SoftTimeLimitExceeded
from sqlalchemy.exc import DataError, IntegrityError, InvalidRequestError, SQLAlchemyError
from sqlalchemy.orm import Session

from database.engine import create_task_session
from helpers.log_context import LogContext, set_log_context

log = logging.getLogger("worker")


class BaseCodecovTask(celery_app.Task):
    @sentry_sdk.trace
    def run(self, *args, **kwargs):
        with self.task_full_runtime.time():
            # Create NEW session for this task
            db_session = create_task_session()
            
            # Validate session is clean
            if db_session.in_transaction():
                log.warning(
                    "New task session has open transaction, rolling back",
                    extra={
                        "task": self.name,
                        "task_id": getattr(self.request, "id", None)
                    }
                )
                db_session.rollback()
            
            # Set up log context
            log_context = LogContext(
                repo_id=kwargs.get("repoid") or kwargs.get("repo_id"),
                owner_id=kwargs.get("ownerid"),
                commit_sha=kwargs.get("commitid") or kwargs.get("commit_id"),
            )
            
            task = get_current_task()
            if task and task.request:
                log_context.task_name = task.name
                task_id = getattr(task.request, "id", None)
                if task_id:
                    log_context.task_id = task_id
            
            log_context.populate_from_sqlalchemy(db_session)
            set_log_context(log_context)
            
            # ... existing metrics and timing code ...
            
            try:
                with self.task_core_runtime.time():
                    result = self.run_impl(db_session, *args, **kwargs)
                    # If we got here, task succeeded - commit
                    if db_session.in_transaction():
                        db_session.commit()
                    return result
            except InterfaceError as ex:
                sentry_sdk.capture_exception(ex)
                db_session.rollback()
            except (DataError, IntegrityError):
                log.exception("Database constraint errors", ...)
                db_session.rollback()
                # ... retry logic ...
            except SQLAlchemyError as ex:
                self._analyse_error(ex, args, kwargs)
                db_session.rollback()
                # ... retry logic ...
            except MaxRetriesExceededError as ex:
                # ... existing handling ...
            except Exception:
                # Catch-all: rollback on any exception
                db_session.rollback()
                raise
            finally:
                # Always cleanup task session
                self.wrap_up_task_session(db_session)
    
    def wrap_up_task_session(self, db_session: Session):
        """Clean up task-specific session.
        
        Ensures session is properly closed and any remaining transaction is rolled back.
        """
        try:
            # Rollback any remaining transaction
            if db_session.in_transaction():
                db_session.rollback()
        except Exception as e:
            log.warning(
                "Error rolling back task session",
                extra={"error": str(e), "task": self.name},
                exc_info=True
            )
        finally:
            try:
                db_session.close()
            except Exception as e:
                log.warning(
                    "Error closing task session",
                    extra={"error": str(e), "task": self.name},
                    exc_info=True
                )
```

## Does Celery Itself Need DB Connection?

**Answer: Yes, but only for routing, not for task execution.**

**Celery Routing (`celery_task_router.py`):**
- Called by Celery **before** task execution
- Needs DB to look up user plan and ownerid
- Happens at worker process level
- **Solution**: Use shared session for routing (read-only, short-lived)

**Task Execution:**
- Celery doesn't need DB connection
- Tasks use their own per-task sessions
- No shared state

## Migration Strategy

### Phase 1: Add Per-Task Session Support (Backward Compatible)

1. Add `create_task_session()` function
2. Add `wrap_up_task_session()` method
3. Keep existing `get_db_session()` and `wrap_up_dbsession()` for routing
4. No breaking changes

### Phase 2: Migrate Tasks Incrementally

1. Update `BaseCodecovTask.run()` to use per-task sessions
2. Test each task type
3. Monitor for issues

### Phase 3: Clean Up

1. Remove shared session usage from tasks
2. Keep shared session only for routing
3. Document the pattern

## Benefits

✅ **Eliminates Transaction Contamination**: Each task gets isolated session
✅ **Automatic Cleanup**: Session closed in `finally` block
✅ **Backward Compatible**: Can migrate incrementally
✅ **Same API**: Task implementations don't need changes
✅ **Connection Efficient**: Still uses shared connection pool
✅ **Handles Routing**: Celery routing still works

## Trade-offs

⚠️ **Slight Overhead**: Session creation per task (minimal)
⚠️ **Migration Effort**: Need to update base task class
⚠️ **Testing Required**: Ensure all tasks work correctly

## Connection Pool Impact

**With Per-Task Sessions + Transaction Pooling:**
- **Client Connections**: Still 1 per worker process
- **Server Connections**: Shared pool (based on concurrent transactions)
- **Sessions**: 1 per task (but share server connections)

**Connection Requirements:**
- `default_pool_size >= num_workers` (for sequential tasks)
- No increase needed if using transaction pooling
- Sessions are lightweight, connections are pooled

## Example: Before and After

**Before (Shared Session):**
```python
# Task A starts transaction
db_session.query(...)  # Transaction starts
# Task A times out
# Transaction left open

# Task B reuses same session
db_session.query(...)  # Uses Task A's transaction ❌
```

**After (Per-Task Session):**
```python
# Task A gets new session
db_session_a = create_task_session()
db_session_a.query(...)  # Transaction starts
# Task A times out
# Session closed, transaction rolled back ✅

# Task B gets NEW session
db_session_b = create_task_session()  # Clean session ✅
db_session_b.query(...)  # New transaction
```

