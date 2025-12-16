# Idle in Transaction Timeout Cases

This document identifies cases where PostgreSQL's `idle_in_transaction_session_timeout` could be hit in the worker app.

## Overview

SQLAlchemy sessions start transactions implicitly when first used (autocommit=False). Transactions remain open until explicitly committed via `db_session.commit()`. If long-running operations occur between transaction start and commit, the idle timeout can be triggered.

**Important Note on Timeout Value**: If PostgreSQL's `idle_in_transaction_session_timeout` is set to 1300000 (milliseconds = ~21 minutes, or seconds = ~15 days), this is likely sufficient even with task retries. However, the **real issue** is that when a task times out with an open transaction:
1. The Redis lock expires (based on `lock_timeout`), allowing retries to acquire it
2. **BUT** the database transaction remains open, potentially holding row-level locks or blocking concurrent operations
3. Retries may fail or block waiting for the original transaction to commit/rollback, even though they can acquire the Redis lock
4. This creates a deadlock-like situation where retries are blocked by database locks from the original timed-out transaction

**Important Note on Locks**: The codebase uses **Redis locks** (via `LockManager`) for distributed locking, not database locks. These Redis locks don't directly hold database transactions, but the database transaction remains open while holding the Redis lock and performing work inside the lock context. Most tasks use `blocking_timeout=None`, meaning they don't block waiting for locks, but the transaction is still idle during long-running operations inside the lock.

**Important Note on Task Timeouts**: Tasks have both **soft** and **hard** time limits:
- **Soft Time Limit**: Raises `SoftTimeLimitExceeded` which can be caught and handled. The `wrap_up_dbsession()` method handles this case, but if a soft timeout occurs during long-running operations before commit, the transaction remains open.
- **Hard Time Limit**: Forcefully kills the task process. If this happens during long-running operations, the `finally` block (which calls `wrap_up_dbsession()`) may not execute, potentially leaving transactions open until PostgreSQL's `idle_in_transaction_session_timeout` kills them.

**Critical Note on Connection Architecture and PgBouncer**: 

**Connection Architecture:**
- **Engine/Connection Pool**: Created at **worker process level** (when `database/engine.py` module is imported)
  - `create_engine()` creates a connection pool with `pool_pre_ping=True`
  - Pool is shared across all sessions using that engine
- **Session**: Created via `scoped_session(sessionmaker())` which uses **thread-local storage**
  - Each thread gets its own session instance
  - But sessions share the underlying connection pool
- **Actual Database Connection**: **Lazy** - established when first query/operation happens, pulled from the pool
- **Celery Worker Model**: Uses **prefork pool** (process-based, not thread-based) based on `prefork_gc_freeze` signal
  - Each worker process runs tasks sequentially in its main thread
  - Each process has its own engine/connection pool
  - Effectively: **one session per worker process** (since one main thread per process)

**If a task dies with an open transaction:**
- The session/connection remains in the worker process
- The next task in the same worker process reuses the session
- The next task inherits the open transaction, causing unpredictable behavior
- This is **worse than idle timeout** - it affects other tasks immediately

**PgBouncer Pooling Modes and Task Termination:**

**Critical Distinction:**
- **Hard Timeout (Process Killed)**: Celery kills the worker process → TCP connection closes → PgBouncer can detect and rollback
- **Soft Timeout (Process Stays Alive)**: Task raises exception but process continues → TCP connection stays open → **PgBouncer cannot detect** → Transaction remains open

**PgBouncer Pooling Modes:**
- **Session Pooling**: 
  - Connection stays with client for entire session
  - If worker process dies (hard timeout), TCP closes → PostgreSQL detects disconnection → transaction rolled back
  - If only task dies but worker stays alive (soft timeout), TCP stays open → **transaction remains open** → **Problematic**
- **Transaction Pooling**: 
  - Connection only for transaction duration
  - If worker process dies (hard timeout), TCP closes → PgBouncer can detect and rollback
  - If only task dies but worker stays alive (soft timeout), TCP stays open → **PgBouncer cannot detect** → **transaction remains open** → **Still problematic**
- **Statement Pooling**: Autocommit mode, no transactions. Not suitable for this codebase.

**Key Insight**: **PgBouncer cannot help if only the task dies but the worker process stays alive** because:
- The TCP connection to PgBouncer remains open (it's at the process level)
- PgBouncer only sees that the connection is still alive
- It has no way to know that the task execution died
- The transaction remains open until PostgreSQL's `idle_in_transaction_session_timeout` or until the next task inherits it

**Recommendation**: 
- Use **transaction pooling** mode in PgBouncer (helps with hard timeouts where process dies)
- **But**: Still need application-level transaction rollback for soft timeouts
- **Critical**: Ensure `wrap_up_dbsession()` is called even on soft timeouts, or add transaction rollback in exception handlers

## Critical Cases

### 1. **UploadFinisherTask._process_reports_with_lock()**

**Location:** `apps/worker/tasks/upload_finisher.py:398-456`

**Transaction Scope:** Transaction starts when `db_session` is first used (line 409), commits at line 448.

**Long-running operations BEFORE commit:**
- **Line 409:** `load_commit_diff(commit, self.name)` - **EXTERNAL API CALL** to git provider
  - Calls `repository_service.get_commit_diff()` which can take seconds/minutes
  - Network latency, rate limiting, or slow provider responses can cause delays
- **Line 423-427:** `lock_manager.locked()` - **REDIS LOCK CONTEXT** 
  - Acquires Redis lock (with `blocking_timeout=None`, so doesn't block waiting)
  - Database transaction remains open while holding Redis lock and doing work inside lock context
- **Line 432-434:** `perform_report_merging()` - **CPU/IO INTENSIVE**
  - Loads intermediate reports from Redis/storage
  - Merges potentially large reports (can be slow for big codebases)
- **Line 446:** `report_service.save_report()` - **STORAGE WRITE**
  - Writes report data to storage (GCS/S3)
  - Network latency and large file sizes can cause delays

**Risk Level:** ⚠️ **HIGH** - Multiple external operations before commit

---

### 2. **PreProcessUpload.process_impl_within_lock()**

**Location:** `apps/worker/tasks/preprocess_upload.py:96-138`

**Transaction Scope:** Transaction starts at line 98 (query), commits at line 128.

**Long-running operations BEFORE commit:**
- **Line 119:** `possibly_update_commit_from_provider_info()` - **EXTERNAL API CALL**
  - Calls `repository_service.get_commit()` to fetch commit details
  - Can be slow if provider is slow or rate-limited
- **Line 122:** `fetch_commit_yaml_and_possibly_store()` - **EXTERNAL API CALL**
  - Calls `fetch_commit_yaml_from_provider()` 
  - Fetches `.codecov.yml` from git provider
  - Network latency and provider response time can cause delays

**Risk Level:** ⚠️ **MEDIUM-HIGH** - Two sequential external API calls

---

### 3. **BundleAnalysisProcessorTask.process_impl_within_lock()**

**Location:** `apps/worker/tasks/bundle_analysis_processor.py:108-305`

**Transaction Scope:** Transaction starts at line 127 (query), commits at lines 224, 241, or 264.

**Long-running operations BEFORE commit:**
- **Line 207:** `report_service.process_upload()` - **PROCESSING INTENSIVE**
  - Processes bundle analysis data
  - May read from storage, process large files
  - Can be slow for large bundles
- **Line 236-238:** `self.retry()` - **RETRY DELAY**
  - If retryable error occurs, task retries with exponential backoff
  - Transaction remains open during retry countdown

**Risk Level:** ⚠️ **MEDIUM** - Processing operations can be slow

---

### 4. **process_upload()**

**Location:** `apps/worker/services/processing/processing.py:26-110`

**Transaction Scope:** Transaction starts when `db_session` is first used, commits at line 56 or 101.

**Long-running operations BEFORE commit:**
- **Line 59-60:** `report_service.build_report_from_raw_content()` - **PROCESSING INTENSIVE**
  - Processes raw report content
  - Can be slow for large reports
- **Line 71:** `save_intermediate_report()` - **STORAGE WRITE**
  - Writes intermediate report to storage
- **Line 95:** `rewrite_or_delete_upload()` - **STORAGE OPERATION**
  - May delete or rewrite files in storage
  - Network latency can cause delays

**Risk Level:** ⚠️ **MEDIUM** - Storage operations can be slow

---

### 5. **fetch_and_update_pull_request_information()**

**Location:** `apps/worker/services/repository.py:527-586`

**Transaction Scope:** Transaction starts at line 535 (query), commits at line 584.

**Long-running operations BEFORE commit:**
- **Line 541:** `repository_service.get_pull_request()` - **EXTERNAL API CALL**
  - Fetches PR information from git provider
  - Can be slow due to network latency or rate limiting
- **Line 568:** `_pick_best_base_comparedto_pair()` - **EXTERNAL API CALL**
  - May call `repository_service.get_compare()` 
  - Fetches comparison data from provider

**Risk Level:** ⚠️ **MEDIUM-HIGH** - External API calls before commit

---

### 6. **HTTPRequestTask.run_impl()**

**Location:** `apps/worker/tasks/http_request.py:18-65`

**Transaction Scope:** Transaction starts when `db_session` is first used (though not actively used), commits in `wrap_up_dbsession()`.

**Long-running operations BEFORE commit:**
- **Line 43-44:** `client.request()` - **EXTERNAL HTTP CALL**
  - Makes HTTP request with configurable timeout (default 10s)
  - Can be slow if external service is slow
  - Transaction remains open during entire HTTP request

**Risk Level:** ⚠️ **MEDIUM** - External HTTP calls

---

### 7. **ReportService.save_report()**

**Location:** `apps/worker/services/report/__init__.py:707+`

**Transaction Scope:** Called within an open transaction, writes to storage.

**Long-running operations:**
- Writes report chunks to storage (GCS/S3)
- Large reports can take significant time to upload
- Network latency and storage service response time

**Risk Level:** ⚠️ **MEDIUM** - Storage writes can be slow

---

## Timeout-Related Cases

### 8. **Task Timeout During Long-Running Operations**

**Location:** Any task with long-running operations before commit

**Scenario:** When a task hits its soft or hard timeout during long-running operations (external API calls, storage operations, CPU-intensive processing), the database transaction remains open.

**Soft Timeout (`SoftTimeLimitExceeded`):**
- Can be caught and handled
- `wrap_up_dbsession()` handles soft timeouts during commit
- **BUT**: If soft timeout occurs during long-running operations BEFORE commit, transaction remains open
- Example: `UploadFinisherTask` catches `SoftTimeLimitExceeded` at line 364, but transaction is already open from line 409

**Hard Timeout:**
- Forcefully kills the **worker process** (Celery prefork pool)
- The `finally` block (line 448-449 in `base.py`) does not execute
- `wrap_up_dbsession()` is never called
- **TCP connection to PgBouncer closes** (process died)
- **PgBouncer can detect** dead connection and rollback (if using transaction pooling)
- **But**: If using session pooling, transaction may remain open until PostgreSQL detects disconnection
- **Risk**: Depends on PgBouncer pooling mode

**Soft Timeout:**
- Raises `SoftTimeLimitExceeded` exception but **worker process stays alive**
- The `finally` block executes, calling `wrap_up_dbsession()`
- **BUT**: If soft timeout occurs during long-running operations BEFORE commit, transaction is already open
- **TCP connection to PgBouncer stays open** (process alive)
- **PgBouncer cannot detect** task failure - connection appears healthy
- **Transaction remains open** until next task inherits it or PostgreSQL timeout
- **Most Dangerous for Shared Sessions**: Next task inherits open transaction

**Risk Level:** ⚠️ **HIGH** - Especially for hard timeouts where cleanup cannot occur

**Affected Tasks:**
- All tasks with long-running operations before commit
- Tasks with hard time limits longer than PostgreSQL `idle_in_transaction_session_timeout`
- Tasks that perform external API calls or storage operations before commit

---

### 9. **Retry Blocking Due to Open Transactions**

**Location:** Any task that times out with an open transaction

**Scenario:** Even if PostgreSQL's `idle_in_transaction_session_timeout` is set high enough (e.g., 1300000ms = ~21 minutes), the real problem is that retries fail due to locks held by the original transaction.

**How It Happens:**
1. Task A starts a transaction (implicitly when `db_session` is first used)
2. Task A acquires a Redis lock
3. Task A performs long-running operations (external API, storage, processing)
4. Task A times out (soft or hard) before committing
5. Redis lock expires (based on `lock_timeout`), allowing retry to acquire it
6. **BUT**: Database transaction remains open, holding locks
7. Task A retries (or new task processes same work)
8. Retry acquires Redis lock successfully
9. **Retry fails/blocks** when trying to access/modify rows locked by the original transaction

**Why This Is Worse Than Idle Timeout:**
- The idle timeout might never be hit (if timeout is 1300000ms)
- But retries are blocked immediately by database locks
- Creates a deadlock-like situation where work cannot proceed
- Redis lock is available, but database locks prevent progress

**Tasks Most Affected:**
- Tasks using `SELECT FOR UPDATE`:
  - `MarkOwnerForDeletionTask` (line 59: `Owner.objects.select_for_update()`)
  - `DetectFlakes` (line 165: `TAUpload.objects.select_for_update()`)
  - `start_repo_cleanup` (line 74: `Repository.objects.select_for_update(nowait=True)`)
- Tasks that modify data before committing:
  - Any task that writes to database before `db_session.commit()`
  - Retries may see inconsistent state or be blocked by row locks

**Risk Level:** ⚠️ **CRITICAL** - Prevents retries from succeeding, even though Redis locks are available

---

### 10. **Shared Session Contamination (Worker Process-Level Connections)**

**Location:** All tasks using `get_db_session()` (which is a `scoped_session`)

**Connection Architecture:**
- **Engine/Connection Pool**: Created at worker process level (module import time)
- **Session**: `scoped_session` uses thread-local storage, but Celery uses prefork (process-based)
- **Result**: Effectively one session per worker process (one main thread per process)
- **Connections**: Pooled at worker process level, lazy-loaded on first query

**Scenario:** Since sessions are effectively shared at the worker process level, if a task dies with an open transaction, the next task in the same worker process inherits that transaction.

**How It Happens:**
1. Task A in Worker Process 1 starts a transaction via `get_db_session()`
2. Task A performs long-running operations
3. Task A dies (hard timeout, OOM kill, crash) before committing
4. Transaction remains open in the shared session
5. Task B in the same Worker Process 1 starts executing
6. Task B calls `get_db_session()` - **gets the same session with open transaction**
7. Task B's operations are now part of Task A's transaction
8. Task B commits - **commits both Task A's and Task B's changes together**
9. Or Task B fails due to inconsistent state from Task A's partial transaction

**Why This Is Critical:**
- **Not just about idle timeout** - affects other tasks immediately
- **Data integrity risk** - tasks may commit changes from previous failed tasks
- **Unpredictable behavior** - tasks may see inconsistent state
- **Hard to debug** - failures appear unrelated to the original task

**PgBouncer Considerations:**
- **Session Pooling Mode**: 
  - Connection stays with client for entire session
  - If client dies, transaction stays open until PostgreSQL detects disconnection
  - **Problematic** - open transactions persist across task boundaries
- **Transaction Pooling Mode**:
  - Connection only for transaction duration
  - PgBouncer can detect dead clients and rollback transactions
  - **Better** - but still risky if task dies mid-transaction before PgBouncer detects it
- **Statement Pooling Mode**:
  - Autocommit mode, no transactions
  - Not suitable for this codebase (requires transactions)

**Mitigation Strategies:**
1. **Use Transaction Pooling** in PgBouncer (recommended)
2. **Add transaction rollback on task failure** - ensure transactions are rolled back in `on_failure` handler
3. **Use per-task sessions** instead of shared `scoped_session` (major refactor)
4. **Add transaction state checks** - verify no open transaction before starting new work
5. **Monitor for transaction contamination** - detect when tasks inherit open transactions

**Risk Level:** ⚠️ **CRITICAL** - Can cause data corruption and unpredictable behavior across tasks

---

### 11. **PgBouncer Limitation: Cannot Detect Task-Level Failures**

**Location:** All tasks with open transactions when soft timeout occurs

**Critical Limitation**: PgBouncer **cannot detect** when only a task dies but the worker process stays alive.

**How PgBouncer Works:**
- PgBouncer operates at the **TCP connection level**, not the task/application level
- It can detect when a **TCP connection closes** (worker process dies)
- It **cannot detect** when a task fails but the TCP connection remains open (worker process alive)

**Scenarios:**

**Scenario A: Hard Timeout (Process Killed)**
1. Task hits hard timeout
2. Celery kills the worker process
3. TCP connection to PgBouncer closes
4. **PgBouncer detects** dead connection
5. **Transaction Pooling**: PgBouncer can rollback the transaction ✅
6. **Session Pooling**: Transaction may remain open until PostgreSQL detects disconnection ⚠️

**Scenario B: Soft Timeout (Process Stays Alive)**
1. Task hits soft timeout, raises `SoftTimeLimitExceeded`
2. Worker process **stays alive**
3. TCP connection to PgBouncer **remains open**
4. **PgBouncer sees healthy connection** - no way to know task failed
5. Transaction remains open in the session
6. **Next task inherits open transaction** ❌
7. **Neither Transaction nor Session Pooling can help** - connection appears healthy

**Why This Matters:**
- **Soft timeouts are more common** than hard timeouts
- **Soft timeouts are more dangerous** for shared sessions (next task inherits transaction)
- **PgBouncer provides no protection** for soft timeouts
- **Application-level transaction rollback is required**

**Mitigation:**
1. **Application-level rollback**: Ensure transactions are rolled back in exception handlers
2. **Transaction state validation**: Check for open transactions at task start and rollback if found
3. **Commit before long-running operations**: Prevents transactions from staying open during external calls
4. **Monitor for transaction contamination**: Detect when tasks inherit open transactions

**Risk Level:** ⚠️ **CRITICAL** - PgBouncer cannot protect against the most common failure scenario (soft timeouts)

---

### 12. **Celery Worker Query Tracking and Transaction Detection Limitations**

**Location:** All tasks using database connections

**Question 1: How does Celery worker know which queries go to which task?**

**Answer: It doesn't track this at the database level.**

**How It Actually Works:**
- **No Database-Level Tracking**: Celery workers do not tag or associate database queries with specific tasks at the database connection level
- **Shared Session**: All tasks in a worker process share the same `scoped_session` instance
- **Application-Level Tracking Only**: Task identification exists only in application code:
  - `LogContext` tracks `task_id` and `task_name` (line 385-390 in `base.py`)
  - This is used for logging and metrics, not database query tracking
  - PostgreSQL has no knowledge of which queries belong to which task
- **Connection Pool**: Queries are executed through a shared connection pool
- **No Query Tagging**: There's no mechanism to tag queries with task IDs or track query-to-task associations

**Implication:**
- If a transaction is left open, there's no way to identify which task started it by querying the database
- PostgreSQL `pg_stat_activity` shows the connection and query, but not the Celery task that initiated it
- Debugging hung transactions requires application-level logging, not database queries

**Question 2: Can Celery detect hung transactions due to killed tasks?**

**Answer: No, Celery has no built-in mechanism to detect hung transactions.**

**Detection Mechanisms:**

**What Celery CAN Detect:**
- **Task completion/failure**: Celery tracks task state (PENDING, STARTED, SUCCESS, FAILURE, RETRY)
- **Process death**: If worker process dies, Celery knows the task failed
- **Timeouts**: Celery enforces soft/hard time limits

**What Celery CANNOT Detect:**
- **Open transactions**: No monitoring of database transaction state
- **Hung transactions**: No mechanism to detect transactions left open by killed tasks
- **Transaction contamination**: No detection when next task inherits open transaction
- **Database connection state**: No awareness of transaction boundaries

**Why This Is a Problem:**
1. **Hard Timeout**: Process dies → Connection closes → No way for Celery to know transaction was left open
2. **Soft Timeout**: Process stays alive → Transaction remains open → Celery has no visibility into this
3. **No Cleanup**: Even if Celery knew, there's no automatic rollback mechanism
4. **Silent Failure**: Transaction contamination happens silently - next task just inherits it

**Current Mitigation (Limited):**
- `wrap_up_dbsession()` in `finally` block attempts cleanup
- But if `finally` doesn't execute (hard timeout), no cleanup occurs
- No validation that transaction was actually closed
- No detection of inherited transactions

**What Would Be Needed:**
1. **Transaction State Monitoring**: Check for open transactions at task start
2. **Automatic Rollback**: Rollback any inherited open transactions before starting new work
3. **Transaction Tagging**: Tag transactions with task IDs (would require PostgreSQL extensions or application-level tracking)
4. **Health Checks**: Periodic checks for hung transactions in worker processes

**Risk Level:** ⚠️ **CRITICAL** - No visibility or detection of hung transactions, leading to silent failures

---

## Better Technologies and Solutions

### 1. **Per-Task Sessions (Recommended)**

**Problem**: Shared `scoped_session` causes transaction contamination

**Solution**: Create a new session for each task instead of reusing a shared session

**Implementation:**
```python
# Instead of shared scoped_session:
def run(self, *args, **kwargs):
    db_session = get_db_session()  # Shared session
    
# Use per-task session:
def run(self, *args, **kwargs):
    # Create new session for this task
    db_session = sessionmaker(bind=engine)()
    try:
        return self.run_impl(db_session, *args, **kwargs)
    finally:
        db_session.rollback()  # Always rollback on exception
        db_session.close()
```

**Benefits:**
- ✅ Each task gets isolated session
- ✅ No transaction contamination between tasks
- ✅ Automatic cleanup when task ends
- ✅ Simpler to reason about

**Trade-offs:**
- ⚠️ Slightly more overhead (session creation per task)
- ⚠️ Requires refactoring existing code

**Risk Reduction:** ⭐⭐⭐⭐⭐ (Eliminates transaction contamination)

---

### 2. **PostgreSQL `application_name` Tagging**

**Problem**: No way to identify which task/queries belong to which task in PostgreSQL

**Solution**: Set `application_name` connection parameter with task ID

**Implementation:**
```python
from sqlalchemy import event
from celery._state import get_current_task

def set_application_name(connection, branch):
    task = get_current_task()
    if task and task.request:
        task_id = getattr(task.request, "id", None)
        if task_id:
            connection.execute(f"SET application_name = 'celery_task_{task_id}'")

# In engine creation:
engine = create_engine(
    database_url,
    connect_args={"application_name": "codecov_worker"},
    pool_pre_ping=True,
)

# Add event listener:
event.listen(engine, "connect", set_application_name)
```

**Benefits:**
- ✅ Queries visible in `pg_stat_activity` with task ID
- ✅ Can identify hung transactions by task
- ✅ Better observability and debugging
- ✅ Minimal code changes

**Trade-offs:**
- ⚠️ Doesn't prevent hung transactions, only helps identify them
- ⚠️ Requires PostgreSQL 9.0+

**Risk Reduction:** ⭐⭐⭐ (Improves observability, doesn't prevent issues)

---

### 3. **Transaction Context Managers with Automatic Rollback**

**Problem**: Transactions left open when exceptions occur

**Solution**: Use context managers that guarantee rollback on exception

**Implementation:**
```python
from contextlib import contextmanager

@contextmanager
def transaction_context(db_session):
    """Context manager that ensures transaction is closed."""
    try:
        yield db_session
        db_session.commit()
    except Exception:
        db_session.rollback()
        raise
    finally:
        # Verify transaction is closed
        if db_session.in_transaction():
            db_session.rollback()

# Usage:
def run_impl(self, db_session, *args, **kwargs):
    with transaction_context(db_session):
        # All work here
        # Automatic rollback on exception
        pass
```

**Benefits:**
- ✅ Guaranteed transaction cleanup
- ✅ Works with existing code structure
- ✅ Prevents transaction contamination
- ✅ Clear transaction boundaries

**Trade-offs:**
- ⚠️ Requires wrapping all transaction code
- ⚠️ May need to refactor existing code

**Risk Reduction:** ⭐⭐⭐⭐ (Prevents most transaction leaks)

---

### 4. **PostgreSQL Statement and Lock Timeouts**

**Problem**: Long-running queries and locks block other operations

**Solution**: Set PostgreSQL timeouts at connection level

**Implementation:**
```python
# In engine creation:
engine = create_engine(
    database_url,
    connect_args={
        "options": "-c statement_timeout=300000 -c lock_timeout=10000"
    },
    pool_pre_ping=True,
)

# Or per-task:
def run(self, *args, **kwargs):
    db_session = get_db_session()
    # Set timeouts for this task
    db_session.execute("SET statement_timeout = '5min'")
    db_session.execute("SET lock_timeout = '10s'")
    try:
        return self.run_impl(db_session, *args, **kwargs)
    finally:
        db_session.rollback()
        db_session.close()
```

**Benefits:**
- ✅ Prevents queries from running indefinitely
- ✅ Prevents locks from blocking indefinitely
- ✅ Helps with retry blocking issues
- ✅ PostgreSQL-native solution

**Trade-offs:**
- ⚠️ May need to tune timeout values
- ⚠️ Legitimate long-running queries may fail

**Risk Reduction:** ⭐⭐⭐⭐ (Prevents long-running operations from blocking)

---

### 5. **Connection-Level Transaction State Validation**

**Problem**: No detection of inherited open transactions

**Solution**: Check transaction state at task start and rollback if found

**Implementation:**
```python
def run(self, *args, **kwargs):
    db_session = get_db_session()
    
    # Check for open transaction from previous task
    if db_session.in_transaction():
        log.warning(
            "Found open transaction at task start, rolling back",
            extra={"task": self.name, "task_id": self.request.id}
        )
        db_session.rollback()
        # Optionally: close and recreate session
        db_session.close()
        db_session = sessionmaker(bind=engine)()
    
    try:
        return self.run_impl(db_session, *args, **kwargs)
    finally:
        if db_session.in_transaction():
            db_session.rollback()
        db_session.close()
```

**Benefits:**
- ✅ Detects transaction contamination
- ✅ Prevents inheritance of open transactions
- ✅ Can be added incrementally
- ✅ Provides visibility into the problem

**Trade-offs:**
- ⚠️ Doesn't prevent the root cause
- ⚠️ May mask underlying issues
- ⚠️ Adds overhead to every task

**Risk Reduction:** ⭐⭐⭐ (Detects and mitigates, doesn't prevent)

---

### 6. **APM and Database Monitoring Tools**

**Problem**: No visibility into transaction state and query tracking

**Solutions:**
- **Datadog APM**: Tracks database queries with task context
- **New Relic**: Database transaction monitoring
- **pg_stat_statements**: PostgreSQL extension for query tracking
- **Custom metrics**: Track transaction duration and state

**Benefits:**
- ✅ Better observability
- ✅ Can identify problematic patterns
- ✅ Historical analysis
- ✅ Alerting on anomalies

**Trade-offs:**
- ⚠️ Doesn't prevent issues, only monitors
- ⚠️ May have performance overhead
- ⚠️ Requires setup and maintenance

**Risk Reduction:** ⭐⭐⭐ (Improves visibility, enables proactive fixes)

---

### 7. **Alternative Architecture: Per-Task Connection Isolation**

**Problem**: Shared connections cause transaction contamination

**Solution**: Use connection pool with isolation per task

**Implementation:**
```python
# Use connection pool with checkout/checkin pattern
from sqlalchemy.pool import QueuePool

class TaskConnectionManager:
    def __init__(self, engine):
        self.engine = engine
    
    def get_connection(self):
        """Get a connection for this task, ensure it's clean."""
        conn = self.engine.connect()
        # Verify no open transaction
        if conn.in_transaction():
            conn.rollback()
        return conn
    
    def return_connection(self, conn):
        """Return connection to pool, ensuring it's clean."""
        if conn.in_transaction():
            conn.rollback()
        conn.close()

# Usage:
def run(self, *args, **kwargs):
    conn = connection_manager.get_connection()
    db_session = Session(bind=conn)
    try:
        return self.run_impl(db_session, *args, **kwargs)
    finally:
        db_session.rollback()
        db_session.close()
        connection_manager.return_connection(conn)
```

**Benefits:**
- ✅ Complete isolation between tasks
- ✅ Guaranteed clean state
- ✅ No transaction contamination
- ✅ Better error handling

**Trade-offs:**
- ⚠️ More complex connection management
- ⚠️ Requires significant refactoring
- ⚠️ May impact performance

**Risk Reduction:** ⭐⭐⭐⭐⭐ (Complete isolation, eliminates contamination)

---

### 8. **PostgreSQL Advisory Locks for Transaction Tracking**

**Problem**: No way to track which task owns a transaction

**Solution**: Use PostgreSQL advisory locks to mark transaction ownership

**Implementation:**
```python
import hashlib

def get_task_lock_id(task_id):
    """Convert task ID to PostgreSQL integer for advisory lock."""
    return int(hashlib.md5(task_id.encode()).hexdigest()[:8], 16)

def run(self, *args, **kwargs):
    db_session = get_db_session()
    task_id = self.request.id
    lock_id = get_task_lock_id(task_id)
    
    # Acquire advisory lock for this task
    db_session.execute(f"SELECT pg_advisory_lock({lock_id})")
    
    try:
        return self.run_impl(db_session, *args, **kwargs)
    finally:
        # Release lock (also releases on connection close)
        try:
            db_session.execute(f"SELECT pg_advisory_unlock({lock_id})")
        except:
            pass
        db_session.rollback()
        db_session.close()
```

**Benefits:**
- ✅ Can identify transaction ownership
- ✅ Automatic release on connection close
- ✅ Can detect orphaned transactions
- ✅ PostgreSQL-native solution

**Trade-offs:**
- ⚠️ Adds complexity
- ⚠️ Doesn't prevent contamination, only tracks it
- ⚠️ Limited number of locks (2^63)

**Risk Reduction:** ⭐⭐⭐ (Improves tracking, doesn't prevent)

---

## Recommended Approach

**Best Solution**: **Combine multiple approaches**

1. **Immediate (Low Risk)**:
   - Add `application_name` tagging for observability
   - Add transaction state validation at task start
   - Set PostgreSQL statement/lock timeouts

2. **Short-term (Medium Risk)**:
   - Implement transaction context managers
   - Add automatic rollback in exception handlers
   - Improve monitoring and alerting

3. **Long-term (Higher Risk, Best Solution)**:
   - Migrate to per-task sessions
   - Or implement per-task connection isolation
   - Complete transaction boundary enforcement

**Priority**: Start with #1 (immediate), plan for #3 (long-term) as it provides the best protection.

---

## Complete Code Solution: Per-Task Sessions

See `PER_TASK_SESSION_SOLUTION.md` for a complete implementation guide.

### Quick Summary

**Does Celery itself need DB connection?**

**Answer: Yes, but only for routing, not for task execution.**

- **Routing** (`celery_task_router.py`): Called BEFORE task execution to determine queue
  - Needs DB to look up user plan and ownerid
  - Happens at worker process level
  - **Solution**: Use shared session for routing (read-only, short-lived)
  
- **Task Execution**: Celery doesn't need DB connection
  - Tasks use their own per-task sessions
  - No shared state between tasks

**Key Implementation Points:**

1. **Keep engine at worker level** (shared connection pool)
2. **Create new session per task** (`create_task_session()`)
3. **Cleanup in `finally` block** (always rollback/close)
4. **Handle routing separately** (use shared session or temporary session)

**Code Pattern:**
```python
def run(self, *args, **kwargs):
    # Create NEW session for this task
    db_session = create_task_session()
    try:
        result = self.run_impl(db_session, *args, **kwargs)
        if db_session.in_transaction():
            db_session.commit()
        return result
    except Exception:
        db_session.rollback()
        raise
    finally:
        db_session.close()
```

**Benefits:**
- ✅ Eliminates transaction contamination
- ✅ Automatic cleanup
- ✅ Same API (no task code changes needed)
- ✅ Efficient (uses shared connection pool)
- ✅ Handles Celery routing

---

## PgBouncer Connection Limits and Per-Task Connections

### The Connection Limit Challenge

**Problem**: If each task gets its own connection, you need many connections:
- Example: 100 concurrent tasks = 100 connections
- With multiple workers: 10 workers × 100 tasks = 1000 connections
- This can exhaust database connection limits

### PgBouncer Connection Settings

**Key Settings:**

1. **`max_client_conn`** (default: 100)
   - Maximum number of **client connections** to PgBouncer
   - Each Celery worker process = 1 client connection
   - **Not** the number of tasks - tasks share the worker's connection
   - Increase if you have many worker processes
   - May require increasing OS file descriptor limits

2. **`default_pool_size`** (default: 20)
   - Number of **server connections** per database/user combination
   - This is the actual pool size to PostgreSQL
   - Shared across all clients (worker processes)
   - **Critical**: This limits concurrent database operations

3. **`max_db_connections`** (per-database override)
   - Override `default_pool_size` for specific databases
   - Can set different limits per database

4. **`max_user_connections`** (per-user override)
   - Override `default_pool_size` for specific users
   - Can set different limits per user

5. **`reserve_pool_size`** (default: 0)
   - Reserve connections for admin/emergency use
   - Not used for normal client connections

### Connection Architecture with Per-Task Sessions

**Important Distinction:**
- **Client Connection**: Worker process → PgBouncer (1 per worker process)
- **Server Connection**: PgBouncer → PostgreSQL (pooled, shared)
- **Task Session**: SQLAlchemy session (can be per-task, but shares server connection)

**With Per-Task Sessions:**
```
Worker Process 1 (1 client conn)
  ├─ Task A (session 1) ──┐
  ├─ Task B (session 2) ──┼─→ PgBouncer ──→ PostgreSQL
  └─ Task C (session 3) ──┘   (pool of server conns)
```

**Key Insight**: 
- Per-task **sessions** don't require per-task **connections**
- Sessions can share the same server connection (with transaction pooling)
- Only **concurrent transactions** need separate server connections

### Calculating Connection Requirements

**Formula:**
```
Required server connections = max_concurrent_transactions_per_worker × num_workers
```

**Example:**
- 10 worker processes
- Each worker handles 1 task at a time (prefork, sequential)
- Max concurrent transactions = 10
- **Required**: `default_pool_size >= 10`

**With Concurrent Tasks:**
- 10 worker processes
- Each worker handles 5 concurrent tasks (thread pool)
- Max concurrent transactions = 50
- **Required**: `default_pool_size >= 50`

### Recommended PgBouncer Configuration

**For Per-Task Sessions with Sequential Tasks (Current Setup):**
```ini
[pgbouncer]
# Allow many worker processes to connect
max_client_conn = 1000

# Pool size = number of workers (since tasks are sequential)
default_pool_size = 100

# Reserve some connections for admin
reserve_pool_size = 5

# Use transaction pooling (recommended)
pool_mode = transaction

# Timeout settings
server_idle_timeout = 600
query_timeout = 0  # Let PostgreSQL handle query timeouts
```

**For Per-Task Sessions with Concurrent Tasks:**
```ini
[pgbouncer]
max_client_conn = 1000

# Pool size = workers × concurrent_tasks_per_worker
default_pool_size = 500  # 100 workers × 5 concurrent tasks

reserve_pool_size = 10
pool_mode = transaction
```

### Connection Pooling Strategies

**Strategy 1: Transaction Pooling (Recommended)**
- **How it works**: Server connection returned to pool after each transaction
- **Benefit**: Fewer server connections needed
- **Requirement**: `default_pool_size >= max_concurrent_transactions`
- **Best for**: Per-task sessions with transaction boundaries

**Strategy 2: Session Pooling**
- **How it works**: Server connection held for entire client session
- **Benefit**: Simpler, no transaction boundary issues
- **Requirement**: `default_pool_size >= max_client_conn`
- **Problem**: Doesn't help with transaction contamination

**Strategy 3: Statement Pooling**
- **How it works**: Autocommit mode, no transactions
- **Benefit**: Minimal connections needed
- **Problem**: Not suitable for this codebase (requires transactions)

### Monitoring Connection Usage

**Check Current Usage:**
```sql
-- In PgBouncer admin console
SHOW POOLS;
SHOW STATS;
SHOW CLIENTS;
SHOW SERVERS;
```

**Key Metrics:**
- `cl_waiting`: Clients waiting for connection (indicates pool exhaustion)
- `sv_active`: Active server connections
- `sv_idle`: Idle server connections
- `maxwait`: Maximum wait time for connection

**Alert When:**
- `cl_waiting > 0`: Pool is exhausted, tasks are waiting
- `sv_active >= default_pool_size`: Approaching limit
- `maxwait > threshold`: Tasks waiting too long

### Recommendations

**For Per-Task Sessions:**

1. **Use Transaction Pooling**:
   - Allows sharing server connections across tasks
   - Reduces connection requirements
   - Still provides transaction isolation

2. **Set `default_pool_size` appropriately**:
   - For sequential tasks: `num_workers + buffer`
   - For concurrent tasks: `num_workers × concurrent_tasks + buffer`
   - Add 20% buffer for safety

3. **Monitor connection usage**:
   - Set up alerts for `cl_waiting > 0`
   - Track connection pool utilization
   - Adjust pool size based on actual usage

4. **Consider Connection Limits**:
   - PostgreSQL `max_connections` must be >= `default_pool_size`
   - Account for admin connections, replication, etc.
   - Example: If `default_pool_size = 100`, set PostgreSQL `max_connections = 150`

**Example Configuration:**
```ini
# PgBouncer config
max_client_conn = 1000
default_pool_size = 200  # 100 workers × 2 (buffer)
reserve_pool_size = 10
pool_mode = transaction
server_idle_timeout = 600

# PostgreSQL config (postgresql.conf)
max_connections = 250  # 200 pool + 50 for admin/replication
```

**Trade-off Analysis:**
- ✅ Per-task sessions provide transaction isolation
- ✅ Transaction pooling allows connection sharing
- ⚠️ Need to size pool correctly
- ⚠️ Monitor for connection exhaustion
- ⚠️ May need to increase PostgreSQL `max_connections`

---

## Common Patterns Leading to Timeouts

1. **External API Calls Before Commit**
   - Git provider API calls (`get_commit_diff`, `get_commit`, `get_pull_request`, `get_compare`)
   - Network latency, rate limiting, or slow provider responses

2. **Storage Operations Before Commit**
   - Reading/writing files to GCS/S3
   - Large file operations
   - Network latency

3. **CPU-Intensive Processing Before Commit**
   - Report merging for large codebases
   - Bundle analysis processing
   - Diff application

4. **Redis Lock Context with Open Transactions**
   - **Important Note**: The locks mentioned are **Redis locks** (not database locks), implemented via `LockManager` using `redis.lock()`
   - Most tasks use `blocking_timeout=None`, so they don't block waiting for locks (they fail immediately if unavailable)
   - However, the database transaction remains open **while holding the Redis lock** and doing work inside the lock context
   - Long-running operations inside the lock context keep the transaction idle
   - Example: In `UploadFinisherTask._process_reports_with_lock()`, the transaction starts before acquiring the Redis lock, and remains open during all operations inside `lock_manager.locked()` context

5. **Task Timeouts**
   - **Soft timeouts**: Can occur during long-running operations, leaving transaction open
   - **Hard timeouts**: Forcefully kill the task, potentially preventing cleanup (`wrap_up_dbsession()` may not execute)
   - Transactions remain open until PostgreSQL timeout or connection cleanup

6. **Retry Logic and Transaction Locks**
   - When tasks retry with exponential backoff, transaction may remain open
   - **Critical Issue**: If a task times out with an open transaction:
     - Redis lock expires, allowing retry to acquire it
     - Database transaction remains open, holding locks
     - Retry task can acquire Redis lock but may be blocked by database locks
     - This creates a situation where retries fail due to locks from the original transaction
   - Tasks using `SELECT FOR UPDATE` (like `mark_owner_for_deletion`, `detect_flakes`, `cleanup/repository`) are particularly vulnerable

## Recommendations

1. **Commit Early**: Commit database changes before long-running operations when possible
   - This prevents transactions from holding locks during external operations
   - Reduces risk of retries being blocked by open transactions

2. **Split Operations**: Separate database operations from external API/storage operations
   - Do database work first, commit, then do external work
   - Prevents long-running external operations from keeping transactions open

3. **Use Savepoints**: Use SQLAlchemy savepoints for nested transaction-like behavior
   - Allows partial rollback without closing entire transaction

4. **Handle Timeouts Properly**: 
   - Ensure `wrap_up_dbsession()` is called even on hard timeouts (though this may not be possible if process is killed)
   - Consider adding transaction rollback on timeout to release locks
   - Monitor for transactions left open after timeouts

5. **Monitor**: Add metrics to track transaction duration and identify problematic patterns
   - Track how long transactions stay open
   - Monitor for retries blocked by open transactions

6. **Set Appropriate Task Timeouts**: 
   - Ensure task hard time limits are reasonable
   - If timeout is 1300000ms (~21 minutes), this is likely sufficient
   - **But**: Focus on preventing transactions from staying open, not just increasing timeout

7. **Transaction Rollback on Timeout**: 
   - Consider rolling back transactions when soft timeouts occur during long-running operations
   - This releases locks and allows retries to proceed
   - May require restructuring code to commit before long-running operations

8. **PgBouncer Configuration**:
   - **Use Transaction Pooling Mode** - helps with hard timeouts where worker process dies (TCP closes)
   - **BUT**: PgBouncer **cannot detect** soft timeouts where only task dies but worker stays alive
   - **Critical Limitation**: PgBouncer operates at TCP connection level, not task level
   - If worker process stays alive, TCP connection stays open → PgBouncer sees healthy connection → transaction remains open
   - Avoid Session Pooling Mode - connections persist across tasks, allowing transaction contamination
   - Configure appropriate `server_idle_timeout` to detect dead connections
   - **Still need application-level transaction rollback** for soft timeouts

9. **Handle Shared Session Contamination**:
   - Add transaction state validation at task start - check for open transactions and rollback if found
   - Consider per-task sessions instead of shared `scoped_session` (requires refactoring)
   - Add monitoring to detect when tasks inherit open transactions
   - Ensure `on_failure` handler rolls back transactions to prevent contamination

