import contextlib
from collections.abc import Generator

from sqlalchemy.orm import Session

from app import celery_app


@contextlib.contextmanager
def run_tasks() -> Generator[None]:
    prev = celery_app.conf.task_always_eager
    celery_app.conf.update(task_always_eager=True)
    try:
        yield
    finally:
        celery_app.conf.update(task_always_eager=prev)


GLOBALS_USING_SESSION = [
    "database.engine.get_db_session",
    "tasks.base.get_db_session",
]

GLOBALS_USING_TASK_SESSION = [
    "tasks.base.create_task_session",
    "database.engine.create_task_session",
    "celery_task_router.create_task_session",
]


def hook_session(mocker, dbsession: Session, request=None):
    """
    Configure the session factory to use a shared test session.
    
    This uses the session factory pattern instead of patching, making it much cleaner.
    All tasks will use the same test session, so they can see test data.
    
    Args:
        mocker: pytest-mock fixture
        dbsession: The test database session to use
        request: pytest request fixture (optional, for cleanup)
    """
    from database.engine import set_test_session_factory
    
    mocker.patch("shared.metrics")
    # Patch get_db_session for routing and legacy code
    for path in GLOBALS_USING_SESSION:
        mocker.patch(path, return_value=dbsession)
    
    # Patch close_old_connections to avoid Django database access issues in tests
    mocker.patch("tasks.base.close_old_connections")
    
    # Patch close() on the session to be a no-op so routing sessions don't close the test session
    # apply_async() calls routing_session.close() directly, so we need to prevent that
    mocker.patch.object(dbsession, "close", lambda: None)
    
    # Patch in_transaction() to return False so tasks don't think there's an open transaction
    # This prevents BaseCodecovTask.run() from rolling back test data
    # We don't patch rollback() because SQLAlchemy needs it to handle errors normally
    mocker.patch.object(dbsession, "in_transaction", lambda: False)
    
    # Patch commit() to be flush() instead - we don't want to commit in tests, just flush
    # This ensures changes are visible but don't persist beyond the test
    original_commit = dbsession.commit
    def flush_instead_of_commit():
        dbsession.flush()
    mocker.patch.object(dbsession, "commit", flush_instead_of_commit)
    
    # Use the session factory pattern - set a factory that returns the test session
    # This is much cleaner than patching create_task_session everywhere
    set_test_session_factory(lambda: dbsession)
    
    # Cleanup: restore default behavior after test
    def cleanup():
        set_test_session_factory(None)
        # Restore original commit if it was patched
        if 'original_commit' in locals():
            dbsession.commit = original_commit
    
    # Register cleanup if request fixture is available
    if request is not None:
        request.addfinalizer(cleanup)


GLOBALS_USING_REPO_PROVIDER = [
    "services.comparison.get_repo_provider_service",
    "services.report.get_repo_provider_service",
    "tasks.notify.get_repo_provider_service",
    "tasks.upload_finisher.get_repo_provider_service",
    "tasks.base.get_repo_provider_service",
]


def hook_repo_provider(mocker, mock_repo_provider):
    """
    Hooks / mocks various `get_repo_provider_service` locals.
    Due to how import resolution works in python, we have to patch this
    *everywhere* that is *imported* into, instead of patching the function where
    it is defined.
    The reason is that imports are resolved at import time, and overriding the
    function definition after the fact does not work.
    """
    for path in GLOBALS_USING_REPO_PROVIDER:
        mocker.patch(path, return_value=mock_repo_provider)


def ensure_hard_time_limit_task_is_numeric(
    mocker, task_instance, default_value: int = 720
):
    """
    Ensures that hard_time_limit_task returns a numeric value for testing.

    This helper patches hard_time_limit_task to return a proper integer value,
    preventing issues where MagicMock objects might be returned when app.conf
    is mocked.

    Use this helper when testing code that calls get_lock_timeout() to ensure
    hard_time_limit_task returns a proper numeric value.

    Args:
        mocker: The pytest mocker fixture
        task_instance: The task instance to patch
        default_value: The default value to return (default: 720)
    """
    # Get the original property getter
    original_getter = task_instance.__class__.hard_time_limit_task.fget

    def safe_hard_time_limit_task(self):
        try:
            value = original_getter(self)
            if isinstance(value, int | float):
                # Use the original value if it's valid and greater than 0
                # Otherwise fall back to default_value
                if value > 0:
                    return int(value)
        except AttributeError | TypeError:
            pass
        return default_value

    mocker.patch.object(
        task_instance.__class__,
        "hard_time_limit_task",
        property(safe_hard_time_limit_task),
    )
