import contextlib
from collections.abc import Generator

from sqlalchemy.orm import Session

from app import celery_app
from database.engine import set_test_session_factory


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


def hook_session(mocker, dbsession: Session, request=None):
    """Configure all tasks to use the shared test session."""
    mocker.patch("shared.metrics")
    for path in GLOBALS_USING_SESSION:
        mocker.patch(path, return_value=dbsession)

    # Prevent session from being closed during tests
    mocker.patch("tasks.base.close_old_connections")
    mocker.patch.object(dbsession, "close", lambda: None)
    mocker.patch.object(dbsession, "in_transaction", lambda: False)

    # Replace commit with flush to keep data visible within test transaction
    original_commit = dbsession.commit

    def flush_instead_of_commit():
        dbsession.flush()

    mocker.patch.object(dbsession, "commit", flush_instead_of_commit)

    # Configure create_task_session to return the test session
    set_test_session_factory(lambda: dbsession)

    def cleanup():
        set_test_session_factory(None)
        dbsession.commit = original_commit

    # Always register cleanup - use request.addfinalizer if available,
    # otherwise use mocker's stopall which runs after each test
    if request is not None:
        request.addfinalizer(cleanup)
    else:
        # Fallback: register cleanup with mocker's stopall mechanism
        mocker.stopall  # noqa: B018 - access triggers registration
        original_stopall = mocker.stopall

        def cleanup_and_stopall():
            cleanup()
            original_stopall()

        mocker.stopall = cleanup_and_stopall


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
