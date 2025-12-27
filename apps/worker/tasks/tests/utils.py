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

GLOBALS_USING_TASK_SESSION = [
    "tasks.base.create_task_session",
    "database.engine.create_task_session",
    "celery_task_router.create_task_session",
]


def hook_session(mocker, dbsession: Session, request=None):
    """Configure all tasks to use the shared test session."""

    mocker.patch("shared.metrics")
    for path in GLOBALS_USING_SESSION:
        mocker.patch(path, return_value=dbsession)

    mocker.patch("tasks.base.close_old_connections")
    mocker.patch.object(dbsession, "close", lambda: None)
    mocker.patch.object(dbsession, "in_transaction", lambda: False)

    original_commit = dbsession.commit

    def flush_instead_of_commit():
        dbsession.flush()

    mocker.patch.object(dbsession, "commit", flush_instead_of_commit)

    set_test_session_factory(lambda: dbsession)

    def cleanup():
        set_test_session_factory(None)
        dbsession.commit = original_commit

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
    """Patch get_repo_provider_service in all modules that import it."""
    for path in GLOBALS_USING_REPO_PROVIDER:
        mocker.patch(path, return_value=mock_repo_provider)


def ensure_hard_time_limit_task_is_numeric(
    mocker, task_instance, default_value: int = 720
):
    """Patch hard_time_limit_task to return a numeric value instead of MagicMock."""
    original_getter = task_instance.__class__.hard_time_limit_task.fget

    def safe_hard_time_limit_task(self):
        try:
            value = original_getter(self)
            if isinstance(value, int | float) and value > 0:
                return int(value)
        except AttributeError | TypeError:
            pass
        return default_value

    mocker.patch.object(
        task_instance.__class__,
        "hard_time_limit_task",
        property(safe_hard_time_limit_task),
    )
