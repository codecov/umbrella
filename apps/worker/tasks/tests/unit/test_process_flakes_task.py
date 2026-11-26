import pytest
from celery.exceptions import Retry
from django.db import OperationalError

from tasks.process_flakes import ProcessFlakesTask


def test_process_flakes_task_success(mocker):
    mocker.patch("tasks.process_flakes.process_flakes_for_repo", return_value=True)
    assert ProcessFlakesTask().run_impl(None, repo_id=1) == {"successful": True}


def test_process_flakes_task_retries_on_db_unavailable(mocker):
    mocker.patch(
        "tasks.process_flakes.process_flakes_for_repo",
        side_effect=OperationalError('could not translate host name "pgbouncer"'),
    )
    task = ProcessFlakesTask()
    mock_retry = mocker.patch.object(task, "safe_retry", side_effect=Retry())
    capture = mocker.patch("tasks.process_flakes.sentry_sdk.capture_exception")

    with pytest.raises(Retry):
        task.run_impl(None, repo_id=1)

    mock_retry.assert_called_once()
    capture.assert_not_called()


def test_process_flakes_task_reports_when_retries_exhausted(mocker):
    exc = OperationalError('could not translate host name "pgbouncer"')
    mocker.patch("tasks.process_flakes.process_flakes_for_repo", side_effect=exc)
    task = ProcessFlakesTask()
    mocker.patch.object(task, "safe_retry", return_value=False)
    capture = mocker.patch("tasks.process_flakes.sentry_sdk.capture_exception")

    assert task.run_impl(None, repo_id=1) == {"successful": False}
    capture.assert_called_once_with(exc)


def test_process_flakes_task_reports_other_errors(mocker):
    exc = ValueError("boom")
    mocker.patch("tasks.process_flakes.process_flakes_for_repo", side_effect=exc)
    task = ProcessFlakesTask()
    mock_retry = mocker.patch.object(task, "safe_retry")
    capture = mocker.patch("tasks.process_flakes.sentry_sdk.capture_exception")

    assert task.run_impl(None, repo_id=1) == {"successful": False}
    mock_retry.assert_not_called()
    capture.assert_called_once_with(exc)
