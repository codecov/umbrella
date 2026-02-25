from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, call, patch

import psycopg2
import pytest
from celery import chain
from celery.contrib.testing.mocks import TaskMessage
from celery.exceptions import MaxRetriesExceededError, Retry, SoftTimeLimitExceeded
from prometheus_client import REGISTRY
from redis.exceptions import LockError
from redis.lock import Lock
from sqlalchemy.exc import (
    DBAPIError,
    IntegrityError,
    InvalidRequestError,
    StatementError,
)

from database.enums import CommitErrorTypes
from database.models.core import GITHUB_APP_INSTALLATION_DEFAULT_NAME
from database.tests.factories.core import OwnerFactory, RepositoryFactory
from helpers.exceptions import NoConfiguredAppsAvailable, RepositoryWithoutValidBotError
from shared.celery_config import (
    sync_repos_task_name,
    upload_breadcrumb_task_name,
    upload_task_name,
)
from shared.django_apps.upload_breadcrumbs.models import BreadcrumbData, Errors
from shared.plan.constants import PlanName
from shared.torngit.exceptions import TorngitClientError
from tasks.base import (
    _RETRY_COUNTDOWN_CEILING,
    _RETRY_COUNTDOWN_FLOOR_SECONDS,
    BaseCodecovRequest,
    BaseCodecovTask,
    clamp_retry_countdown,
)
from tasks.base import celery_app as base_celery_app
from tests.helpers import mock_all_plans_and_tiers

here = Path(__file__)


class TestClampRetryCountdown:
    def test_below_floor_returns_floor(self):
        assert clamp_retry_countdown(0) == _RETRY_COUNTDOWN_FLOOR_SECONDS
        assert clamp_retry_countdown(-1) == _RETRY_COUNTDOWN_FLOOR_SECONDS
        assert (
            clamp_retry_countdown(_RETRY_COUNTDOWN_FLOOR_SECONDS - 1)
            == _RETRY_COUNTDOWN_FLOOR_SECONDS
        )

    def test_above_ceiling_returns_ceiling(self):
        assert (
            clamp_retry_countdown(_RETRY_COUNTDOWN_CEILING + 1)
            == _RETRY_COUNTDOWN_CEILING
        )
        assert clamp_retry_countdown(99999) == _RETRY_COUNTDOWN_CEILING

    def test_within_range_returns_value(self):
        mid = (_RETRY_COUNTDOWN_FLOOR_SECONDS + _RETRY_COUNTDOWN_CEILING) // 2
        assert clamp_retry_countdown(mid) == mid

    def test_at_floor_returns_floor(self):
        assert (
            clamp_retry_countdown(_RETRY_COUNTDOWN_FLOOR_SECONDS)
            == _RETRY_COUNTDOWN_FLOOR_SECONDS
        )

    def test_at_ceiling_returns_ceiling(self):
        assert (
            clamp_retry_countdown(_RETRY_COUNTDOWN_CEILING) == _RETRY_COUNTDOWN_CEILING
        )


@pytest.fixture
def mock_self_app(mocker, celery_app):
    mock_app = celery_app
    mock_app.tasks[upload_breadcrumb_task_name] = mocker.MagicMock()

    return mocker.patch.object(
        BaseCodecovTask,
        "app",
        mock_app,
    )


class MockDateTime(datetime):
    """
    `@pytest.mark.freeze_time()` is convenient but will freeze time for
    everything, including timeseries metrics for which a timestamp is
    a primary key.

    This class can be used to mock time more narrowly.
    """

    @classmethod
    def now(cls):
        return datetime.fromisoformat("2023-06-13T10:01:01.000123")


class SampleTask(BaseCodecovTask, name="test.SampleTask"):
    def run_impl(self, dbsession):
        return {"unusual": "return", "value": ["There"]}


class SampleTaskWithArbitraryError(
    BaseCodecovTask, name="test.SampleTaskWithArbitraryError"
):
    def __init__(self, error):
        self.error = error

    def run_impl(self, dbsession):
        raise self.error

    def retry(self, max_retries=None, countdown=None, exc=None, **kwargs):
        # Fake retry method
        raise Retry()


class SampleTaskWithArbitraryPostgresError(
    BaseCodecovTask, name="test.SampleTaskWithArbitraryPostgresError"
):
    def __init__(self, error):
        self.error = error

    def run_impl(self, dbsession):
        raise DBAPIError("statement", "params", self.error)

    def retry(self, max_retries=None, countdown=None, exc=None, **kwargs):
        # Fake retry method
        raise Retry()


class SampleTaskWithSoftTimeout(BaseCodecovTask, name="test.SampleTaskWithSoftTimeout"):
    def run_impl(self, dbsession):
        raise SoftTimeLimitExceeded()


class FailureSampleTask(BaseCodecovTask, name="test.FailureSampleTask"):
    def run_impl(self, *args, **kwargs):
        raise Exception("Whhhhyyyyyyy")


class RetrySampleTask(BaseCodecovTask, name="test.RetrySampleTask"):
    def run(self, *args, **kwargs):
        self.retry()


@pytest.mark.django_db(databases={"default", "timeseries"})
class TestBaseCodecovTask:
    def test_hard_time_limit_task_with_request_data(self, mocker):
        mocker.patch.object(SampleTask, "request", timelimit=[200, 123])
        r = SampleTask()
        assert r.hard_time_limit_task == 200

    def test_hard_time_limit_task_from_default_app(self, mocker):
        mocker.patch.object(SampleTask, "request", timelimit=None)
        r = SampleTask()
        assert r.hard_time_limit_task == 720

    @patch("tasks.base.datetime", MockDateTime)
    def test_sample_run(self, mocker, dbsession):
        mocked_get_db_session = mocker.patch("tasks.base.get_db_session")
        mock_task_request = mocker.patch("tasks.base.BaseCodecovTask.request")
        fake_request_values = {
            "created_timestamp": "2023-06-13 10:00:00.000000",
            "delivery_info": {"routing_key": "my-queue"},
        }
        mock_task_request.get.side_effect = (
            lambda key, default: fake_request_values.get(key, default)
        )
        mocked_get_db_session.return_value = dbsession
        task_instance = SampleTask()
        result = task_instance.run()
        assert result == {"unusual": "return", "value": ["There"]}
        assert (
            REGISTRY.get_sample_value(
                "worker_tasks_timers_time_in_queue_seconds_sum",
                labels={"task": SampleTask.name, "queue": "my-queue"},
            )
            == 61.000123
        )

    def test_sample_run_db_exception(self, mocker, dbsession):
        mocked_get_db_session = mocker.patch("tasks.base.get_db_session")
        mocked_get_db_session.return_value = dbsession
        # Mock request to skip queue metrics
        mocker.patch("tasks.base.BaseCodecovTask.request", None)
        with pytest.raises(Retry):
            SampleTaskWithArbitraryError(
                DBAPIError("statement", "params", "orig")
            ).run()

    def test_sample_run_integrity_error(self, mocker, dbsession):
        mocked_get_db_session = mocker.patch("tasks.base.get_db_session")
        mocked_get_db_session.return_value = dbsession
        # Mock request to skip queue metrics
        mocker.patch("tasks.base.BaseCodecovTask.request", None)
        with pytest.raises(Retry):
            SampleTaskWithArbitraryError(
                IntegrityError("statement", "params", "orig")
            ).run()

    def test_sample_run_deadlock_exception(self, mocker, dbsession):
        mocked_get_db_session = mocker.patch("tasks.base.get_db_session")
        mocked_get_db_session.return_value = dbsession
        # Mock request to skip queue metrics
        mocker.patch("tasks.base.BaseCodecovTask.request", None)
        with pytest.raises(Retry):
            SampleTaskWithArbitraryPostgresError(
                psycopg2.errors.DeadlockDetected()
            ).run()

    def test_sample_run_operationalerror_exception(self, mocker, dbsession):
        mocked_get_db_session = mocker.patch("tasks.base.get_db_session")
        mocked_get_db_session.return_value = dbsession
        # Mock request to skip queue metrics
        mocker.patch("tasks.base.BaseCodecovTask.request", None)
        with pytest.raises(Retry):
            SampleTaskWithArbitraryPostgresError(psycopg2.OperationalError()).run()

    def test_sample_run_softimeout(self, mocker, dbsession):
        mocked_get_db_session = mocker.patch("tasks.base.get_db_session")
        mocked_get_db_session.return_value = dbsession
        # Mock request to skip queue metrics
        mocker.patch("tasks.base.BaseCodecovTask.request", None)
        with pytest.raises(SoftTimeLimitExceeded):
            SampleTaskWithSoftTimeout().run()

    def test_wrap_up_dbsession_success(self, mocker):
        task = BaseCodecovTask()
        fake_session = mocker.MagicMock()
        task.wrap_up_dbsession(fake_session)
        assert fake_session.commit.call_count == 1
        assert fake_session.close.call_count == 1

    def test_wrap_up_dbsession_timeout_but_ok(self, mocker):
        task = BaseCodecovTask()
        fake_session = mocker.MagicMock(
            commit=mocker.MagicMock(side_effect=[SoftTimeLimitExceeded(), 1])
        )
        task.wrap_up_dbsession(fake_session)
        assert fake_session.commit.call_count == 2
        assert fake_session.close.call_count == 1

    def test_wrap_up_dbsession_timeout_nothing_works(self, mocker):
        mocked_get_db_session = mocker.patch("tasks.base.get_db_session")
        task = BaseCodecovTask()
        fake_session = mocker.MagicMock(
            commit=mocker.MagicMock(
                side_effect=[SoftTimeLimitExceeded(), InvalidRequestError()]
            )
        )
        task.wrap_up_dbsession(fake_session)
        assert fake_session.commit.call_count == 2
        assert fake_session.close.call_count == 0
        assert mocked_get_db_session.remove.call_count == 1

    def test_wrap_up_dbsession_invalid_nothing_works(self, mocker):
        mocked_get_db_session = mocker.patch("tasks.base.get_db_session")
        task = BaseCodecovTask()
        fake_session = mocker.MagicMock(
            commit=mocker.MagicMock(side_effect=[InvalidRequestError()])
        )
        task.wrap_up_dbsession(fake_session)
        assert fake_session.commit.call_count == 1
        assert fake_session.close.call_count == 0
        assert mocked_get_db_session.remove.call_count == 1

    def test_run_success_commits_sqlalchemy(self, mocker, dbsession):
        mock_wrap_up = mocker.patch("tasks.base.BaseCodecovTask.wrap_up_dbsession")
        mock_dbsession_rollback = mocker.patch.object(dbsession, "rollback")
        mock_get_db_session = mocker.patch(
            "tasks.base.get_db_session", return_value=dbsession
        )
        # Mock request to skip queue metrics
        mocker.patch("tasks.base.BaseCodecovTask.request", None)

        task = SampleTask()
        task.run()

        assert mock_wrap_up.call_args_list == [call(dbsession)]

        assert mock_dbsession_rollback.call_count == 0

    def test_run_db_errors_rollback(self, mocker, dbsession, celery_app):
        mock_dbsession_rollback = mocker.patch.object(dbsession, "rollback")
        mock_wrap_up = mocker.patch("tasks.base.BaseCodecovTask.wrap_up_dbsession")
        mock_get_db_session = mocker.patch(
            "tasks.base.get_db_session", return_value=dbsession
        )

        # IntegrityError and DataError are subclasses of SQLAlchemyError that
        # have their own `except` clause.
        # Mock request to skip queue metrics
        mocker.patch("tasks.base.BaseCodecovTask.request", None)
        task = SampleTaskWithArbitraryError(IntegrityError("", {}, None))
        registered_task = celery_app.register_task(task)
        task = celery_app.tasks[registered_task.name]
        task.apply()

        assert mock_dbsession_rollback.call_args_list == [call()]

        assert mock_wrap_up.call_args_list == [call(dbsession)]

    def test_run_sqlalchemy_error_rollback(self, mocker, dbsession, celery_app):
        mock_dbsession_rollback = mocker.patch.object(dbsession, "rollback")
        mock_wrap_up = mocker.patch("tasks.base.BaseCodecovTask.wrap_up_dbsession")
        mock_get_db_session = mocker.patch(
            "tasks.base.get_db_session", return_value=dbsession
        )
        # Mock request to skip queue metrics
        mocker.patch("tasks.base.BaseCodecovTask.request", None)

        # StatementError is a subclass of SQLAlchemyError just like
        # IntegrityError and DataError, but this test case is different because
        # it is caught by a different except clause.
        task = SampleTaskWithArbitraryError(StatementError("", "", None, None))
        registered_task = celery_app.register_task(task)
        task = celery_app.tasks[registered_task.name]
        task.apply()

        assert mock_dbsession_rollback.call_args_list == [call()]

        assert mock_wrap_up.call_args_list == [call(dbsession)]

    def test_get_repo_provider_service_working(self, mocker, mock_self_app):
        mock_repo_provider = mocker.MagicMock()
        mock_get_repo_provider_service = mocker.patch(
            "tasks.base.get_repo_provider_service", return_value=mock_repo_provider
        )

        task = BaseCodecovTask()
        mock_repo = mocker.MagicMock()
        assert task.get_repo_provider_service(mock_repo) == mock_repo_provider
        mock_get_repo_provider_service.assert_called_with(
            mock_repo, GITHUB_APP_INSTALLATION_DEFAULT_NAME, None
        )
        mock_self_app.tasks[upload_breadcrumb_task_name].apply_async.assert_not_called()

    def test_get_repo_provider_service_rate_limited(self, mocker, mock_self_app):
        mocker.patch(
            "tasks.base.get_repo_provider_service",
            side_effect=NoConfiguredAppsAvailable(
                apps_count=2,
                rate_limited_count=2,
                suspended_count=0,
            ),
        )
        mocker.patch("tasks.base.get_seconds_to_next_hour", return_value=120)
        mock_commit = mocker.MagicMock()
        mock_commit.commitid = "abc123"

        task = BaseCodecovTask()
        mock_retry = mocker.patch.object(task, "retry")
        mock_repo = mocker.MagicMock()
        assert task.get_repo_provider_service(mock_repo, commit=mock_commit) is None
        task.retry.assert_called_with(countdown=120)
        mock_self_app.tasks[upload_breadcrumb_task_name].apply_async.assert_has_calls(
            [
                call(
                    kwargs={
                        "commit_sha": mock_commit.commitid,
                        "repo_id": mock_repo.repoid,
                        "breadcrumb_data": BreadcrumbData(
                            error=Errors.INTERNAL_APP_RATE_LIMITED,
                        ),
                        "upload_ids": [],
                        "sentry_trace_id": None,
                    }
                ),
                call(
                    kwargs={
                        "commit_sha": mock_commit.commitid,
                        "repo_id": mock_repo.repoid,
                        "breadcrumb_data": BreadcrumbData(
                            error=Errors.INTERNAL_RETRYING,
                        ),
                        "upload_ids": [],
                        "sentry_trace_id": None,
                    }
                ),
            ]
        )

    def test_get_repo_provider_service_suspended(self, mocker):
        mocker.patch(
            "tasks.base.get_repo_provider_service",
            side_effect=NoConfiguredAppsAvailable(
                apps_count=2,
                rate_limited_count=0,
                suspended_count=2,
            ),
        )
        mocker.patch("tasks.base.get_seconds_to_next_hour", return_value=120)

        task = BaseCodecovTask()
        mock_repo = mocker.MagicMock()
        assert task.get_repo_provider_service(mock_repo) is None

    def test_get_repo_provider_service_no_valid_bot(self, mocker, mock_self_app):
        mocker.patch(
            "tasks.base.get_repo_provider_service",
            side_effect=RepositoryWithoutValidBotError(),
        )
        mock_save_commit_error = mocker.patch("tasks.base.save_commit_error")

        task = BaseCodecovTask()
        mock_repo = mocker.MagicMock()
        mock_repo.repoid = 5
        mock_commit = mocker.MagicMock()
        assert task.get_repo_provider_service(mock_repo, commit=mock_commit) is None
        mock_save_commit_error.assert_called_with(
            mock_commit,
            error_code=CommitErrorTypes.REPO_BOT_INVALID.value,
            error_params={"repoid": 5},
        )
        mock_self_app.tasks[
            upload_breadcrumb_task_name
        ].apply_async.assert_called_once_with(
            kwargs={
                "commit_sha": mock_commit.commitid,
                "repo_id": mock_repo.repoid,
                "breadcrumb_data": BreadcrumbData(
                    error=Errors.REPO_MISSING_VALID_BOT,
                ),
                "upload_ids": [],
                "sentry_trace_id": None,
            }
        )

    def test_get_repo_provider_service_torngit_client_error(
        self, mocker, mock_self_app
    ):
        mocker.patch(
            "tasks.base.get_repo_provider_service", side_effect=TorngitClientError()
        )

        task = BaseCodecovTask()
        mock_repo = mocker.MagicMock()
        mock_repo.repoid = 8
        mock_commit = mocker.MagicMock()
        mock_commit.commitid = "abc123"
        task.get_repo_provider_service(mock_repo, commit=mock_commit)
        mock_self_app.tasks[
            upload_breadcrumb_task_name
        ].apply_async.assert_called_once_with(
            kwargs={
                "commit_sha": mock_commit.commitid,
                "repo_id": mock_repo.repoid,
                "breadcrumb_data": BreadcrumbData(
                    error=Errors.GIT_CLIENT_ERROR,
                ),
                "upload_ids": [],
                "sentry_trace_id": None,
            }
        )

    def test_call_upload_breadcrumb_task_exception(self, mocker, mock_self_app):
        exception = Exception("Test exception")
        mocker.patch("tasks.base.get_repo_provider_service", side_effect=exception)
        mock_self_app.tasks[
            upload_breadcrumb_task_name
        ].apply_async.side_effect = Exception("Task exception")

        task = BaseCodecovTask()
        mock_repo = mocker.MagicMock()
        mock_repo.repoid = 8
        mock_commit = mocker.MagicMock()
        mock_commit.commitid = "abc123"
        # Ensure the exception from _call_upload_breadcrumb_task does not propagate
        task.get_repo_provider_service(mock_repo, commit=mock_commit)
        mock_self_app.tasks[
            upload_breadcrumb_task_name
        ].apply_async.assert_called_once_with(
            kwargs={
                "commit_sha": mock_commit.commitid,
                "repo_id": mock_repo.repoid,
                "breadcrumb_data": BreadcrumbData(
                    error=Errors.UNKNOWN,
                    error_text=repr(exception),
                ),
                "upload_ids": [],
                "sentry_trace_id": None,
            }
        )


@pytest.mark.django_db(databases={"default", "timeseries"})
class TestBaseCodecovTaskHooks:
    def test_sample_task_success(self, celery_app):
        class SampleTask(BaseCodecovTask, name="test.SampleTask"):
            def run_impl(self, dbsession):
                return {"unusual": "return", "value": ["There"]}

        DTask = celery_app.register_task(SampleTask())
        task = celery_app.tasks[DTask.name]

        prom_run_counter_before = REGISTRY.get_sample_value(
            "worker_task_counts_runs_total", labels={"task": DTask.name}
        )
        prom_success_counter_before = REGISTRY.get_sample_value(
            "worker_task_counts_successes_total", labels={"task": DTask.name}
        )
        k = task.apply()
        prom_run_counter_after = REGISTRY.get_sample_value(
            "worker_task_counts_runs_total", labels={"task": DTask.name}
        )
        prom_success_counter_after = REGISTRY.get_sample_value(
            "worker_task_counts_successes_total", labels={"task": DTask.name}
        )

        res = k.get()
        assert res == {"unusual": "return", "value": ["There"]}
        assert prom_run_counter_after - prom_run_counter_before == 1
        assert prom_success_counter_after - prom_success_counter_before == 1

    def test_sample_task_failure(self, celery_app, mocker):
        # Mock request to skip queue metrics
        mocker.patch("tasks.base.BaseCodecovTask.request", None)

        class FailureSampleTask(BaseCodecovTask, name="test.FailureSampleTask"):
            def run_impl(self, *args, **kwargs):
                raise Exception("Whhhhyyyyyyy")

        DTask = celery_app.register_task(FailureSampleTask())
        task = celery_app.tasks[DTask.name]
        with pytest.raises(Exception) as exc:
            prom_run_counter_before = REGISTRY.get_sample_value(
                "worker_task_counts_runs_total", labels={"task": DTask.name}
            )
            prom_failure_counter_before = REGISTRY.get_sample_value(
                "worker_task_counts_failures_total", labels={"task": DTask.name}
            )
            task.apply().get()
            prom_run_counter_after = REGISTRY.get_sample_value(
                "worker_task_counts_runs_total", labels={"task": DTask.name}
            )
            prom_failure_counter_after = REGISTRY.get_sample_value(
                "worker_task_counts_failures_total", labels={"task": DTask.name}
            )
            assert prom_run_counter_after - prom_run_counter_before == 1
            assert prom_failure_counter_after - prom_failure_counter_before == 1
        assert exc.value.args == ("Whhhhyyyyyyy",)

    def test_sample_task_retry(self):
        # Unfortunately we cant really call the task with apply().get()
        # Something happens inside celery as of version 4.3 that makes them
        #   not call on_Retry at all.
        # best we can do is to call on_retry ourselves and ensure this makes the
        # metric be called
        task = RetrySampleTask()
        prom_retry_counter_before = REGISTRY.get_sample_value(
            "worker_task_counts_retries_total", labels={"task": task.name}
        )
        task.on_retry("exc", "task_id", ("args",), {"kwargs": "foo"}, "einfo")
        prom_retry_counter_after = REGISTRY.get_sample_value(
            "worker_task_counts_retries_total", labels={"task": task.name}
        )
        assert prom_retry_counter_after - prom_retry_counter_before == 1

    def test_on_retry_tracks_retry_count(self):
        # Verify that on_retry increments the retry_by_count metric
        task = RetrySampleTask()
        task.request.retries = 3

        counter_before = REGISTRY.get_sample_value(
            "worker_task_counts_retries_by_count_total",
            labels={"task": task.name, "retry_count": "3"},
        )

        task.on_retry("exc", "task_id", ("args",), {"kwargs": "foo"}, "einfo")

        counter_after = REGISTRY.get_sample_value(
            "worker_task_counts_retries_by_count_total",
            labels={"task": task.name, "retry_count": "3"},
        )

        expected_increment = 1 if counter_before is not None else 1
        actual_increment = (counter_after or 0) - (counter_before or 0)
        assert actual_increment == expected_increment


@pytest.mark.django_db(databases={"default", "timeseries"})
class TestBaseCodecovTaskSafeRetry:
    def test_safe_retry_succeeds_below_max_retries(self, mocker):
        task = SampleTask()
        task.request.retries = 2
        task.max_retries = 5
        task.request.headers = {}

        # Mock the parent class's retry method to raise Retry exception (normal behavior)
        mock_retry = mocker.patch("celery.app.task.Task.retry", side_effect=Retry())

        # safe_retry() raises Retry exception when successful (doesn't return True)
        with pytest.raises(Retry):
            task.safe_retry(countdown=60)

        # Verify retry() was called with correct arguments
        # safe_retry now adds attempts to headers for the NEXT attempt
        # Current attempt: retries=2, so attempts=3 (2+1)
        # Next attempt will be: attempts=4 (3+1)
        assert mock_retry.called
        call_kwargs = mock_retry.call_args[1]
        assert call_kwargs["max_retries"] == 5
        assert call_kwargs["countdown"] == 60
        assert call_kwargs["exc"] is None
        assert "headers" in call_kwargs
        assert (
            call_kwargs["headers"]["attempts"] == 4
        )  # (retries 2 + 1) + 1 for next attempt

    def test_safe_retry_fails_at_max_retries(self, mocker):
        task = SampleTask()
        task.request.retries = 10
        task.max_retries = 10
        task.request.headers = {}

        mock_retry = mocker.patch.object(task, "retry")

        counter_before = REGISTRY.get_sample_value(
            "worker_task_counts_max_retries_exceeded_total",
            labels={"task": task.name},
        )

        result = task.safe_retry(countdown=60)

        assert result is False
        mock_retry.assert_not_called()

        counter_after = REGISTRY.get_sample_value(
            "worker_task_counts_max_retries_exceeded_total",
            labels={"task": task.name},
        )

        expected_increment = 1 if counter_before is not None else 1
        actual_increment = (counter_after or 0) - (counter_before or 0)
        assert actual_increment == expected_increment

    def test_safe_retry_uses_exponential_backoff_by_default(self, mocker):
        task = SampleTask()
        task.request.retries = 3
        task.max_retries = 10
        task.request.headers = {}

        # Mock the parent class's retry method so our overridden retry() still runs
        mock_retry = mocker.patch("celery.app.task.Task.retry")

        # Call without countdown - should use exponential backoff
        # Formula: TASK_RETRY_BACKOFF_BASE_SECONDS * (2 ** retry_count)
        # For retry 3: 20 * (2 ** 3) = 20 * 8 = 160
        task.safe_retry()

        mock_retry.assert_called_once()
        call_kwargs = mock_retry.call_args[1]
        assert call_kwargs["countdown"] == 160  # 20 * 2^3
        # Current attempt: retries=3, so attempts=4 (3+1)
        # Next attempt will be: attempts=5 (4+1)
        assert (
            call_kwargs["headers"]["attempts"] == 5
        )  # (retries 3 + 1) + 1 for next attempt

    def test_safe_retry_handles_max_retries_exceeded_exception(self, mocker):
        task = SampleTask()
        task.request.retries = 9
        task.max_retries = 10
        task.request.headers = {}

        # Make parent retry raise MaxRetriesExceededError
        mock_retry = mocker.patch(
            "celery.app.task.Task.retry", side_effect=MaxRetriesExceededError()
        )

        counter_before = REGISTRY.get_sample_value(
            "worker_task_counts_max_retries_exceeded_total",
            labels={"task": task.name},
        )

        result = task.safe_retry(countdown=60)

        assert result is False

        counter_after = REGISTRY.get_sample_value(
            "worker_task_counts_max_retries_exceeded_total",
            labels={"task": task.name},
        )

        expected_increment = 1 if counter_before is not None else 1
        actual_increment = (counter_after or 0) - (counter_before or 0)
        assert actual_increment == expected_increment

    def test_safe_retry_fails_when_attempts_exceeds_max(self, mocker):
        """Test that safe_retry fails when attempts exceeds max_retries even if retries don't."""
        task = SampleTask()
        task.request.retries = 3  # Below max_retries
        task.max_retries = 10
        # Simulate re-delivery: attempts is ahead of retry_count
        task.request.headers = {"attempts": 12}  # Exceeds max_retries (10)

        mock_retry = mocker.patch("celery.app.task.Task.retry")

        counter_before = REGISTRY.get_sample_value(
            "worker_task_counts_max_retries_exceeded_total",
            labels={"task": task.name},
        )

        result = task.safe_retry(countdown=60)

        assert result is False
        mock_retry.assert_not_called()

        counter_after = REGISTRY.get_sample_value(
            "worker_task_counts_max_retries_exceeded_total",
            labels={"task": task.name},
        )

        expected_increment = 1 if counter_before is not None else 1
        actual_increment = (counter_after or 0) - (counter_before or 0)
        assert actual_increment == expected_increment

    def test_safe_retry_merges_existing_headers(self, mocker):
        """Test that safe_retry merges attempts with existing headers."""
        task = SampleTask()
        task.request.retries = 1
        task.max_retries = 5
        task.request.headers = {"custom_header": "value"}

        # Mock the parent class's retry method so our overridden retry() still runs
        mock_retry = mocker.patch("celery.app.task.Task.retry")

        task.safe_retry(countdown=60, custom_kwarg="test")

        call_kwargs = mock_retry.call_args[1]
        assert call_kwargs["headers"]["attempts"] == 3  # (retries 1 + 1) + 1
        assert call_kwargs["headers"]["custom_header"] == "value"
        assert call_kwargs["custom_kwarg"] == "test"
        assert call_kwargs["headers"]["custom_header"] == "value"
        assert call_kwargs["custom_kwarg"] == "test"


@pytest.mark.django_db(databases={"default", "timeseries"})
class TestBaseCodecovTaskGetTotalAttempts:
    def test_get_attempts_without_request(self, mocker):
        """Test _get_attempts when request doesn't exist."""
        mocker.patch("tasks.base.BaseCodecovTask.request", None)
        task = SampleTask()
        assert task.attempts == 0

    def test_get_attempts_without_headers(self):
        """Test _get_attempts when headers don't have attempts."""
        task = SampleTask()
        task.request.retries = 3
        task.request.headers = {}
        assert task.attempts == 4  # retries (3) + 1

    def test_get_attempts_with_valid_header(self):
        """Test _get_attempts when headers have valid attempts."""
        task = SampleTask()
        task.request.retries = 2
        task.request.headers = {"attempts": 5}
        assert task.attempts == 5  # Uses header value

    def test_get_attempts_with_invalid_header_string(self):
        """Test _get_attempts when headers have invalid string value."""
        task = SampleTask()
        task.request.retries = 2
        task.request.headers = {"attempts": "invalid"}
        # Should fallback to retry_count + 1
        assert task.attempts == 3  # retries (2) + 1

    def test_get_attempts_with_invalid_header_none(self):
        """Test _get_attempts when headers have None value."""
        task = SampleTask()
        task.request.retries = 2
        task.request.headers = {"attempts": None}
        # Should fallback to retry_count + 1
        assert task.attempts == 3  # retries (2) + 1

    def test_get_attempts_without_retries_attribute(self, mocker):
        """Test _get_attempts when request doesn't have retries attribute."""
        # Create a mock request without retries attribute
        mock_request = MagicMock(
            spec=[]
        )  # spec=[] prevents auto-creation of attributes
        mock_request.headers = {}
        # Mock hasattr to return False for retries on request
        original_hasattr = hasattr

        def mock_hasattr(obj, name):
            if obj is mock_request and name == "retries":
                return False
            return original_hasattr(obj, name)

        mocker.patch("builtins.hasattr", side_effect=mock_hasattr)
        # Patch at class level BEFORE creating instance to avoid property setter/deleter issues
        # This matches the pattern used in test_hard_time_limit_task_with_request_data
        mock_property = PropertyMock(return_value=mock_request)
        mocker.patch.object(SampleTask, "request", mock_property, create=True)
        task = SampleTask()
        assert task.attempts == 1  # 0 + 1

    def test_get_attempts_with_re_delivery_scenario(self):
        """Test _get_attempts in re-delivery scenario (attempts > retry_count + 1)."""
        task = SampleTask()
        task.request.retries = 2  # Normal retry count
        task.request.headers = {"attempts": 5}  # Higher due to re-delivery
        assert task.attempts == 5  # Should use header value


@pytest.mark.django_db(databases={"default", "timeseries"})
class TestBaseCodecovTaskHasExceededMaxAttempts:
    def test_has_exceeded_max_attempts_none_max_retries(self, mocker):
        """Test _has_exceeded_max_attempts when max_retries is None."""
        mock_request = MagicMock()
        mock_request.retries = 100
        mock_request.headers = {"attempts": 100}
        mock_property = PropertyMock(return_value=mock_request)
        mocker.patch.object(SampleTask, "request", mock_property, create=True)
        task = SampleTask()
        assert task._has_exceeded_max_attempts(max_retries=None) is False

    def test_has_exceeded_max_attempts_by_attempts(self, mocker):
        """Test _has_exceeded_max_attempts when attempts >= max_retries."""
        mock_request = MagicMock()
        mock_request.retries = 5
        mock_request.headers = {"attempts": 11}
        mock_property = PropertyMock(return_value=mock_request)
        mocker.patch.object(SampleTask, "request", mock_property, create=True)
        task = SampleTask()
        assert task._has_exceeded_max_attempts(max_retries=10) is True

    def test_has_exceeded_max_attempts_not_exceeded(self, mocker):
        """Test _has_exceeded_max_attempts when attempts < max_retries."""
        mock_request = MagicMock()
        mock_request.retries = 3
        mock_request.headers = {"attempts": 4}
        mock_property = PropertyMock(return_value=mock_request)
        mocker.patch.object(SampleTask, "request", mock_property, create=True)
        task = SampleTask()
        assert task._has_exceeded_max_attempts(max_retries=10) is False

    def test_has_exceeded_max_attempts_without_request(self, mocker):
        """Test _has_exceeded_max_attempts when request doesn't exist."""
        task = SampleTask()
        original_hasattr = hasattr

        def mock_hasattr(obj, name):
            if obj is task and name == "request":
                return False
            return original_hasattr(obj, name)

        mocker.patch("builtins.hasattr", side_effect=mock_hasattr)
        assert task._has_exceeded_max_attempts(max_retries=10) is False


class TestBaseCodecovRequest:
    """
    All in all, this is a really weird class

    We are trying here to test some of the hooks celery providers for requests

    It's not easy to generate a situation where they can be called without intensely
        faking the situation

    If you every find a better way to test this, delete this class

    If things start going badly because of those tests, delete this class
    """

    def xRequest(self, mocker, name, celery_app):
        # I dont even know what I am doing here. Just trying to create a sample request Copied from
        # https://github.com/celery/celery/blob/4e4d308db88e60afeec97479a5a133671c671fce/t/unit/worker/test_request.py#L54
        id = None
        args = [1]
        kwargs = {"f": "x"}
        on_ack = mocker.Mock(name="on_ack")
        on_reject = mocker.Mock(name="on_reject")
        message = TaskMessage(name, id, args=args, kwargs=kwargs)
        return BaseCodecovRequest(
            message, app=celery_app, on_ack=on_ack, on_reject=on_reject
        )

    def test_sample_task_timeout(self, celery_app, mocker):
        class SampleTask(BaseCodecovTask, name="test.SampleTask"):
            pass

        DTask = celery_app.register_task(SampleTask())
        request = self.xRequest(mocker, DTask.name, celery_app)
        prom_timeout_counter_before = (
            REGISTRY.get_sample_value(
                "worker_task_counts_timeouts_total", labels={"task": DTask.name}
            )
            or 0
        )
        request.on_timeout(True, 10)
        prom_timeout_counter_after = REGISTRY.get_sample_value(
            "worker_task_counts_timeouts_total", labels={"task": DTask.name}
        )
        assert prom_timeout_counter_after - prom_timeout_counter_before == 1

    def test_sample_task_hard_timeout(self, celery_app, mocker):
        class SampleTask(BaseCodecovTask, name="test.SampleTask"):
            pass

        DTask = celery_app.register_task(SampleTask())
        request = self.xRequest(mocker, DTask.name, celery_app)
        prom_timeout_counter_before = (
            REGISTRY.get_sample_value(
                "worker_task_counts_timeouts_total", labels={"task": DTask.name}
            )
            or 0
        )
        prom_hard_timeout_counter_before = (
            REGISTRY.get_sample_value(
                "worker_task_counts_hard_timeouts_total", labels={"task": DTask.name}
            )
            or 0
        )
        request.on_timeout(False, 10)
        prom_timeout_counter_after = REGISTRY.get_sample_value(
            "worker_task_counts_timeouts_total", labels={"task": DTask.name}
        )
        prom_hard_timeout_counter_after = REGISTRY.get_sample_value(
            "worker_task_counts_hard_timeouts_total", labels={"task": DTask.name}
        )
        assert prom_timeout_counter_after - prom_timeout_counter_before == 1
        assert prom_hard_timeout_counter_after - prom_hard_timeout_counter_before == 1


class TestBaseCodecovTaskApplyAsyncOverride:
    @pytest.fixture
    def fake_owners(self, dbsession):
        owner = OwnerFactory.create(plan=PlanName.CODECOV_PRO_MONTHLY.value)
        owner_enterprise_cloud = OwnerFactory.create(
            plan=PlanName.ENTERPRISE_CLOUD_YEARLY.value
        )
        dbsession.add(owner)
        dbsession.add(owner_enterprise_cloud)
        dbsession.flush()
        return (owner, owner_enterprise_cloud)

    @pytest.fixture
    def fake_repos(self, dbsession, fake_owners):
        (owner, owner_enterprise_cloud) = fake_owners
        repo = RepositoryFactory.create(author=owner)
        repo_enterprise_cloud = RepositoryFactory.create(author=owner_enterprise_cloud)
        dbsession.add(repo)
        dbsession.add(repo_enterprise_cloud)
        dbsession.flush()
        return (repo, repo_enterprise_cloud)

    @pytest.mark.freeze_time("2023-06-13T10:01:01.000123")
    def test_apply_async_override(self, mocker):
        mock_get_db_session = mocker.patch("tasks.base.get_db_session")
        mock_celery_task_router = mocker.patch("tasks.base._get_user_plan_from_task")
        mock_route_tasks = mocker.patch(
            "tasks.base.route_tasks_based_on_user_plan",
            return_value={
                "queue": "some_queue",
                "extra_config": {"soft_timelimit": 200, "hard_timelimit": 400},
            },
        )

        task = BaseCodecovTask()
        task.name = "app.tasks.upload.FakeTask"
        mocked_apply_async = mocker.patch.object(base_celery_app.Task, "apply_async")

        kwargs = {"n": 10}
        task.apply_async(kwargs=kwargs)
        assert mock_get_db_session.call_count == 1
        assert mock_celery_task_router.call_count == 1
        assert mock_route_tasks.call_count == 1
        call_kwargs = mocked_apply_async.call_args[1]
        assert call_kwargs["args"] is None
        assert call_kwargs["kwargs"] == kwargs
        assert (
            call_kwargs["headers"]["created_timestamp"] == "2023-06-13T10:01:01.000123"
        )
        assert call_kwargs["headers"]["attempts"] == 1  # New header added
        assert call_kwargs["time_limit"] == 400
        assert call_kwargs["soft_time_limit"] == 200
        assert call_kwargs["user_plan"] == mock_celery_task_router()

    @pytest.mark.freeze_time("2023-06-13T10:01:01.000123")
    def test_apply_async_override_with_chain(self, mocker):
        mock_get_db_session = mocker.patch("tasks.base.get_db_session")
        mock_celery_task_router = mocker.patch("tasks.base._get_user_plan_from_task")
        mock_route_tasks = mocker.patch(
            "tasks.base.route_tasks_based_on_user_plan",
            return_value={
                "queue": "some_queue",
                "extra_config": {"soft_timelimit": 200, "hard_timelimit": 400},
            },
        )

        task = BaseCodecovTask()
        task.name = "app.tasks.upload.FakeTask"
        mocked_apply_async = mocker.patch.object(base_celery_app.Task, "apply_async")

        chain(
            [task.signature(kwargs={"n": 1}), task.signature(kwargs={"n": 10})]
        ).apply_async()
        assert mock_get_db_session.call_count == 1
        assert mock_celery_task_router.call_count == 1
        assert mock_route_tasks.call_count == 1
        assert mocked_apply_async.call_count == 1
        _, kwargs = mocked_apply_async.call_args
        assert "soft_time_limit" in kwargs and kwargs.get("soft_time_limit") == 200
        assert "time_limit" in kwargs and kwargs.get("time_limit") == 400
        assert "kwargs" in kwargs and kwargs.get("kwargs") == {"n": 1}
        assert "chain" in kwargs and len(kwargs.get("chain")) == 1
        assert "task_id" in kwargs
        assert "headers" in kwargs
        headers = kwargs.get("headers")
        assert headers["created_timestamp"] == "2023-06-13T10:01:01.000123"
        assert headers["attempts"] == 1  # New header added

    @pytest.mark.freeze_time("2023-06-13T10:01:01.000123")
    @pytest.mark.django_db
    def test_real_example_no_override(
        self, mocker, dbsession, mock_configuration, fake_repos
    ):
        mock_all_plans_and_tiers()
        mock_configuration.set_params(
            {
                "setup": {
                    "tasks": {
                        "celery": {
                            "enterprise": {
                                "soft_timelimit": 500,
                                "hard_timelimit": 600,
                            },
                        },
                        "upload": {
                            "enterprise": {"soft_timelimit": 400, "hard_timelimit": 450}
                        },
                    }
                }
            }
        )
        mock_get_db_session = mocker.patch(
            "tasks.base.get_db_session", return_value=dbsession
        )
        task = BaseCodecovTask()
        mocker.patch.object(task, "run", return_value="success")
        task.name = sync_repos_task_name

        mocked_super_apply_async = mocker.patch.object(
            base_celery_app.Task, "apply_async"
        )
        repo, _ = fake_repos

        kwargs = {"ownerid": repo.ownerid}
        task.apply_async(kwargs=kwargs)
        assert mock_get_db_session.call_count == 1
        call_kwargs = mocked_super_apply_async.call_args[1]
        assert call_kwargs["args"] is None
        assert call_kwargs["kwargs"] == kwargs
        assert call_kwargs["soft_time_limit"] is None
        assert (
            call_kwargs["headers"]["created_timestamp"] == "2023-06-13T10:01:01.000123"
        )
        assert call_kwargs["headers"]["attempts"] == 1  # New header added
        assert call_kwargs["time_limit"] is None
        assert call_kwargs["user_plan"] == "users-pr-inappm"

    @pytest.mark.freeze_time("2023-06-13T10:01:01.000123")
    @pytest.mark.django_db
    def test_real_example_override_from_celery(
        self, mocker, dbsession, mock_configuration, fake_repos
    ):
        mock_all_plans_and_tiers()
        mock_configuration.set_params(
            {
                "setup": {
                    "tasks": {
                        "celery": {
                            "enterprise": {
                                "soft_timelimit": 500,
                                "hard_timelimit": 600,
                            },
                        },
                        "upload": {
                            "enterprise": {"soft_timelimit": 400, "hard_timelimit": 450}
                        },
                    }
                }
            }
        )
        mock_get_db_session = mocker.patch(
            "tasks.base.get_db_session", return_value=dbsession
        )
        task = BaseCodecovTask()
        mocker.patch.object(task, "run", return_value="success")
        task.name = sync_repos_task_name

        mocked_super_apply_async = mocker.patch.object(
            base_celery_app.Task, "apply_async"
        )
        _, repo_enterprise_cloud = fake_repos

        kwargs = {"ownerid": repo_enterprise_cloud.ownerid}
        task.apply_async(kwargs=kwargs)
        assert mock_get_db_session.call_count == 1
        call_kwargs = mocked_super_apply_async.call_args[1]
        assert call_kwargs["args"] is None
        assert call_kwargs["kwargs"] == kwargs
        assert call_kwargs["soft_time_limit"] == 500
        assert (
            call_kwargs["headers"]["created_timestamp"] == "2023-06-13T10:01:01.000123"
        )
        assert call_kwargs["headers"]["attempts"] == 1  # New header added
        assert call_kwargs["time_limit"] == 600
        assert call_kwargs["user_plan"] == "users-enterprisey"

    @pytest.mark.freeze_time("2023-06-13T10:01:01.000123")
    @pytest.mark.django_db
    def test_real_example_override_from_upload(
        self, mocker, dbsession, mock_configuration, fake_repos
    ):
        mock_all_plans_and_tiers()
        mock_configuration.set_params(
            {
                "setup": {
                    "tasks": {
                        "celery": {
                            "enterprise": {
                                "soft_timelimit": 500,
                                "hard_timelimit": 600,
                            },
                        },
                        "upload": {
                            "enterprise": {"soft_timelimit": 400, "hard_timelimit": 450}
                        },
                    }
                }
            }
        )
        mock_get_db_session = mocker.patch(
            "tasks.base.get_db_session", return_value=dbsession
        )
        task = BaseCodecovTask()
        mocker.patch.object(task, "run", return_value="success")
        task.name = upload_task_name

        mocked_super_apply_async = mocker.patch.object(
            base_celery_app.Task, "apply_async"
        )
        _, repo_enterprise_cloud = fake_repos

        kwargs = {"repoid": repo_enterprise_cloud.repoid}
        task.apply_async(kwargs=kwargs)
        assert mock_get_db_session.call_count == 1
        call_kwargs = mocked_super_apply_async.call_args[1]
        assert call_kwargs["args"] is None
        assert call_kwargs["kwargs"] == kwargs
        assert call_kwargs["soft_time_limit"] == 400
        assert (
            call_kwargs["headers"]["created_timestamp"] == "2023-06-13T10:01:01.000123"
        )
        assert call_kwargs["headers"]["attempts"] == 1  # New header added
        assert call_kwargs["time_limit"] == 450
        assert call_kwargs["user_plan"] == "users-enterprisey"


@pytest.mark.django_db(databases={"default", "timeseries"})
class TestBaseCodecovTaskWithLoggedLock:
    def test_with_logged_lock_logs_acquiring_and_acquired(self, mocker):
        """Test that with_logged_lock logs 'Acquiring lock' and 'Acquired lock'."""
        mock_log = mocker.patch("tasks.base.log")
        mock_lock = mocker.MagicMock(spec=Lock)
        mock_lock.__enter__ = mocker.MagicMock(return_value=None)
        mock_lock.__exit__ = mocker.MagicMock(return_value=None)

        task = BaseCodecovTask()
        with task.with_logged_lock(mock_lock, lock_name="test_lock", repoid=123):
            pass

        acquiring_calls = [
            call
            for call in mock_log.info.call_args_list
            if call[0][0] == "Acquiring lock"
        ]
        assert len(acquiring_calls) == 1
        assert acquiring_calls[0][1]["extra"]["lock_name"] == "test_lock"
        assert acquiring_calls[0][1]["extra"]["repoid"] == 123

        acquired_calls = [
            call
            for call in mock_log.info.call_args_list
            if call[0][0] == "Acquired lock"
        ]
        assert len(acquired_calls) == 1
        assert acquired_calls[0][1]["extra"]["lock_name"] == "test_lock"
        assert acquired_calls[0][1]["extra"]["repoid"] == 123

    def test_with_logged_lock_logs_releasing_with_duration(self, mocker):
        """Test that with_logged_lock logs 'Releasing lock' with duration."""
        mock_log = mocker.patch("tasks.base.log")
        mock_lock = mocker.MagicMock(spec=Lock)
        mock_lock.__enter__ = mocker.MagicMock(return_value=None)
        mock_lock.__exit__ = mocker.MagicMock(return_value=None)

        # Mock time.time to control duration
        mock_time = mocker.patch("tasks.base.time.time")
        mock_time.side_effect = [1000.0, 1000.5]  # 0.5 second duration

        task = BaseCodecovTask()
        with task.with_logged_lock(mock_lock, lock_name="test_lock", commitid="abc123"):
            pass

        releasing_calls = [
            call
            for call in mock_log.info.call_args_list
            if call[0][0] == "Releasing lock"
        ]
        assert len(releasing_calls) == 1
        assert releasing_calls[0][1]["extra"]["lock_name"] == "test_lock"
        assert releasing_calls[0][1]["extra"]["commitid"] == "abc123"
        # Use approximate comparison due to floating point precision
        assert (
            abs(releasing_calls[0][1]["extra"]["lock_duration_seconds"] - 0.5) < 0.001
        )

    def test_with_logged_lock_includes_extra_context(self, mocker):
        """Test that with_logged_lock includes all extra context in logs."""
        mock_log = mocker.patch("tasks.base.log")
        mock_lock = mocker.MagicMock(spec=Lock)
        mock_lock.__enter__ = mocker.MagicMock(return_value=None)
        mock_lock.__exit__ = mocker.MagicMock(return_value=None)

        task = BaseCodecovTask()
        with task.with_logged_lock(
            mock_lock,
            lock_name="test_lock",
            repoid=123,
            commitid="abc123",
            report_type="coverage",
            custom_field="custom_value",
        ):
            pass

        # Check that all extra context is included in all log calls
        for log_call in mock_log.info.call_args_list:
            extra = log_call[1]["extra"]
            assert extra["lock_name"] == "test_lock"
            assert extra["repoid"] == 123
            assert extra["commitid"] == "abc123"
            assert extra["report_type"] == "coverage"
            assert extra["custom_field"] == "custom_value"

    def test_with_logged_lock_executes_code_within_lock(self, mocker):
        """Test that code within with_logged_lock executes correctly."""
        mock_log = mocker.patch("tasks.base.log")
        mock_lock = mocker.MagicMock(spec=Lock)
        mock_lock.__enter__ = mocker.MagicMock(return_value=None)
        mock_lock.__exit__ = mocker.MagicMock(return_value=None)

        task = BaseCodecovTask()
        result = None
        with task.with_logged_lock(mock_lock, lock_name="test_lock"):
            result = "executed"

        assert result == "executed"
        mock_lock.__enter__.assert_called_once()
        mock_lock.__exit__.assert_called_once()

    def test_with_logged_lock_propagates_lock_error(self, mocker):
        """Test that LockError from lock acquisition is not caught by with_logged_lock."""
        mock_log = mocker.patch("tasks.base.log")
        mock_lock = mocker.MagicMock(spec=Lock)
        mock_lock.__enter__ = mocker.MagicMock(side_effect=LockError("Lock failed"))

        task = BaseCodecovTask()
        with pytest.raises(LockError, match="Lock failed"):
            with task.with_logged_lock(mock_lock, lock_name="test_lock"):
                pass

        # Should have logged "Acquiring lock" but not "Acquired lock" or "Releasing lock"
        acquiring_calls = [
            call
            for call in mock_log.info.call_args_list
            if call[0][0] == "Acquiring lock"
        ]
        assert len(acquiring_calls) == 1

        acquired_calls = [
            call
            for call in mock_log.info.call_args_list
            if call[0][0] == "Acquired lock"
        ]
        assert len(acquired_calls) == 0

        releasing_calls = [
            call
            for call in mock_log.info.call_args_list
            if call[0][0] == "Releasing lock"
        ]
        assert len(releasing_calls) == 0

    def test_with_logged_lock_logs_release_even_on_exception(self, mocker):
        """Test that 'Releasing lock' is logged even if code within raises an exception."""
        mock_log = mocker.patch("tasks.base.log")
        mock_lock = mocker.MagicMock(spec=Lock)
        mock_lock.__enter__ = mocker.MagicMock(return_value=None)
        mock_lock.__exit__ = mocker.MagicMock(return_value=None)

        # Mock time.time to control duration
        mock_time = mocker.patch("tasks.base.time.time")
        mock_time.side_effect = [1000.0, 1000.2]  # 0.2 second duration

        task = BaseCodecovTask()
        with pytest.raises(ValueError):
            with task.with_logged_lock(mock_lock, lock_name="test_lock"):
                raise ValueError("Test exception")

        releasing_calls = [
            call
            for call in mock_log.info.call_args_list
            if call[0][0] == "Releasing lock"
        ]
        assert len(releasing_calls) == 1
        # Use approximate comparison due to floating point precision
        assert (
            abs(releasing_calls[0][1]["extra"]["lock_duration_seconds"] - 0.2) < 0.001
        )
