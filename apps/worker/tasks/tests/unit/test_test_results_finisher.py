from unittest.mock import call

import pytest
from celery.exceptions import Retry

from database.models import CommitReport, Repository
from database.tests.factories import CommitFactory
from services.lock_manager import LockRetry
from shared.celery_config import notify_task_name, upload_breadcrumb_task_name
from shared.django_apps.upload_breadcrumbs.models import (
    BreadcrumbData,
    Errors,
    Milestones,
    ReportTypes,
)
from tasks.test_results_finisher import TestResultsFinisherTask


@pytest.fixture
def mock_self_app(mocker, celery_app):
    """Fixture to mock the celery app for TestResultsFinisherTask."""
    mock_app = celery_app
    mock_app.tasks[upload_breadcrumb_task_name] = mocker.MagicMock()
    mock_app.tasks[notify_task_name] = mocker.MagicMock()
    mock_app.conf = mocker.MagicMock(task_time_limit=123)

    return mocker.patch.object(
        TestResultsFinisherTask,
        "app",
        mock_app,
    )


class TestTestResultsFinisherTask:
    @pytest.mark.django_db
    def test_upload_finisher_ta_finish_upload(self, mocker, dbsession):
        ta_finish_upload = mocker.patch(
            "tasks.test_results_finisher.ta_finish_upload",
            return_value={
                "notify_attempted": True,
                "notify_succeeded": True,
                "queue_notify": False,
            },
        )

        commit = CommitFactory.create(
            message="hello world",
            commitid="cd76b0821854a780b60012aed85af0a8263004ad",
            repository__author__unencrypted_oauth_token="test-token",
            repository__author__username="test-user",
            repository__author__service="github",
            repository__name="test-repo",
        )
        dbsession.add(commit)
        dbsession.flush()

        current_report_row = CommitReport(
            commit_id=commit.id_, report_type="test_results"
        )
        dbsession.add(current_report_row)
        dbsession.flush()

        task = TestResultsFinisherTask()
        result = task.run_impl(
            dbsession,
            False,
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml={},
        )

        assert result == {
            "notify_attempted": True,
            "notify_succeeded": True,
            "queue_notify": False,
        }

        assert ta_finish_upload.call_count == 1
        repo = dbsession.query(Repository).filter_by(repoid=commit.repoid).first()
        assert (
            dbsession,
            repo,
            commit,
            mocker.ANY,
        ) == ta_finish_upload.call_args[0]

    @pytest.mark.django_db
    def test_upload_complete_breadcrumb_logged(self, mocker, dbsession, mock_self_app):
        """Test that UPLOAD_COMPLETE breadcrumb is logged after successful processing."""
        mocker.patch(
            "tasks.test_results_finisher.ta_finish_upload",
            return_value={
                "notify_attempted": True,
                "notify_succeeded": True,
                "queue_notify": False,
            },
        )

        commit = CommitFactory.create(
            message="hello world",
            commitid="cd76b0821854a780b60012aed85af0a8263004ad",
            repository__author__unencrypted_oauth_token="test-token",
            repository__author__username="test-user",
            repository__author__service="github",
            repository__name="test-repo",
        )
        dbsession.add(commit)
        dbsession.flush()

        current_report_row = CommitReport(
            commit_id=commit.id_, report_type="test_results"
        )
        dbsession.add(current_report_row)
        dbsession.flush()

        task = TestResultsFinisherTask()
        task.run_impl(
            dbsession,
            False,
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml={},
        )

        # Verify UPLOAD_COMPLETE breadcrumb was logged
        mock_self_app.tasks[upload_breadcrumb_task_name].apply_async.assert_called_with(
            kwargs={
                "commit_sha": commit.commitid,
                "repo_id": commit.repoid,
                "breadcrumb_data": BreadcrumbData(
                    milestone=Milestones.UPLOAD_COMPLETE,
                    report_type=ReportTypes.TEST_RESULTS,
                ),
                "upload_ids": [],
                "sentry_trace_id": None,
            }
        )

    @pytest.mark.django_db
    def test_notifications_triggered_breadcrumb_logged(
        self, mocker, dbsession, mock_self_app
    ):
        """Test that NOTIFICATIONS_TRIGGERED breadcrumb is logged when queue_notify is True."""
        mocker.patch(
            "tasks.test_results_finisher.ta_finish_upload",
            return_value={
                "notify_attempted": True,
                "notify_succeeded": True,
                "queue_notify": True,  # This triggers notifications
            },
        )

        commit = CommitFactory.create(
            message="hello world",
            commitid="cd76b0821854a780b60012aed85af0a8263004ad",
            repository__author__unencrypted_oauth_token="test-token",
            repository__author__username="test-user",
            repository__author__service="github",
            repository__name="test-repo",
        )
        dbsession.add(commit)
        dbsession.flush()

        current_report_row = CommitReport(
            commit_id=commit.id_, report_type="test_results"
        )
        dbsession.add(current_report_row)
        dbsession.flush()

        task = TestResultsFinisherTask()
        task.run_impl(
            dbsession,
            False,
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml={},
        )

        # Verify both UPLOAD_COMPLETE and NOTIFICATIONS_TRIGGERED breadcrumbs were logged
        mock_self_app.tasks[upload_breadcrumb_task_name].apply_async.assert_has_calls(
            [
                call(
                    kwargs={
                        "commit_sha": commit.commitid,
                        "repo_id": commit.repoid,
                        "breadcrumb_data": BreadcrumbData(
                            milestone=Milestones.UPLOAD_COMPLETE,
                            report_type=ReportTypes.TEST_RESULTS,
                        ),
                        "upload_ids": [],
                        "sentry_trace_id": None,
                    }
                ),
                call(
                    kwargs={
                        "commit_sha": commit.commitid,
                        "repo_id": commit.repoid,
                        "breadcrumb_data": BreadcrumbData(
                            milestone=Milestones.NOTIFICATIONS_TRIGGERED,
                            report_type=ReportTypes.TEST_RESULTS,
                        ),
                        "upload_ids": [],
                        "sentry_trace_id": None,
                    }
                ),
            ]
        )

        # Verify notify task was queued
        mock_self_app.tasks[notify_task_name].apply_async.assert_called_once()

    @pytest.mark.django_db
    def test_lock_retry_breadcrumbs_logged(self, mocker, dbsession, mock_self_app):
        """Test that INTERNAL_LOCK_ERROR and INTERNAL_RETRYING breadcrumbs are logged on LockRetry."""
        mocker.patch(
            "tasks.test_results_finisher.LockManager.locked",
            side_effect=LockRetry(countdown=60),
        )

        commit = CommitFactory.create(
            message="hello world",
            commitid="cd76b0821854a780b60012aed85af0a8263004ad",
            repository__author__unencrypted_oauth_token="test-token",
            repository__author__username="test-user",
            repository__author__service="github",
            repository__name="test-repo",
        )
        dbsession.add(commit)
        dbsession.flush()

        current_report_row = CommitReport(
            commit_id=commit.id_, report_type="test_results"
        )
        dbsession.add(current_report_row)
        dbsession.flush()

        task = TestResultsFinisherTask()
        task.request.retries = 0

        with pytest.raises(Retry):
            task.run_impl(
                dbsession,
                False,
                repoid=commit.repoid,
                commitid=commit.commitid,
                commit_yaml={},
            )

        # Verify both INTERNAL_LOCK_ERROR and INTERNAL_RETRYING breadcrumbs were logged
        mock_self_app.tasks[upload_breadcrumb_task_name].apply_async.assert_has_calls(
            [
                call(
                    kwargs={
                        "commit_sha": commit.commitid,
                        "repo_id": commit.repoid,
                        "breadcrumb_data": BreadcrumbData(
                            error=Errors.INTERNAL_LOCK_ERROR,
                            report_type=ReportTypes.TEST_RESULTS,
                        ),
                        "upload_ids": [],
                        "sentry_trace_id": None,
                    }
                ),
                call(
                    kwargs={
                        "commit_sha": commit.commitid,
                        "repo_id": commit.repoid,
                        "breadcrumb_data": BreadcrumbData(
                            error=Errors.INTERNAL_RETRYING,
                            report_type=ReportTypes.TEST_RESULTS,
                        ),
                        "upload_ids": [],
                        "sentry_trace_id": None,
                    }
                ),
            ]
        )

    @pytest.mark.django_db
    def test_unknown_error_breadcrumb_logged(self, mocker, dbsession, mock_self_app):
        """Test that UNKNOWN error breadcrumb is logged on generic Exception."""
        mocker.patch(
            "tasks.test_results_finisher.ta_finish_upload",
            side_effect=ValueError("Something went wrong"),
        )

        commit = CommitFactory.create(
            message="hello world",
            commitid="cd76b0821854a780b60012aed85af0a8263004ad",
            repository__author__unencrypted_oauth_token="test-token",
            repository__author__username="test-user",
            repository__author__service="github",
            repository__name="test-repo",
        )
        dbsession.add(commit)
        dbsession.flush()

        current_report_row = CommitReport(
            commit_id=commit.id_, report_type="test_results"
        )
        dbsession.add(current_report_row)
        dbsession.flush()

        task = TestResultsFinisherTask()

        with pytest.raises(ValueError, match="Something went wrong"):
            task.run_impl(
                dbsession,
                False,
                repoid=commit.repoid,
                commitid=commit.commitid,
                commit_yaml={},
            )

        # Verify UNKNOWN error breadcrumb was logged with error text
        mock_self_app.tasks[
            upload_breadcrumb_task_name
        ].apply_async.assert_called_once_with(
            kwargs={
                "commit_sha": commit.commitid,
                "repo_id": commit.repoid,
                "breadcrumb_data": BreadcrumbData(
                    error=Errors.UNKNOWN,
                    error_text="ValueError('Something went wrong')",
                    report_type=ReportTypes.TEST_RESULTS,
                ),
                "upload_ids": [],
                "sentry_trace_id": None,
            }
        )
