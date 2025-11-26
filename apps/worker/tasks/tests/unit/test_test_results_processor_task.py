from pathlib import Path
from unittest.mock import call

import pytest

from database.models import CommitReport
from database.tests.factories import CommitFactory, UploadFactory
from shared.celery_config import upload_breadcrumb_task_name
from shared.django_apps.upload_breadcrumbs.models import (
    BreadcrumbData,
    Errors,
    Milestones,
    ReportTypes,
)
from tasks.test_results_processor import TestResultsProcessorTask

here = Path(__file__)


@pytest.fixture
def mock_self_app(mocker, celery_app):
    """Fixture to mock the celery app for TestResultsProcessorTask."""
    mock_app = celery_app
    mock_app.tasks[upload_breadcrumb_task_name] = mocker.MagicMock()
    mock_app.conf = mocker.MagicMock(task_time_limit=123)

    return mocker.patch.object(
        TestResultsProcessorTask,
        "app",
        mock_app,
    )


@pytest.mark.integration
def test_test_results_processor_ta_processor_fail(
    mocker,
    mock_configuration,
    dbsession,
    codecov_vcr,
    mock_storage,
    mock_redis,
    celery_app,
):
    url = "whatever.txt"
    with open(here.parent.parent / "samples" / "sample_test.json") as f:
        content = f.read()
        mock_storage.write_file("archive", url, content)
    upload = UploadFactory.create(storage_path=url)
    dbsession.add(upload)
    dbsession.flush()
    redis_queue = [{"url": url, "upload_id": upload.id_}]
    mocker.patch.object(TestResultsProcessorTask, "app", celery_app)

    mocker.patch(
        "tasks.test_results_processor.ta_processor",
        side_effect=Exception("test"),
    )

    commit = CommitFactory.create(
        message="hello world",
        commitid="cd76b0821854a780b60012aed85af0a8263004ad",
        repository__author__unencrypted_oauth_token="test7lk5ndmtqzxlx06rip65nac9c7epqopclnoy",
        repository__author__username="joseph-sentry",
        repository__author__service="github",
        repository__name="codecov-demo",
    )
    dbsession.add(commit)
    dbsession.flush()

    current_report_row = CommitReport(commit_id=commit.id_)
    dbsession.add(current_report_row)
    dbsession.flush()

    with pytest.raises(Exception, match="test"):
        TestResultsProcessorTask().run_impl(
            dbsession,
            previous_result=False,
            repoid=upload.report.commit.repoid,
            commitid=commit.commitid,
            commit_yaml={},
            arguments_list=redis_queue,
        )


class TestTestResultsProcessorBreadcrumbs:
    @pytest.mark.django_db
    def test_processing_upload_breadcrumb_logged(
        self, mocker, dbsession, mock_self_app
    ):
        """Test that PROCESSING_UPLOAD breadcrumb is logged at the start of processing."""
        mocker.patch(
            "tasks.test_results_processor.ta_processor",
            return_value=None,
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

        current_report_row = CommitReport(commit_id=commit.id_)
        dbsession.add(current_report_row)
        dbsession.flush()

        upload = UploadFactory.create(report=current_report_row, storage_path="url")
        dbsession.add(upload)
        dbsession.flush()

        arguments_list = [{"url": "some_url", "upload_id": upload.id_}]

        task = TestResultsProcessorTask()
        result = task.run_impl(
            dbsession,
            previous_result=False,
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml={},
            arguments_list=arguments_list,
        )

        assert result is True

        # Verify PROCESSING_UPLOAD breadcrumb was logged
        mock_self_app.tasks[
            upload_breadcrumb_task_name
        ].apply_async.assert_called_once_with(
            kwargs={
                "commit_sha": commit.commitid,
                "repo_id": commit.repoid,
                "breadcrumb_data": BreadcrumbData(
                    milestone=Milestones.PROCESSING_UPLOAD,
                    report_type=ReportTypes.TEST_RESULTS,
                ),
                "upload_ids": [upload.id_],
                "sentry_trace_id": None,
            }
        )

    @pytest.mark.django_db
    def test_unknown_error_breadcrumb_logged(self, mocker, dbsession, mock_self_app):
        """Test that UNKNOWN error breadcrumb is logged on generic Exception."""
        mocker.patch(
            "tasks.test_results_processor.ta_processor",
            side_effect=ValueError("Processing failed"),
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

        current_report_row = CommitReport(commit_id=commit.id_)
        dbsession.add(current_report_row)
        dbsession.flush()

        upload = UploadFactory.create(report=current_report_row, storage_path="url")
        dbsession.add(upload)
        dbsession.flush()

        arguments_list = [{"url": "some_url", "upload_id": upload.id_}]

        task = TestResultsProcessorTask()

        with pytest.raises(ValueError, match="Processing failed"):
            task.run_impl(
                dbsession,
                previous_result=False,
                repoid=commit.repoid,
                commitid=commit.commitid,
                commit_yaml={},
                arguments_list=arguments_list,
            )

        # Verify both PROCESSING_UPLOAD (at start) and error breadcrumbs were logged
        mock_self_app.tasks[upload_breadcrumb_task_name].apply_async.assert_has_calls(
            [
                call(
                    kwargs={
                        "commit_sha": commit.commitid,
                        "repo_id": commit.repoid,
                        "breadcrumb_data": BreadcrumbData(
                            milestone=Milestones.PROCESSING_UPLOAD,
                            report_type=ReportTypes.TEST_RESULTS,
                        ),
                        "upload_ids": [upload.id_],
                        "sentry_trace_id": None,
                    }
                ),
                call(
                    kwargs={
                        "commit_sha": commit.commitid,
                        "repo_id": commit.repoid,
                        "breadcrumb_data": BreadcrumbData(
                            milestone=Milestones.PROCESSING_UPLOAD,
                            error=Errors.UNKNOWN,
                            error_text="ValueError('Processing failed')",
                            report_type=ReportTypes.TEST_RESULTS,
                        ),
                        "upload_ids": [upload.id_],
                        "sentry_trace_id": None,
                    }
                ),
            ]
        )

    @pytest.mark.django_db
    def test_processing_upload_breadcrumb_with_multiple_uploads(
        self, mocker, dbsession, mock_self_app
    ):
        """Test that PROCESSING_UPLOAD breadcrumb includes all upload_ids."""
        mocker.patch(
            "tasks.test_results_processor.ta_processor",
            return_value=None,
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

        current_report_row = CommitReport(commit_id=commit.id_)
        dbsession.add(current_report_row)
        dbsession.flush()

        upload1 = UploadFactory.create(report=current_report_row, storage_path="url1")
        upload2 = UploadFactory.create(report=current_report_row, storage_path="url2")
        dbsession.add(upload1)
        dbsession.add(upload2)
        dbsession.flush()

        arguments_list = [
            {"url": "url1", "upload_id": upload1.id_},
            {"url": "url2", "upload_id": upload2.id_},
        ]

        task = TestResultsProcessorTask()
        result = task.run_impl(
            dbsession,
            previous_result=False,
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml={},
            arguments_list=arguments_list,
        )

        assert result is True

        # Verify PROCESSING_UPLOAD breadcrumb includes both upload_ids
        mock_self_app.tasks[
            upload_breadcrumb_task_name
        ].apply_async.assert_called_once_with(
            kwargs={
                "commit_sha": commit.commitid,
                "repo_id": commit.repoid,
                "breadcrumb_data": BreadcrumbData(
                    milestone=Milestones.PROCESSING_UPLOAD,
                    report_type=ReportTypes.TEST_RESULTS,
                ),
                "upload_ids": [upload1.id_, upload2.id_],
                "sentry_trace_id": None,
            }
        )
