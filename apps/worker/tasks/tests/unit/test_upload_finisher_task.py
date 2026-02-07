from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import ANY, call

import pytest
from celery.exceptions import Retry, SoftTimeLimitExceeded

from celery_config import notify_error_task_name
from shared.django_apps.enums import ReportType
from database.models.reports import CommitReport, Upload
from database.tests.factories import CommitFactory, PullFactory, RepositoryFactory
from database.tests.factories.core import UploadFactory
from database.tests.factories.timeseries import DatasetFactory
from helpers.checkpoint_logger import _kwargs_key
from helpers.checkpoint_logger.flows import UploadFlow
from helpers.exceptions import RepositoryWithoutValidBotError
from helpers.log_context import LogContext, set_log_context
from services.lock_manager import LockRetry
from services.processing.intermediate import intermediate_report_key
from services.processing.merging import get_joined_flag, update_uploads
from services.processing.types import MergeResult, ProcessingResult
from services.timeseries import MeasurementName
from shared.celery_config import (
    compute_comparison_task_name,
    notify_task_name,
    pulls_task_name,
    timeseries_save_commit_measurements_task_name,
    upload_breadcrumb_task_name,
)
from shared.django_apps.upload_breadcrumbs.models import (
    BreadcrumbData,
    Errors,
    Milestones,
)
from shared.reports.enums import UploadState
from shared.torngit.exceptions import TorngitObjectNotFoundError
from shared.yaml import UserYaml
from tasks.upload_finisher import (
    ReportService,
    ShouldCallNotifyResult,
    UploadFinisherTask,
    load_commit_diff,
)

here = Path(__file__)


@pytest.fixture
def mock_self_app(mocker, celery_app):
    mock_app = celery_app
    mock_app.tasks[upload_breadcrumb_task_name] = mocker.MagicMock()
    mock_app.tasks[notify_task_name] = mocker.MagicMock()
    mock_app.tasks[notify_error_task_name] = mocker.MagicMock()
    mock_app.tasks[pulls_task_name] = mocker.MagicMock()
    mock_app.tasks[compute_comparison_task_name] = mocker.MagicMock()
    mock_app.tasks[timeseries_save_commit_measurements_task_name] = mocker.MagicMock()
    mock_app.conf = mocker.MagicMock(task_time_limit=123)
    mock_app.send_task = mocker.MagicMock()

    return mocker.patch.object(
        UploadFinisherTask,
        "app",
        mock_app,
    )


def _start_upload_flow(mocker):
    mocker.patch(
        "helpers.checkpoint_logger._get_milli_timestamp",
        side_effect=[1337, 9001, 10000, 15000, 20000, 25000],
    )
    set_log_context(LogContext())
    UploadFlow.log(UploadFlow.UPLOAD_TASK_BEGIN)
    UploadFlow.log(UploadFlow.PROCESSING_BEGIN)
    UploadFlow.log(UploadFlow.INITIAL_PROCESSING_COMPLETE)


def test_load_commit_diff_no_diff(mock_configuration, dbsession, mock_repo_provider):
    commit = CommitFactory.create()
    dbsession.add(commit)
    dbsession.flush()
    mock_repo_provider.get_commit_diff.side_effect = TorngitObjectNotFoundError(
        "response", "message"
    )
    diff = load_commit_diff(commit)
    assert diff is None


def test_load_commit_diff_no_bot(mocker, mock_configuration, dbsession):
    commit = CommitFactory.create()
    dbsession.add(commit)
    dbsession.flush()
    mock_get_repo_service = mocker.patch(
        "tasks.upload_finisher.get_repo_provider_service"
    )
    mock_get_repo_service.side_effect = RepositoryWithoutValidBotError()
    diff = load_commit_diff(commit)
    assert diff is None


def test_mark_uploads_as_failed(dbsession):
    commit = CommitFactory.create()
    dbsession.add(commit)
    dbsession.flush()
    report = CommitReport(commit_id=commit.id_)
    dbsession.add(report)
    dbsession.flush()
    upload_1 = UploadFactory.create(report=report, state="started", storage_path="url")
    upload_2 = UploadFactory.create(report=report, state="started", storage_path="url2")
    dbsession.add(upload_1)
    dbsession.add(upload_2)
    dbsession.flush()

    results: list[ProcessingResult] = [
        {
            "upload_id": upload_1.id,
            "successful": False,
            "error": {"code": "report_empty", "params": {}},
        },
        {
            "upload_id": upload_2.id,
            "successful": False,
            "error": {"code": "report_expired", "params": {}},
        },
    ]

    update_uploads(dbsession, UserYaml({}), results, [], MergeResult({}, set()))
    dbsession.expire_all()

    assert upload_1.state == "error"
    assert len(upload_1.errors) == 1
    assert upload_1.errors[0].error_code == "report_empty"
    assert upload_1.errors[0].error_params == {}
    assert upload_1.errors[0].report_upload == upload_1

    assert upload_2.state == "error"
    assert len(upload_2.errors) == 1
    assert upload_2.errors[0].error_code == "report_expired"
    assert upload_2.errors[0].error_params == {}
    assert upload_2.errors[0].report_upload == upload_2


@pytest.mark.parametrize(
    "flag, joined",
    [("nightly", False), ("unittests", True), ("ui", True), ("other", True)],
)
def test_not_joined_flag(flag, joined):
    yaml = UserYaml(
        {
            "flags": {
                "nightly": {"joined": False},
                "unittests": {"joined": True},
                "ui": {"paths": ["ui/"]},
            }
        }
    )
    assert get_joined_flag(yaml, [flag]) == joined


class TestUploadFinisherTask:
    @pytest.mark.django_db
    def test_upload_finisher_task_call(
        self,
        mocker,
        mock_configuration,
        dbsession,
        codecov_vcr,
        mock_storage,
        mock_checkpoint_submit,
        mock_repo_provider,
        mock_redis,
        mock_self_app,
    ):
        mock_redis.scard.return_value = 0
        mocker.patch("tasks.upload_finisher.load_intermediate_reports", return_value=[])
        mocker.patch("tasks.upload_finisher.update_uploads")
        url = "v4/raw/2019-05-22/C3C4715CA57C910D11D5EB899FC86A7E/4c4e4654ac25037ae869caeb3619d485970b6304/a84d445c-9c1e-434f-8275-f18f1f320f81.txt"

        commit = CommitFactory.create(
            message="dsidsahdsahdsa",
            commitid="abf6d4df662c47e32460020ab14abf9303581429",
            branch="thisbranch",
            ci_passed=True,
            repository__branch="thisbranch",
            repository__updatestamp=None,
            repository__author__unencrypted_oauth_token="testulk3d54rlhxkjyzomq2wh8b7np47xabcrkx8",
            repository__author__username="ThiagoCodecov",
            repository__author__service="github",
            author__service="github",
            notified=True,
            repository__yaml={
                "codecov": {"max_report_age": "1y ago"}
            },  # Sorry, this is a timebomb now
        )
        dbsession.add(commit)
        dbsession.flush()
        previous_results = [
            {"upload_id": 0, "arguments": {"url": url}, "successful": True}
        ]

        _start_upload_flow(mocker)
        result = UploadFinisherTask().run_impl(
            dbsession,
            previous_results,
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml={},
        )

        assert result == {"notifications_called": True}
        dbsession.refresh(commit)
        assert commit.message == "dsidsahdsahdsa"

        # Verify repository timestamp is updated
        dbsession.refresh(commit.repository)
        assert commit.repository.updatestamp is not None
        assert (datetime.now(tz=UTC) - commit.repository.updatestamp).seconds < 60

        mock_checkpoint_submit.assert_any_call(
            "batch_processing_duration",
            UploadFlow.INITIAL_PROCESSING_COMPLETE,
            UploadFlow.BATCH_PROCESSING_COMPLETE,
            data={
                UploadFlow.UPLOAD_TASK_BEGIN: 1337,
                UploadFlow.PROCESSING_BEGIN: 9001,
                UploadFlow.INITIAL_PROCESSING_COMPLETE: 10000,
                UploadFlow.BATCH_PROCESSING_COMPLETE: 15000,
                UploadFlow.PROCESSING_COMPLETE: 20000,
            },
        )
        mock_checkpoint_submit.assert_any_call(
            "total_processing_duration",
            UploadFlow.PROCESSING_BEGIN,
            UploadFlow.PROCESSING_COMPLETE,
            data={
                UploadFlow.UPLOAD_TASK_BEGIN: 1337,
                UploadFlow.PROCESSING_BEGIN: 9001,
                UploadFlow.INITIAL_PROCESSING_COMPLETE: 10000,
                UploadFlow.BATCH_PROCESSING_COMPLETE: 15000,
                UploadFlow.PROCESSING_COMPLETE: 20000,
            },
        )
        mock_self_app.tasks[upload_breadcrumb_task_name].apply_async.assert_has_calls(
            [
                call(
                    kwargs={
                        "commit_sha": commit.commitid,
                        "repo_id": commit.repoid,
                        "breadcrumb_data": BreadcrumbData(
                            milestone=Milestones.NOTIFICATIONS_TRIGGERED,
                        ),
                        "upload_ids": [0],
                        "sentry_trace_id": None,
                    }
                ),
                call(
                    kwargs={
                        "commit_sha": commit.commitid,
                        "repo_id": commit.repoid,
                        "breadcrumb_data": BreadcrumbData(
                            milestone=Milestones.UPLOAD_COMPLETE,
                        ),
                        "upload_ids": [0],
                        "sentry_trace_id": None,
                    }
                ),
            ]
        )

    @pytest.mark.django_db
    def test_upload_finisher_task_call_no_author(
        self,
        mocker,
        mock_configuration,
        dbsession,
        mock_storage,
        mock_repo_provider,
        mock_self_app,
    ):
        mocker.patch("tasks.upload_finisher.load_intermediate_reports", return_value=[])
        mocker.patch("tasks.upload_finisher.update_uploads")
        url = "v4/raw/2019-05-22/C3C4715CA57C910D11D5EB899FC86A7E/4c4e4654ac25037ae869caeb3619d485970b6304/a84d445c-9c1e-434f-8275-f18f1f320f81.txt"

        commit = CommitFactory.create(
            message="dsidsahdsahdsa",
            author=None,
            branch="thisbranch",
            ci_passed=True,
            repository__branch="thisbranch",
            repository__author__username="ThiagoCodecov",
            repository__yaml={
                "codecov": {"max_report_age": "1y ago"}
            },  # Sorry, this is a timebomb now
        )
        dbsession.add(commit)
        dbsession.flush()
        previous_results = [
            {"upload_id": 0, "arguments": {"url": url}, "successful": True}
        ]
        result = UploadFinisherTask().run_impl(
            dbsession,
            previous_results,
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml={},
        )
        expected_result = {"notifications_called": True}
        assert expected_result == result
        dbsession.refresh(commit)
        assert commit.message == "dsidsahdsahdsa"
        mock_self_app.tasks[upload_breadcrumb_task_name].apply_async.assert_has_calls(
            [
                call(
                    kwargs={
                        "commit_sha": commit.commitid,
                        "repo_id": commit.repoid,
                        "breadcrumb_data": BreadcrumbData(
                            milestone=Milestones.NOTIFICATIONS_TRIGGERED,
                        ),
                        "upload_ids": [0],
                        "sentry_trace_id": None,
                    }
                ),
                call(
                    kwargs={
                        "commit_sha": commit.commitid,
                        "repo_id": commit.repoid,
                        "breadcrumb_data": BreadcrumbData(
                            milestone=Milestones.UPLOAD_COMPLETE,
                        ),
                        "upload_ids": [0],
                        "sentry_trace_id": None,
                    }
                ),
            ]
        )

    @pytest.mark.django_db
    def test_upload_finisher_task_call_different_branch(
        self,
        mocker,
        mock_configuration,
        dbsession,
        mock_storage,
        mock_repo_provider,
        mock_self_app,
    ):
        mocker.patch("tasks.upload_finisher.load_intermediate_reports", return_value=[])
        mocker.patch("tasks.upload_finisher.update_uploads")
        url = "v4/raw/2019-05-22/C3C4715CA57C910D11D5EB899FC86A7E/4c4e4654ac25037ae869caeb3619d485970b6304/a84d445c-9c1e-434f-8275-f18f1f320f81.txt"

        commit = CommitFactory.create(
            message="dsidsahdsahdsa",
            branch="other_branch",
            ci_passed=True,
            repository__branch="thisbranch",
            repository__author__username="ThiagoCodecov",
            repository__yaml={
                "codecov": {"max_report_age": "1y ago"}
            },  # Sorry, this is a timebomb now
        )
        dbsession.add(commit)
        dbsession.flush()
        previous_results = [
            {"upload_id": 0, "arguments": {"url": url}, "successful": True}
        ]
        result = UploadFinisherTask().run_impl(
            dbsession,
            previous_results,
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml={},
        )
        expected_result = {"notifications_called": True}
        assert expected_result == result
        dbsession.refresh(commit)
        assert commit.message == "dsidsahdsahdsa"
        mock_self_app.tasks[upload_breadcrumb_task_name].apply_async.assert_has_calls(
            [
                call(
                    kwargs={
                        "commit_sha": commit.commitid,
                        "repo_id": commit.repoid,
                        "breadcrumb_data": BreadcrumbData(
                            milestone=Milestones.NOTIFICATIONS_TRIGGERED,
                        ),
                        "upload_ids": [0],
                        "sentry_trace_id": None,
                    }
                ),
                call(
                    kwargs={
                        "commit_sha": commit.commitid,
                        "repo_id": commit.repoid,
                        "breadcrumb_data": BreadcrumbData(
                            milestone=Milestones.UPLOAD_COMPLETE,
                        ),
                        "upload_ids": [0],
                        "sentry_trace_id": None,
                    }
                ),
            ]
        )

    def test_should_call_notifications(self, dbsession, mocker):
        commit_yaml = {"codecov": {"max_report_age": "1y ago"}}
        commit = CommitFactory.create(
            message="dsidsahdsahdsa",
            commitid="abf6d4df662c47e32460020ab14abf9303581429",
            repository__author__unencrypted_oauth_token="testulk3d54rlhxkjyzomq2wh8b7np47xabcrkx8",
            repository__author__username="ThiagoCodecov",
            repository__yaml=commit_yaml,
        )
        dbsession.add(commit)
        dbsession.flush()

        assert (
            UploadFinisherTask().should_call_notifications(
                commit,
                commit_yaml,
                [{"arguments": {"url": "url"}, "successful": True}],
            )
            == ShouldCallNotifyResult.NOTIFY
        )

    def test_should_call_notifications_manual_trigger(self, dbsession):
        commit_yaml = {"codecov": {"notify": {"manual_trigger": True}}}
        commit = CommitFactory.create(
            message="dsidsahdsahdsa",
            commitid="abf6d4df662c47e32460020ab14abf9303581429",
            repository__author__unencrypted_oauth_token="aabbcc",
            repository__author__username="Codecov",
            repository__yaml=commit_yaml,
        )
        dbsession.add(commit)
        dbsession.flush()

        assert (
            UploadFinisherTask().should_call_notifications(commit, commit_yaml, [])
            == ShouldCallNotifyResult.DO_NOT_NOTIFY
        )

    def test_should_call_notifications_manual_trigger_off(self, dbsession, mocker):
        commit_yaml = {
            "codecov": {"max_report_age": "1y ago", "notify": {"manual_trigger": False}}
        }
        commit = CommitFactory.create(
            message="dsidsahdsahdsa",
            commitid="abf6d4df662c47e32460020ab14abf9303581429",
            repository__author__unencrypted_oauth_token="testulk3d54rlhxkjyzomq2wh8b7np47xabcrkx8",
            repository__author__username="ThiagoCodecov",
            repository__yaml=commit_yaml,
        )
        dbsession.add(commit)
        dbsession.flush()

        assert (
            UploadFinisherTask().should_call_notifications(
                commit,
                commit_yaml,
                [{"arguments": {"url": "url"}, "successful": True}],
            )
            == ShouldCallNotifyResult.NOTIFY
        )

    @pytest.mark.parametrize(
        "notify_error,result",
        [
            (True, ShouldCallNotifyResult.NOTIFY_ERROR),
            (False, ShouldCallNotifyResult.DO_NOT_NOTIFY),
        ],
    )
    def test_should_call_notifications_no_successful_reports(
        self, dbsession, mocker, notify_error, result
    ):
        commit_yaml = {
            "codecov": {
                "max_report_age": "1y ago",
                "notify": {"notify_error": notify_error},
            }
        }
        commit = CommitFactory.create(
            message="dsidsahdsahdsa",
            commitid="abf6d4df662c47e32460020ab14abf9303581429",
            repository__author__unencrypted_oauth_token="testulk3d54rlhxkjyzomq2wh8b7np47xabcrkx8",
            repository__author__username="ThiagoCodecov",
            repository__yaml=commit_yaml,
        )
        dbsession.add(commit)
        dbsession.flush()

        assert (
            UploadFinisherTask().should_call_notifications(
                commit,
                commit_yaml,
                12 * [{"arguments": {"url": "url"}, "successful": False}],
            )
            == result
        )

    def test_should_call_notifications_not_enough_builds(self, dbsession, mocker):
        commit_yaml = {"codecov": {"notify": {"after_n_builds": 9}}}
        commit = CommitFactory.create(
            message="dsidsahdsahdsa",
            commitid="abf6d4df662c47e32460020ab14abf9303581429",
            repository__author__unencrypted_oauth_token="testulk3d54rlhxkjyzomq2wh8b7np47xabcrkx8",
            repository__author__username="ThiagoCodecov",
            repository__yaml=commit_yaml,
        )
        dbsession.add(commit)

        mocked_report = mocker.patch.object(
            ReportService, "get_existing_report_for_commit"
        )
        mocked_report.return_value = mocker.MagicMock(
            sessions=[mocker.MagicMock()] * 8
        )  # 8 sessions

        assert (
            UploadFinisherTask().should_call_notifications(
                commit,
                commit_yaml,
                9 * [{"arguments": {"url": "url"}, "successful": True}],
            )
            == ShouldCallNotifyResult.DO_NOT_NOTIFY
        )

    def test_should_call_notifications_more_than_enough_builds(self, dbsession, mocker):
        commit_yaml = {"codecov": {"notify": {"after_n_builds": 9}}}
        commit = CommitFactory.create(
            message="dsidsahdsahdsa",
            commitid="abf6d4df662c47e32460020ab14abf9303581429",
            repository__author__unencrypted_oauth_token="testulk3d54rlhxkjyzomq2wh8b7np47xabcrkx8",
            repository__author__username="ThiagoCodecov",
            repository__yaml=commit_yaml,
        )
        dbsession.add(commit)

        mocked_report = mocker.patch.object(
            ReportService, "get_existing_report_for_commit"
        )
        mocked_report.return_value = mocker.MagicMock(
            sessions=[mocker.MagicMock()] * 10
        )  # 10 sessions

        assert (
            UploadFinisherTask().should_call_notifications(
                commit,
                commit_yaml,
                2 * [{"arguments": {"url": "url"}, "successful": True}],
            )
            == ShouldCallNotifyResult.NOTIFY
        )

    def test_should_call_notifications_with_pending_uploads_in_db(
        self, dbsession, mocker
    ):
        """Test that notifications are not called when DB shows pending uploads"""
        commit_yaml = {"codecov": {"max_report_age": "1y ago"}}
        commit = CommitFactory.create(
            message="dsidsahdsahdsa",
            commitid="abf6d4df662c47e32460020ab14abf9303581429",
            repository__author__unencrypted_oauth_token="testulk3d54rlhxkjyzomq2wh8b7np47xabcrkx8",
            repository__author__username="ThiagoCodecov",
            repository__yaml=commit_yaml,
        )
        # Create coverage uploads in UPLOADED state (still being processed)
        # These should block notifications since they're coverage uploads
        upload1 = UploadFactory.create(
            report__commit=commit,
            report__report_type=ReportType.COVERAGE.value,
            state="started",
            state_id=UploadState.UPLOADED.db_id,
        )
        upload2 = UploadFactory.create(
            report__commit=commit,
            report__report_type=ReportType.COVERAGE.value,
            state="started",
            state_id=UploadState.UPLOADED.db_id,
        )
        dbsession.add(commit)
        dbsession.add(upload1)
        dbsession.add(upload2)
        dbsession.flush()

        assert (
            UploadFinisherTask().should_call_notifications(
                commit,
                commit_yaml,
                [{"arguments": {"url": "url"}, "successful": True}],
                db_session=dbsession,
            )
            == ShouldCallNotifyResult.DO_NOT_NOTIFY
        )

    def test_finish_reports_processing(self, dbsession, mocker, mock_self_app):
        commit_yaml = {}
        commit = CommitFactory.create(
            message="dsidsahdsahdsa",
            commitid="abf6d4df662c47e32460020ab14abf9303581429",
            repository__author__unencrypted_oauth_token="testulk3d54rlhxkjyzomq2wh8b7np47xabcrkx8",
            repository__author__username="ThiagoCodecov",
            repository__yaml=commit_yaml,
        )
        dbsession.add(commit)
        dbsession.flush()

        _start_upload_flow(mocker)
        res = UploadFinisherTask().finish_reports_processing(
            dbsession,
            commit,
            UserYaml(commit_yaml),
            [{"upload_id": 1, "successful": True}],
        )
        assert res == {"notifications_called": True}
        mock_self_app.tasks[notify_task_name].apply_async.assert_called_with(
            kwargs={
                "commitid": commit.commitid,
                "current_yaml": commit_yaml,
                "repoid": commit.repoid,
                _kwargs_key(UploadFlow): ANY,
            },
        )
        assert mock_self_app.send_task.call_count == 0
        mock_self_app.tasks[
            upload_breadcrumb_task_name
        ].apply_async.assert_called_once_with(
            kwargs={
                "commit_sha": commit.commitid,
                "repo_id": commit.repoid,
                "breadcrumb_data": BreadcrumbData(
                    milestone=Milestones.NOTIFICATIONS_TRIGGERED,
                ),
                "upload_ids": [1],
                "sentry_trace_id": None,
            }
        )

    def test_finish_reports_processing_with_pull(
        self, dbsession, mocker, mock_self_app
    ):
        commit_yaml = {}
        repository = RepositoryFactory.create(
            author__unencrypted_oauth_token="testulk3d54rlhxkjyzomq2wh8b7np47xabcrkx8",
            author__username="ThiagoCodecov",
            yaml=commit_yaml,
        )
        pull = PullFactory.create(repository=repository)

        dbsession.add(repository)
        dbsession.add(pull)
        dbsession.flush()

        compared_to = CommitFactory.create(repository=repository)
        pull.compared_to = compared_to.commitid
        commit = CommitFactory.create(
            message="dsidsahdsahdsa",
            commitid="abf6d4df662c47e32460020ab14abf9303581429",
            repository=repository,
            pullid=pull.pullid,
        )
        dbsession.add(commit)
        dbsession.add(compared_to)
        dbsession.flush()

        _start_upload_flow(mocker)
        res = UploadFinisherTask().finish_reports_processing(
            dbsession,
            commit,
            UserYaml(commit_yaml),
            [{"upload_id": 1, "successful": True}],
        )
        assert res == {"notifications_called": True}
        mock_self_app.tasks[notify_task_name].apply_async.assert_called_with(
            kwargs={
                "commitid": commit.commitid,
                "current_yaml": commit_yaml,
                "repoid": commit.repoid,
                _kwargs_key(UploadFlow): ANY,
            },
        )
        mock_self_app.tasks[pulls_task_name].apply_async.assert_called_with(
            kwargs={
                "pullid": pull.pullid,
                "repoid": pull.repoid,
                "should_send_notifications": False,
            }
        )
        assert mock_self_app.send_task.call_count == 0

        mock_self_app.tasks[
            compute_comparison_task_name
        ].apply_async.assert_called_once()
        mock_self_app.tasks[
            upload_breadcrumb_task_name
        ].apply_async.assert_called_once_with(
            kwargs={
                "commit_sha": commit.commitid,
                "repo_id": commit.repoid,
                "breadcrumb_data": BreadcrumbData(
                    milestone=Milestones.NOTIFICATIONS_TRIGGERED,
                ),
                "upload_ids": [1],
                "sentry_trace_id": None,
            }
        )

    @pytest.mark.parametrize(
        "notify_error",
        [True, False],
    )
    def test_finish_reports_processing_no_notification(
        self, dbsession, mocker, notify_error, mock_self_app
    ):
        commit_yaml = {"codecov": {"notify": {"notify_error": notify_error}}}
        commit = CommitFactory.create(
            message="dsidsahdsahdsa",
            commitid="abf6d4df662c47e32460020ab14abf9303581429",
            repository__author__unencrypted_oauth_token="testulk3d54rlhxkjyzomq2wh8b7np47xabcrkx8",
            repository__author__username="ThiagoCodecov",
            repository__yaml=commit_yaml,
        )
        dbsession.add(commit)
        dbsession.flush()

        _start_upload_flow(mocker)
        res = UploadFinisherTask().finish_reports_processing(
            dbsession, commit, UserYaml(commit_yaml), [{"successful": False}]
        )
        assert res == {"notifications_called": False}
        if notify_error:
            assert mock_self_app.send_task.call_count == 0
            mock_self_app.tasks[notify_error_task_name].apply_async.assert_called_once()
            mock_self_app.tasks[notify_task_name].apply_async.assert_not_called()
        else:
            assert mock_self_app.send_task.call_count == 0
            mock_self_app.tasks[notify_error_task_name].apply_async.assert_not_called()
            mock_self_app.tasks[notify_task_name].apply_async.assert_not_called()
        mock_self_app.tasks[upload_breadcrumb_task_name].apply_async.assert_not_called()

    @pytest.mark.django_db
    def test_upload_finisher_task_calls_save_commit_measurements_task(
        self, mocker, dbsession, mock_storage, mock_repo_provider, mock_self_app
    ):
        mocker.patch("tasks.upload_finisher.load_intermediate_reports", return_value=[])
        mocker.patch("tasks.upload_finisher.update_uploads")

        mocker.patch("tasks.upload_finisher.is_timeseries_enabled", return_value=True)

        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()

        mocker.patch(
            "tasks.upload_finisher.repository_datasets_query",
            return_value=[
                DatasetFactory.create(
                    repository_id=commit.repository.repoid,
                    name=MeasurementName.coverage.value,
                ),
                DatasetFactory.create(
                    repository_id=commit.repository.repoid,
                    name=MeasurementName.flag_coverage.value,
                ),
                DatasetFactory.create(
                    repository_id=commit.repository.repoid,
                    name=MeasurementName.component_coverage.value,
                ),
            ],
        )

        previous_results = [{"upload_id": 0, "arguments": {}, "successful": True}]
        UploadFinisherTask().run_impl(
            dbsession,
            previous_results,
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml={},
        )

        mock_self_app.tasks[
            timeseries_save_commit_measurements_task_name
        ].apply_async.assert_called_once_with(
            kwargs={
                "commitid": commit.commitid,
                "repoid": commit.repoid,
                "dataset_names": [
                    MeasurementName.coverage.value,
                    MeasurementName.flag_coverage.value,
                    MeasurementName.component_coverage.value,
                ],
            }
        )
        mock_self_app.tasks[upload_breadcrumb_task_name].apply_async.assert_has_calls(
            [
                call(
                    kwargs={
                        "commit_sha": commit.commitid,
                        "repo_id": commit.repoid,
                        "breadcrumb_data": BreadcrumbData(
                            milestone=Milestones.NOTIFICATIONS_TRIGGERED,
                        ),
                        "upload_ids": [0],
                        "sentry_trace_id": None,
                    }
                ),
                call(
                    kwargs={
                        "commit_sha": commit.commitid,
                        "repo_id": commit.repoid,
                        "breadcrumb_data": BreadcrumbData(
                            milestone=Milestones.UPLOAD_COMPLETE,
                        ),
                        "upload_ids": [0],
                        "sentry_trace_id": None,
                    }
                ),
            ]
        )

    @pytest.mark.django_db
    def test_retry_on_report_lock(self, dbsession, mocker, mock_redis, mock_self_app):
        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()

        # Mock LockManager to raise LockRetry for UPLOAD_PROCESSING lock
        m = mocker.MagicMock()
        m.return_value.locked.return_value.__enter__.side_effect = LockRetry(60)
        mocker.patch("tasks.upload_finisher.LockManager", m)

        task = UploadFinisherTask()
        task.request.retries = 0
        task.request.headers = {}

        with pytest.raises(Retry):
            task.run_impl(
                dbsession,
                [{"upload_id": 0, "successful": True, "arguments": {}}],
                repoid=commit.repoid,
                commitid=commit.commitid,
                commit_yaml={},
            )
        mock_self_app.tasks[upload_breadcrumb_task_name].apply_async.assert_has_calls(
            [
                call(
                    kwargs={
                        "commit_sha": commit.commitid,
                        "repo_id": commit.repoid,
                        "breadcrumb_data": BreadcrumbData(
                            milestone=Milestones.UPLOAD_COMPLETE,
                            error=Errors.INTERNAL_LOCK_ERROR,
                        ),
                        "upload_ids": [0],
                        "sentry_trace_id": None,
                    }
                ),
                call(
                    kwargs={
                        "commit_sha": commit.commitid,
                        "repo_id": commit.repoid,
                        "breadcrumb_data": BreadcrumbData(
                            milestone=Milestones.UPLOAD_COMPLETE,
                            error=Errors.INTERNAL_RETRYING,
                        ),
                        "upload_ids": [0],
                        "sentry_trace_id": None,
                    }
                ),
            ]
        )

    @pytest.mark.django_db
    def test_die_on_finisher_lock(
        self,
        mocker,
        dbsession,
        mock_configuration,
        mock_storage,
        mock_repo_provider,
        mock_redis,
        mock_self_app,
    ):
        mock_redis.scard.return_value = 0
        mocker.patch("tasks.upload_finisher.load_intermediate_reports", return_value=[])
        mocker.patch("tasks.upload_finisher.update_uploads")
        url = "v4/raw/2019-05-22/C3C4715CA57C910D11D5EB899FC86A7E/4c4e4654ac25037ae869caeb3619d485970b6304/a84d445c-9c1e-434f-8275-f18f1f320f81.txt"

        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()

        # Mock LockManager: first call (UPLOAD_PROCESSING) succeeds, second call (UPLOAD_FINISHER) raises LockRetry
        # Create two mock instances
        first_lock_manager = mocker.MagicMock()  # For UPLOAD_PROCESSING - succeeds
        second_lock_manager = (
            mocker.MagicMock()
        )  # For UPLOAD_FINISHER - raises LockRetry
        second_lock_manager.locked.return_value.__enter__.side_effect = LockRetry(60)

        lock_manager_mock = mocker.MagicMock()
        lock_manager_mock.side_effect = [first_lock_manager, second_lock_manager]
        mocker.patch("tasks.upload_finisher.LockManager", lock_manager_mock)

        task = UploadFinisherTask()
        task.request.retries = 0
        task.request.headers = {}

        # Task should call self.retry() which raises Retry exception
        with pytest.raises(Retry):
            task.run_impl(
                dbsession,
                [{"upload_id": 0, "arguments": {"url": url}, "successful": True}],
                repoid=commit.repoid,
                commitid=commit.commitid,
                commit_yaml={},
            )
        # Breadcrumb should be called twice: INTERNAL_LOCK_ERROR and INTERNAL_RETRYING
        assert (
            mock_self_app.tasks[upload_breadcrumb_task_name].apply_async.call_count == 2
        )
        mock_self_app.tasks[upload_breadcrumb_task_name].apply_async.assert_any_call(
            kwargs={
                "commit_sha": commit.commitid,
                "repo_id": commit.repoid,
                "breadcrumb_data": BreadcrumbData(
                    milestone=Milestones.UPLOAD_COMPLETE,
                    error=Errors.INTERNAL_LOCK_ERROR,
                ),
                "upload_ids": [0],
                "sentry_trace_id": None,
            }
        )
        mock_self_app.tasks[upload_breadcrumb_task_name].apply_async.assert_any_call(
            kwargs={
                "commit_sha": commit.commitid,
                "repo_id": commit.repoid,
                "breadcrumb_data": BreadcrumbData(
                    milestone=Milestones.UPLOAD_COMPLETE,
                    error=Errors.INTERNAL_RETRYING,
                ),
                "upload_ids": [0],
                "sentry_trace_id": None,
            }
        )

    @pytest.mark.django_db
    def test_soft_time_limit_handling(self, dbsession, mocker, mock_self_app):
        mocker.patch(
            "tasks.upload_finisher.load_commit_diff", side_effect=SoftTimeLimitExceeded
        )

        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()

        previous_results = [{"upload_id": 0, "successful": True, "arguments": {}}]

        UploadFinisherTask().run_impl(
            dbsession,
            previous_results,
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml={},
        )

        mock_self_app.tasks[
            upload_breadcrumb_task_name
        ].apply_async.assert_called_once_with(
            kwargs={
                "commit_sha": commit.commitid,
                "repo_id": commit.repoid,
                "breadcrumb_data": BreadcrumbData(
                    milestone=Milestones.UPLOAD_COMPLETE,
                    error=Errors.TASK_TIMED_OUT,
                ),
                "upload_ids": [0],
                "sentry_trace_id": None,
            }
        )

    @pytest.mark.django_db
    def test_generic_exception_handling(self, dbsession, mocker, mock_self_app):
        """Test that the generic exception handler captures and logs unexpected errors."""
        mock_sentry = mocker.patch("tasks.upload_finisher.sentry_sdk.capture_exception")

        # Mock an unexpected error during the _process_reports_with_lock call
        mocker.patch(
            "tasks.upload_finisher.UploadFinisherTask._process_reports_with_lock",
            side_effect=ValueError("Unexpected error occurred"),
        )

        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()

        previous_results = [{"upload_id": 0, "successful": True, "arguments": {}}]

        result = UploadFinisherTask().run_impl(
            dbsession,
            previous_results,
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml={},
        )

        # Assert that the error is captured by Sentry
        mock_sentry.assert_called_once()
        captured_exception = mock_sentry.call_args[0][0]
        assert isinstance(captured_exception, ValueError)
        assert str(captured_exception) == "Unexpected error occurred"

        # Assert that the function returns error information
        assert result == {
            "error": "Unexpected error occurred",
            "upload_ids": [0],
        }

        # Assert that the breadcrumb task is called with the error
        mock_self_app.tasks[
            upload_breadcrumb_task_name
        ].apply_async.assert_called_once_with(
            kwargs={
                "commit_sha": commit.commitid,
                "repo_id": commit.repoid,
                "breadcrumb_data": BreadcrumbData(
                    milestone=Milestones.UPLOAD_COMPLETE,
                    error=Errors.UNKNOWN,
                    error_text="ValueError('Unexpected error occurred')",
                ),
                "upload_ids": [0],
                "sentry_trace_id": None,
            }
        )

    @pytest.mark.django_db
    def test_idempotency_check_skips_already_processed_uploads(
        self, dbsession, mocker, mock_self_app
    ):
        """Test that finisher skips work if all uploads are already in final state.

        This test validates the idempotency check that prevents wasted work when:
        - Multiple finishers are triggered (e.g., visibility timeout re-queuing)
        - Finisher is manually retried

        The check only skips when ALL uploads exist in DB and are in final states.
        """
        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()

        report = CommitReport(commit_id=commit.id_)
        dbsession.add(report)
        dbsession.flush()

        # Create uploads that are already in "processed" state
        upload_1 = UploadFactory.create(report=report, state="processed")
        upload_2 = UploadFactory.create(report=report, state="error")
        dbsession.add(upload_1)
        dbsession.add(upload_2)
        dbsession.flush()

        # Mock the _process_reports_with_lock to verify it's NOT called
        mock_process = mocker.patch.object(
            UploadFinisherTask, "_process_reports_with_lock"
        )

        previous_results = [
            {"upload_id": upload_1.id, "successful": True, "arguments": {}},
            {"upload_id": upload_2.id, "successful": False, "arguments": {}},
        ]

        result = UploadFinisherTask().run_impl(
            dbsession,
            previous_results,
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml={},
        )

        # Verify that the finisher skipped all work
        assert result == {
            "already_completed": True,
            "upload_ids": [upload_1.id, upload_2.id],
        }

        # Verify that _process_reports_with_lock was NOT called
        mock_process.assert_not_called()

    @pytest.mark.django_db
    def test_idempotency_check_proceeds_when_uploads_not_finished(
        self, dbsession, mocker, mock_storage, mock_repo_provider, mock_self_app
    ):
        """Test that finisher proceeds normally if uploads are still in 'started' state."""
        mocker.patch("tasks.upload_finisher.load_intermediate_reports", return_value=[])
        mocker.patch("tasks.upload_finisher.update_uploads")

        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()

        report = CommitReport(commit_id=commit.id_)
        dbsession.add(report)
        dbsession.flush()

        # Create uploads that are still in "started" state
        upload_1 = UploadFactory.create(report=report, state="started")
        dbsession.add(upload_1)
        dbsession.flush()

        # Mock the _process_reports_with_lock to verify it IS called
        mock_process = mocker.patch.object(
            UploadFinisherTask, "_process_reports_with_lock"
        )

        previous_results = [
            {"upload_id": upload_1.id, "successful": True, "arguments": {}},
        ]

        UploadFinisherTask().run_impl(
            dbsession,
            previous_results,
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml={},
        )

        # Verify that _process_reports_with_lock WAS called
        mock_process.assert_called_once()

    @pytest.mark.django_db
    def test_reconstruct_processing_results_falls_back_to_database_when_redis_expires(
        self,
        dbsession,
        mocker,
        mock_storage,
        mock_repo_provider,
        mock_redis,
        mock_self_app,
    ):
        """Test that finisher falls back to database when Redis ProcessingState expires.

        This tests the edge case where Redis keys expire after 24h TTL, but uploads
        were processed and have intermediate reports. The finisher should find them
        via database query and include them in the final report.
        """
        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()

        report = CommitReport(commit_id=commit.id_)
        dbsession.add(report)
        dbsession.flush()

        # Create uploads in "started" state (simulating Redis state expired)
        upload_1 = UploadFactory.create(
            report=report, state="started", state_id=UploadState.UPLOADED.db_id
        )
        upload_2 = UploadFactory.create(
            report=report, state="started", state_id=UploadState.UPLOADED.db_id
        )
        dbsession.add(upload_1)
        dbsession.add(upload_2)
        dbsession.flush()

        # Mock Redis to simulate intermediate reports exist (confirms uploads were processed)
        mock_redis.exists.side_effect = lambda key: (
            key == intermediate_report_key(upload_1.id)
            or key == intermediate_report_key(upload_2.id)
        )

        # Mock ProcessingState to return empty (simulating Redis expiration)
        mock_state = mocker.MagicMock()
        mock_state.get_uploads_for_merging.return_value = set()  # Redis expired
        mock_state.get_upload_numbers.return_value = mocker.MagicMock(
            processing=0, processed=0
        )
        mocker.patch("tasks.upload_finisher.ProcessingState", return_value=mock_state)

        # Mock the processing methods
        mocker.patch("tasks.upload_finisher.load_intermediate_reports", return_value=[])
        mocker.patch("tasks.upload_finisher.update_uploads")
        mock_process = mocker.patch.object(
            UploadFinisherTask, "_process_reports_with_lock"
        )

        # Call run_impl without processing_results to trigger reconstruction
        task = UploadFinisherTask()
        task.run_impl(
            dbsession,
            processing_results=None,  # Triggers reconstruction
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml={},
        )

        # Verify that _find_started_uploads_with_reports was called (via reconstruction)
        # This is verified by checking that _process_reports_with_lock was called
        # with processing_results containing our uploads
        mock_process.assert_called_once()
        call_args = mock_process.call_args
        # processing_results is the 4th positional argument (index 0 is args tuple)
        processing_results = call_args[0][3]

        # Verify both uploads are included in processing_results
        upload_ids_in_results = {r["upload_id"] for r in processing_results}
        assert upload_1.id in upload_ids_in_results
        assert upload_2.id in upload_ids_in_results
        assert len(processing_results) == 2

        # Verify both are marked as successful (have intermediate reports)
        assert all(r["successful"] for r in processing_results)

    @pytest.mark.django_db
    def test_reconstruct_processing_results_returns_empty_when_no_uploads_found(
        self, dbsession, mocker, mock_redis, mock_self_app
    ):
        """Test that finisher returns empty list when no uploads found in Redis or DB.

        This tests the edge case where Redis expires AND no uploads exist in database
        in "started" state with intermediate reports.
        """
        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()

        # Mock ProcessingState to return empty (simulating Redis expiration)
        mock_state = mocker.MagicMock()
        mock_state.get_uploads_for_merging.return_value = set()  # Redis expired
        mocker.patch("tasks.upload_finisher.ProcessingState", return_value=mock_state)

        # Call run_impl without processing_results to trigger reconstruction
        task = UploadFinisherTask()
        result = task._reconstruct_processing_results(dbsession, mock_state, commit)

        # Verify empty list returned when no uploads found
        assert result == []

    @pytest.mark.django_db
    def test_coverage_notifications_not_blocked_by_test_results_uploads(
        self,
        dbsession,
        mocker,
        mock_storage,
        mock_repo_provider,
        mock_self_app,
    ):
        """
        Regression test for CCMRG-1909: Coverage notifications should not be blocked
        by test_results uploads that are still pending.

        This test verifies that when all coverage uploads are processed, notifications
        proceed even if test_results uploads are still in UPLOADED state.
        """
        mocker.patch("tasks.upload_finisher.load_intermediate_reports", return_value=[])
        # Don't mock update_uploads - let it run normally to update upload state
        mocker.patch(
            "tasks.upload_finisher.get_repo_provider_service",
            return_value=mock_repo_provider,
        )
        # Mock dependencies that _process_reports_with_lock needs (we'll handle update_uploads in the mock)
        mocker.patch("tasks.upload_finisher.load_commit_diff", return_value=None)

        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()

        # Create coverage report and upload (processed)
        coverage_report = CommitReport(
            commit_id=commit.id_,
            report_type=ReportType.COVERAGE.value,
        )
        dbsession.add(coverage_report)
        dbsession.flush()

        # Create coverage upload in "started" state initially (will be processed by _process_reports_with_lock)
        coverage_upload = UploadFactory.create(
            report=coverage_report,
            state="started",
            state_id=UploadState.UPLOADED.db_id,
        )
        dbsession.add(coverage_upload)
        dbsession.flush()

        # Create test_results report and upload (still in UPLOADED state)
        test_results_report = CommitReport(
            commit_id=commit.id_,
            report_type=ReportType.TEST_RESULTS.value,
        )
        dbsession.add(test_results_report)
        dbsession.flush()

        test_results_upload = UploadFactory.create(
            report=test_results_report,
            state="started",
            state_id=UploadState.UPLOADED.db_id,
        )
        dbsession.add(test_results_upload)
        dbsession.flush()

        processing_results = [
            {"upload_id": coverage_upload.id_, "successful": True, "arguments": {}},
        ]

        # Mock the _process_reports_with_lock to verify it IS called
        # Call update_uploads directly using the SQLAlchemy session (same as test_mark_uploads_as_failed)
        def mock_process_reports(
            db_session,
            commit,
            commit_yaml,
            processing_results,
            milestone,
            upload_ids,
            state,
        ):
            # Call update_uploads to update the upload state to PROCESSED
            # This uses the same SQLAlchemy session that the query will use
            update_uploads(
                db_session,
                commit_yaml,
                processing_results,
                [],  # intermediate_reports
                MergeResult({}, set()),  # merge_result
            )
            # update_uploads already calls flush(), but ensure the session is ready for queries
            # The flush() in update_uploads makes the update visible to subsequent queries
            db_session.expire_all()  # Expire all objects so queries refetch from DB

            # Verify the update worked by querying the upload directly
            updated_upload = (
                db_session.query(Upload)
                .filter(Upload.id_ == coverage_upload.id_)
                .first()
            )
            assert updated_upload is not None, "Upload should exist"
            assert updated_upload.state == "processed", (
                f"Upload state should be 'processed', got '{updated_upload.state}'"
            )
            assert updated_upload.state_id == UploadState.PROCESSED.db_id, (
                f"Upload state_id should be {UploadState.PROCESSED.db_id}, got {updated_upload.state_id}"
            )

        mock_process = mocker.patch.object(
            UploadFinisherTask,
            "_process_reports_with_lock",
            side_effect=mock_process_reports,
        )
        mock_handle_finisher_lock = mocker.patch.object(
            UploadFinisherTask, "_handle_finisher_lock", return_value={"notified": True}
        )

        result = UploadFinisherTask().run_impl(
            dbsession,
            processing_results,
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml={},
        )

        # Verify that _process_reports_with_lock WAS called
        mock_process.assert_called_once()

        # Verify that _handle_finisher_lock WAS called (notifications should proceed)
        # This is the key assertion - notifications should NOT be blocked by test_results uploads
        mock_handle_finisher_lock.assert_called_once()


class TestCommitRefreshAfterLock:
    """Tests for CCMRG-2028: commit must be refreshed after acquiring lock."""

    @pytest.mark.django_db
    def test_commit_is_refreshed_after_acquiring_lock(
        self,
        dbsession,
        mocker,
        mock_redis,
        mock_self_app,
    ):
        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()

        refresh_calls = []
        original_refresh = dbsession.refresh

        def tracking_refresh(obj):
            refresh_calls.append(obj)
            return original_refresh(obj)

        mocker.patch.object(dbsession, "refresh", side_effect=tracking_refresh)
        mock_redis.scard.return_value = 0
        mocker.patch("tasks.upload_finisher.load_intermediate_reports", return_value=[])
        mocker.patch("tasks.upload_finisher.update_uploads")
        mocker.patch("tasks.upload_finisher.cleanup_intermediate_reports")
        mocker.patch.object(
            UploadFinisherTask, "_handle_finisher_lock", return_value={}
        )

        task = UploadFinisherTask()
        task.request.retries = 0
        task.request.headers = {}

        task.run_impl(
            dbsession,
            [{"upload_id": 0, "successful": True, "arguments": {}}],
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml={},
        )

        assert any(obj.commitid == commit.commitid for obj in refresh_calls)

    @pytest.mark.django_db
    def test_stale_commit_sees_report_after_refresh(
        self,
        dbsession,
        mocker,
        mock_redis,
        mock_self_app,
        mock_storage,
        mock_configuration,
        mock_repo_provider,
    ):
        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()

        commit.report_json = {"files": {}, "sessions": {}}
        dbsession.commit()

        # Simulate stale commit by expiring the cached attributes
        dbsession.expire(commit, ["_report_json", "_report_json_storage_path"])

        mock_redis.scard.return_value = 0
        mocker.patch("tasks.upload_finisher.load_commit_diff", return_value=None)
        mocker.patch("tasks.upload_finisher.load_intermediate_reports", return_value=[])
        mocker.patch("tasks.upload_finisher.update_uploads")
        mocker.patch("tasks.upload_finisher.cleanup_intermediate_reports")
        mocker.patch.object(
            UploadFinisherTask, "_handle_finisher_lock", return_value={}
        )

        original_get_existing = ReportService.get_existing_report_for_commit
        get_existing_calls = []

        def tracking_get_existing(self, commit, report_class=None):
            has_report = commit._report_json is not None
            get_existing_calls.append(has_report)
            return original_get_existing(self, commit, report_class)

        mocker.patch.object(
            ReportService, "get_existing_report_for_commit", tracking_get_existing
        )

        task = UploadFinisherTask()
        task.request.retries = 0
        task.request.headers = {}

        task.run_impl(
            dbsession,
            [{"upload_id": 0, "successful": True, "arguments": {}}],
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml={},
        )

        assert get_existing_calls and get_existing_calls[0] is True
