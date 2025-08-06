from pathlib import Path
from unittest.mock import ANY, call

import pytest
from celery.exceptions import Retry, SoftTimeLimitExceeded
from redis.exceptions import LockError

from celery_config import notify_error_task_name
from database.models.reports import CommitReport
from database.tests.factories import CommitFactory, PullFactory, RepositoryFactory
from database.tests.factories.core import UploadFactory
from database.tests.factories.timeseries import DatasetFactory
from helpers.checkpoint_logger import _kwargs_key
from helpers.checkpoint_logger.flows import UploadFlow
from helpers.exceptions import RepositoryWithoutValidBotError
from helpers.log_context import LogContext, set_log_context
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
            repository__owner__unencrypted_oauth_token="testulk3d54rlhxkjyzomq2wh8b7np47xabcrkx8",
            repository__owner__username="ThiagoCodecov",
            repository__owner__service="github",
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
            repository__owner__username="ThiagoCodecov",
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
            repository__owner__username="ThiagoCodecov",
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

    def test_should_call_notifications(self, dbsession):
        commit_yaml = {"codecov": {"max_report_age": "1y ago"}}
        commit = CommitFactory.create(
            message="dsidsahdsahdsa",
            commitid="abf6d4df662c47e32460020ab14abf9303581429",
            repository__owner__unencrypted_oauth_token="testulk3d54rlhxkjyzomq2wh8b7np47xabcrkx8",
            repository__owner__username="ThiagoCodecov",
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
            repository__owner__unencrypted_oauth_token="aabbcc",
            repository__owner__username="Codecov",
            repository__yaml=commit_yaml,
        )
        dbsession.add(commit)
        dbsession.flush()

        assert (
            UploadFinisherTask().should_call_notifications(commit, commit_yaml, [])
            == ShouldCallNotifyResult.DO_NOT_NOTIFY
        )

    def test_should_call_notifications_manual_trigger_off(self, dbsession):
        commit_yaml = {
            "codecov": {"max_report_age": "1y ago", "notify": {"manual_trigger": False}}
        }
        commit = CommitFactory.create(
            message="dsidsahdsahdsa",
            commitid="abf6d4df662c47e32460020ab14abf9303581429",
            repository__owner__unencrypted_oauth_token="testulk3d54rlhxkjyzomq2wh8b7np47xabcrkx8",
            repository__owner__username="ThiagoCodecov",
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
        self, dbsession, notify_error, result
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
            repository__owner__unencrypted_oauth_token="testulk3d54rlhxkjyzomq2wh8b7np47xabcrkx8",
            repository__owner__username="ThiagoCodecov",
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
            repository__owner__unencrypted_oauth_token="testulk3d54rlhxkjyzomq2wh8b7np47xabcrkx8",
            repository__owner__username="ThiagoCodecov",
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
            repository__owner__unencrypted_oauth_token="testulk3d54rlhxkjyzomq2wh8b7np47xabcrkx8",
            repository__owner__username="ThiagoCodecov",
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

    def test_finish_reports_processing(self, dbsession, mocker, mock_self_app):
        commit_yaml = {}
        commit = CommitFactory.create(
            message="dsidsahdsahdsa",
            commitid="abf6d4df662c47e32460020ab14abf9303581429",
            repository__owner__unencrypted_oauth_token="testulk3d54rlhxkjyzomq2wh8b7np47xabcrkx8",
            repository__owner__username="ThiagoCodecov",
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
            owner__unencrypted_oauth_token="testulk3d54rlhxkjyzomq2wh8b7np47xabcrkx8",
            owner__username="ThiagoCodecov",
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
            repository__owner__unencrypted_oauth_token="testulk3d54rlhxkjyzomq2wh8b7np47xabcrkx8",
            repository__owner__username="ThiagoCodecov",
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
    def test_retry_on_report_lock(self, dbsession, mock_redis, mock_self_app):
        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()

        mock_redis.lock.side_effect = LockError()

        task = UploadFinisherTask()
        task.request.retries = 0

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

        mock_redis.lock.side_effect = [mocker.MagicMock(), LockError()]

        task = UploadFinisherTask()
        task.request.retries = 0

        result = task.run_impl(
            dbsession,
            [{"upload_id": 0, "arguments": {"url": url}, "successful": True}],
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml={},
        )
        assert result is None
        mock_self_app.tasks[
            upload_breadcrumb_task_name
        ].apply_async.assert_called_once_with(
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
