import pytest
from celery.exceptions import Retry

from database.models.reports import Upload
from database.tests.factories.core import (
    CommitFactory,
    ReportFactory,
    RepositoryFactory,
)
from helpers.exceptions import (
    NoConfiguredAppsAvailable,
    OwnerWithoutValidBotError,
    RepositoryWithoutValidBotError,
)
from services.lock_manager import LockRetry
from services.report import ReportService
from shared.celery_config import upload_breadcrumb_task_name
from shared.django_apps.upload_breadcrumbs.models import (
    BreadcrumbData,
    Errors,
    Milestones,
)
from tasks.preprocess_upload import PreProcessUpload


@pytest.fixture
def mock_self_app(mocker, celery_app):
    mock_app = celery_app
    mock_app.tasks[upload_breadcrumb_task_name] = mocker.MagicMock()

    return mocker.patch.object(
        PreProcessUpload,
        "app",
        mock_app,
    )


class TestPreProcessUpload:
    @pytest.mark.django_db
    def test_preprocess_task(
        self,
        mocker,
        mock_configuration,
        dbsession,
        mock_storage,
        mock_redis,
        sample_report,
        mock_self_app,
    ):
        # get_existing_report_for_commit gets called for the parent commit
        mocker.patch.object(
            ReportService,
            "get_existing_report_for_commit",
            return_value=sample_report,
        )
        commit_yaml = {
            "flag_management": {
                "individual_flags": [
                    {
                        "name": "unit",
                        "carryforward": True,
                    }
                ]
            }
        }
        mocker.patch(
            "services.repository.fetch_commit_yaml_from_provider",
            return_value=commit_yaml,
        )
        mock_save_commit = mocker.patch(
            "services.repository.save_repo_yaml_to_database_if_needed"
        )

        def fake_possibly_shift(report, base, head):
            return report

        mock_possibly_shift = mocker.patch.object(
            ReportService,
            "_possibly_shift_carryforward_report",
            side_effect=fake_possibly_shift,
        )
        commit, report = self.create_commit_and_report(dbsession)

        result = PreProcessUpload().process_impl_within_lock(
            dbsession, repoid=commit.repository.repoid, commitid=commit.commitid
        )
        for sess_id in sample_report.sessions.keys():
            upload = (
                dbsession.query(Upload)
                .filter_by(report_id=commit.report.id_, order_number=sess_id)
                .first()
            )
            assert upload
            assert upload.flag_names == ["unit"]
        assert result == {
            "preprocessed_upload": True,
            "reportid": str(report.external_id),
            "updated_commit": False,
        }
        mock_save_commit.assert_called_with(commit, commit_yaml)
        mock_possibly_shift.assert_called()
        mock_self_app.tasks[
            upload_breadcrumb_task_name
        ].apply_async.assert_called_once_with(
            kwargs={
                "commit_sha": commit.commitid,
                "repo_id": commit.repository.repoid,
                "breadcrumb_data": BreadcrumbData(
                    milestone=Milestones.READY_FOR_REPORT,
                ),
                "upload_ids": [],
                "sentry_trace_id": None,
            }
        )

    def create_commit_and_report(self, dbsession):
        repository = RepositoryFactory()
        parent_commit = CommitFactory(repository=repository)
        parent_commit_report = ReportFactory(commit=parent_commit)
        commit = CommitFactory(
            _report_json=None,
            parent_commit_id=parent_commit.commitid,
            repository=repository,
        )
        report = ReportFactory(commit=commit)
        dbsession.add(parent_commit)
        dbsession.add(parent_commit_report)
        dbsession.add(commit)
        dbsession.add(report)
        dbsession.flush()
        return commit, report

    def test_run_impl_already_running(self, dbsession, mock_redis, mock_self_app):
        mock_redis.get = lambda _name: True
        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()
        result = PreProcessUpload().run_impl(
            dbsession,
            repoid=commit.repository.repoid,
            commitid=commit.commitid,
        )
        assert result == {"preprocessed_upload": False, "reason": "already_running"}
        mock_self_app.tasks[
            upload_breadcrumb_task_name
        ].apply_async.assert_called_once_with(
            kwargs={
                "commit_sha": commit.commitid,
                "repo_id": commit.repository.repoid,
                "breadcrumb_data": BreadcrumbData(
                    milestone=Milestones.READY_FOR_REPORT,
                ),
                "upload_ids": [],
                "sentry_trace_id": None,
            }
        )

    def test_run_impl_unobtainable_lock(
        self, dbsession, mocker, mock_redis, mock_self_app
    ):
        mock_redis.get = lambda _name: False
        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()
        # Mock LockManager to raise LockRetry
        m = mocker.MagicMock()
        m.return_value.locked.return_value.__enter__.side_effect = LockRetry(60)
        mocker.patch("tasks.preprocess_upload.LockManager", m)
        task = PreProcessUpload()
        task.request.retries = 0  # Set retries to 0 so it will retry
        # Task should call self.retry() which raises Retry exception
        with pytest.raises(Retry):
            task.run_impl(
                dbsession,
                repoid=commit.repository.repoid,
                commitid=commit.commitid,
            )
        # Breadcrumb should be called twice: INTERNAL_LOCK_ERROR and INTERNAL_RETRYING
        assert (
            mock_self_app.tasks[upload_breadcrumb_task_name].apply_async.call_count == 2
        )
        mock_self_app.tasks[upload_breadcrumb_task_name].apply_async.assert_any_call(
            kwargs={
                "commit_sha": commit.commitid,
                "repo_id": commit.repository.repoid,
                "breadcrumb_data": BreadcrumbData(
                    milestone=Milestones.READY_FOR_REPORT,
                    error=Errors.INTERNAL_LOCK_ERROR,
                ),
                "upload_ids": [],
                "sentry_trace_id": None,
            }
        )
        mock_self_app.tasks[upload_breadcrumb_task_name].apply_async.assert_any_call(
            kwargs={
                "commit_sha": commit.commitid,
                "repo_id": commit.repository.repoid,
                "breadcrumb_data": BreadcrumbData(
                    milestone=Milestones.READY_FOR_REPORT,
                    error=Errors.INTERNAL_RETRYING,
                ),
                "upload_ids": [],
                "sentry_trace_id": None,
            }
        )

    def test_get_repo_service_repo_and_owner_lack_bot(
        self, dbsession, mocker, mock_self_app
    ):
        mock_owner_bot = mocker.patch(
            "shared.bots.repo_bots.get_owner_or_appropriate_bot"
        )
        mock_owner_bot.side_effect = OwnerWithoutValidBotError()

        mock_github_installations = mocker.patch(
            "shared.bots.github_apps.get_github_app_info_for_owner"
        )
        mock_github_installations.return_value = []

        mock_save_error = mocker.patch("tasks.preprocess_upload.save_commit_error")

        commit = CommitFactory.create(repository__private=True, repository__bot=None)
        repo_service = PreProcessUpload().get_repo_service(commit, None)

        assert repo_service is None
        mock_save_error.assert_called()
        mock_self_app.tasks[
            upload_breadcrumb_task_name
        ].apply_async.assert_called_once_with(
            kwargs={
                "commit_sha": commit.commitid,
                "repo_id": commit.repository.repoid,
                "breadcrumb_data": BreadcrumbData(
                    milestone=Milestones.READY_FOR_REPORT,
                    error=Errors.REPO_MISSING_VALID_BOT,
                ),
                "upload_ids": [],
                "sentry_trace_id": None,
            }
        )

    def test_get_repo_provider_service_no_bot(self, dbsession, mocker, mock_self_app):
        mocker.patch("tasks.preprocess_upload.save_commit_error")
        mock_get_repo_service = mocker.patch(
            "tasks.preprocess_upload.get_repo_provider_service"
        )
        mock_get_repo_service.side_effect = RepositoryWithoutValidBotError()
        commit = CommitFactory.create()
        repo_provider = PreProcessUpload().get_repo_service(commit, None)
        assert repo_provider is None
        mock_self_app.tasks[
            upload_breadcrumb_task_name
        ].apply_async.assert_called_once_with(
            kwargs={
                "commit_sha": commit.commitid,
                "repo_id": commit.repository.repoid,
                "breadcrumb_data": BreadcrumbData(
                    milestone=Milestones.READY_FOR_REPORT,
                    error=Errors.REPO_MISSING_VALID_BOT,
                ),
                "upload_ids": [],
                "sentry_trace_id": None,
            }
        )

    def test_preprocess_upload_fail_no_provider_service(
        self, dbsession, mocker, mock_self_app
    ):
        mocker.patch("tasks.preprocess_upload.save_commit_error")
        mock_get_repo_service = mocker.patch(
            "tasks.preprocess_upload.get_repo_provider_service"
        )
        mock_get_repo_service.side_effect = RepositoryWithoutValidBotError()
        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()
        res = PreProcessUpload().process_impl_within_lock(
            db_session=dbsession, repoid=commit.repoid, commitid=commit.commitid
        )
        assert res == {
            "preprocessed_upload": False,
            "updated_commit": False,
            "error": "Failed to get repository_service",
        }
        mock_self_app.tasks[
            upload_breadcrumb_task_name
        ].apply_async.assert_called_once_with(
            kwargs={
                "commit_sha": commit.commitid,
                "repo_id": commit.repository.repoid,
                "breadcrumb_data": BreadcrumbData(
                    milestone=Milestones.READY_FOR_REPORT,
                    error=Errors.REPO_MISSING_VALID_BOT,
                ),
                "upload_ids": [],
                "sentry_trace_id": None,
            }
        )

    def test_get_repo_service_no_configured_apps_rate_limited(
        self, dbsession, mocker, mock_self_app
    ):
        """Test that rate-limited NoConfiguredAppsAvailable triggers a retry."""
        mock_get_repo_service = mocker.patch(
            "tasks.preprocess_upload.get_repo_provider_service"
        )
        mock_get_repo_service.side_effect = NoConfiguredAppsAvailable(
            apps_count=2,
            rate_limited_count=2,
            suspended_count=0,
        )
        mocker.patch(
            "tasks.preprocess_upload.get_seconds_to_next_hour", return_value=120
        )

        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()

        with pytest.raises(Retry):
            PreProcessUpload().get_repo_service(commit, None)

    def test_get_repo_service_no_configured_apps_suspended(
        self, dbsession, mocker, mock_self_app
    ):
        """Test that suspended apps (not rate-limited) returns None and saves error."""
        mock_get_repo_service = mocker.patch(
            "tasks.preprocess_upload.get_repo_provider_service"
        )
        mock_get_repo_service.side_effect = NoConfiguredAppsAvailable(
            apps_count=1,
            rate_limited_count=0,
            suspended_count=1,
        )
        mock_save_error = mocker.patch("tasks.preprocess_upload.save_commit_error")

        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()

        repo_service = PreProcessUpload().get_repo_service(commit, None)

        assert repo_service is None
        mock_save_error.assert_called()
        mock_self_app.tasks[
            upload_breadcrumb_task_name
        ].apply_async.assert_called_once_with(
            kwargs={
                "commit_sha": commit.commitid,
                "repo_id": commit.repository.repoid,
                "breadcrumb_data": BreadcrumbData(
                    milestone=Milestones.READY_FOR_REPORT,
                    error=Errors.REPO_MISSING_VALID_BOT,
                ),
                "upload_ids": [],
                "sentry_trace_id": None,
            }
        )
