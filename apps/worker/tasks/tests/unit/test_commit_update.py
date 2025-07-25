import datetime as dt

import pytest

from database.models import Branch
from database.tests.factories import BranchFactory, CommitFactory, PullFactory
from helpers.exceptions import RepositoryWithoutValidBotError
from shared.celery_config import upload_breadcrumb_task_name
from shared.django_apps.upload_breadcrumbs.models import (
    BreadcrumbData,
    Errors,
    Milestones,
)
from shared.torngit.exceptions import (
    TorngitClientError,
    TorngitObjectNotFoundError,
    TorngitRepoNotFoundError,
)
from tasks.commit_update import CommitUpdateTask


@pytest.fixture
def mock_self_app(mocker):
    return mocker.patch.object(
        CommitUpdateTask,
        "app",
        tasks={
            upload_breadcrumb_task_name: mocker.MagicMock(),
        },
    )


@pytest.mark.integration
@pytest.mark.django_db
class TestCommitUpdate:
    def test_update_commit(
        self,
        mocker,
        mock_configuration,
        dbsession,
        codecov_vcr,
        mock_redis,
        mock_self_app,
    ):
        commit = CommitFactory.create(
            message="",
            commitid="a2d3e3c30547a000f026daa47610bb3f7b63aece",
            repository__owner__unencrypted_oauth_token="ghp_test3c8iyfspq6h4s9ugpmq19qp7826rv20o",
            repository__owner__username="test-acc9",
            repository__owner__service="github",
            repository__owner__service_id="104562106",
            repository__name="test_example",
            pullid=1,
        )
        dbsession.add(commit)
        dbsession.flush()

        result = CommitUpdateTask().run_impl(dbsession, commit.repoid, commit.commitid)
        expected_result = {"was_updated": True}
        assert expected_result == result
        assert commit.message == "random-commit-msg"
        assert commit.parent_commit_id is None
        assert commit.branch == "featureA"
        assert commit.pullid == 1
        mock_self_app.tasks[
            upload_breadcrumb_task_name
        ].apply_async.assert_called_once_with(
            kwargs={
                "commit_sha": commit.commitid,
                "repo_id": commit.repository.repoid,
                "breadcrumb_data": BreadcrumbData(
                    milestone=Milestones.COMMIT_PROCESSED,
                ),
                "sentry_trace_id": None,
            }
        )

    def test_update_commit_bot_unauthorized(
        self,
        mocker,
        mock_configuration,
        dbsession,
        mock_redis,
        mock_repo_provider,
        mock_storage,
        mock_self_app,
    ):
        mock_repo_provider.get_commit.side_effect = TorngitClientError(
            401, "response", "message"
        )
        commit = CommitFactory.create(
            message="",
            parent_commit_id=None,
            repository__owner__unencrypted_oauth_token="un-authorized",
            repository__owner__username="test-acc9",
            repository__yaml={"codecov": {"max_report_age": "764y ago"}},
        )
        mock_repo_provider.data = {
            "repo": {"repoid": commit.repoid, "commit": commit.commitid}
        }
        dbsession.add(commit)
        dbsession.flush()

        result = CommitUpdateTask().run_impl(dbsession, commit.repoid, commit.commitid)
        assert {"was_updated": False} == result
        assert commit.message == ""
        assert commit.parent_commit_id is None
        mock_self_app.tasks[
            upload_breadcrumb_task_name
        ].apply_async.assert_called_once_with(
            kwargs={
                "commit_sha": commit.commitid,
                "repo_id": commit.repository.repoid,
                "breadcrumb_data": BreadcrumbData(
                    milestone=Milestones.COMMIT_PROCESSED, error=Errors.GIT_CLIENT_ERROR
                ),
                "sentry_trace_id": None,
            }
        )

    def test_update_commit_no_bot(
        self,
        mocker,
        mock_configuration,
        dbsession,
        mock_redis,
        mock_repo_provider,
        mock_storage,
        mock_self_app,
    ):
        mock_get_repo_service = mocker.patch(
            "tasks.commit_update.get_repo_provider_service"
        )
        mock_get_repo_service.side_effect = RepositoryWithoutValidBotError()
        commit = CommitFactory.create(
            message="",
            parent_commit_id=None,
            commitid="a2d3e3c30547a000f026daa47610bb3f7b63aece",
            repository__owner__unencrypted_oauth_token="ghp_test3c8iyfspq6h4s9ugpmq19qp7826rv20o",
            repository__owner__username="test-acc9",
            repository__yaml={"codecov": {"max_report_age": "764y ago"}},
            repository__name="test_example",
        )
        dbsession.add(commit)
        dbsession.flush()
        result = CommitUpdateTask().run_impl(dbsession, commit.repoid, commit.commitid)
        expected_result = {"was_updated": False}
        assert expected_result == result
        assert commit.message == ""
        assert commit.parent_commit_id is None
        mock_self_app.tasks[
            upload_breadcrumb_task_name
        ].apply_async.assert_called_once_with(
            kwargs={
                "commit_sha": commit.commitid,
                "repo_id": commit.repository.repoid,
                "breadcrumb_data": BreadcrumbData(
                    milestone=Milestones.COMMIT_PROCESSED,
                    error=Errors.REPO_MISSING_VALID_BOT,
                ),
                "sentry_trace_id": None,
            }
        )

    def test_update_commit_repo_not_found(
        self,
        mocker,
        mock_configuration,
        dbsession,
        mock_redis,
        mock_repo_provider,
        mock_storage,
        mock_self_app,
    ):
        mock_get_repo_service = mocker.patch(
            "tasks.commit_update.get_repo_provider_service"
        )
        mock_get_repo_service.side_effect = TorngitRepoNotFoundError(
            "fake_response", "message"
        )
        commit = CommitFactory.create(
            message="",
            parent_commit_id=None,
            repository__owner__unencrypted_oauth_token="ghp_test3c8iyfspq6h4s9ugpmq19qp7826rv20o",
            repository__owner__username="test-acc9",
            repository__yaml={"codecov": {"max_report_age": "764y ago"}},
            repository__name="test_example",
        )
        dbsession.add(commit)
        dbsession.flush()

        result = CommitUpdateTask().run_impl(dbsession, commit.repoid, commit.commitid)
        expected_result = {"was_updated": False}
        assert expected_result == result
        assert commit.message == ""
        assert commit.parent_commit_id is None
        mock_self_app.tasks[
            upload_breadcrumb_task_name
        ].apply_async.assert_called_once_with(
            kwargs={
                "commit_sha": commit.commitid,
                "repo_id": commit.repository.repoid,
                "breadcrumb_data": BreadcrumbData(
                    milestone=Milestones.COMMIT_PROCESSED, error=Errors.REPO_NOT_FOUND
                ),
                "sentry_trace_id": None,
            }
        )

    def test_update_commit_not_found(
        self,
        mocker,
        mock_configuration,
        dbsession,
        mock_redis,
        mock_repo_provider,
        mock_storage,
        mock_self_app,
    ):
        mock_update_commit_from_provider = mocker.patch(
            "tasks.commit_update.possibly_update_commit_from_provider_info"
        )
        mock_update_commit_from_provider.side_effect = TorngitObjectNotFoundError(
            "fake_response", "message"
        )
        commit = CommitFactory.create(
            message="",
            parent_commit_id=None,
            repository__owner__unencrypted_oauth_token="ghp_test3c8iyfspq6h4s9ugpmq19qp7826rv20o",
            repository__owner__username="test-acc9",
            repository__yaml={"codecov": {"max_report_age": "764y ago"}},
            repository__name="test_example",
        )
        dbsession.add(commit)
        dbsession.flush()

        result = CommitUpdateTask().run_impl(dbsession, commit.repoid, commit.commitid)
        expected_result = {"was_updated": False}
        assert expected_result == result
        assert commit.message == ""
        assert commit.parent_commit_id is None
        mock_self_app.tasks[
            upload_breadcrumb_task_name
        ].apply_async.assert_called_once_with(
            kwargs={
                "commit_sha": commit.commitid,
                "repo_id": commit.repository.repoid,
                "breadcrumb_data": BreadcrumbData(
                    milestone=Milestones.COMMIT_PROCESSED, error=Errors.GIT_CLIENT_ERROR
                ),
                "sentry_trace_id": None,
            }
        )

    @pytest.mark.parametrize("branch_authors", [None, False, True])
    @pytest.mark.parametrize("prev_head", ["old_head", "new_head"])
    @pytest.mark.parametrize("deleted", [False, True])
    def test_update_commit_already_populated(
        self,
        mocker,
        mock_configuration,
        dbsession,
        mock_redis,
        mock_repo_provider,
        mock_storage,
        mock_self_app,
        branch_authors,
        prev_head,
        deleted,
    ):
        commit = CommitFactory.create(
            message="commit_msg",
            parent_commit_id=None,
            repository__owner__unencrypted_oauth_token="ghp_test3c8iyfspq6h4s9ugpmq19qp7826rv20o",
            repository__owner__username="test-acc9",
            repository__yaml={"codecov": {"max_report_age": "764y ago"}},
            repository__name="test_example",
            timestamp=dt.datetime.fromisoformat("2019-02-01T17:59:47"),
        )
        dbsession.add(commit)
        dbsession.flush()

        commit.branch = "featureA"
        commit.pullid = 1
        dbsession.flush()

        old_head = CommitFactory.create(
            message="",
            commitid="b2d3e3c30547a000f026daa47610bb3f7b63aece",
            repository=commit.repository,
            timestamp=dt.datetime.fromisoformat("2019-01-01T17:59:47"),
        )
        dbsession.add(old_head)
        dbsession.flush()

        old_head.branch = "featureA"
        old_head.pullid = 1
        dbsession.flush()

        pull = PullFactory(
            repository=commit.repository, pullid=1, head=old_head.commitid
        )
        dbsession.add(pull)
        dbsession.flush()

        if branch_authors is False:
            branch_authors = []
        elif branch_authors is True:
            branch_authors = [commit.author_id]

        if prev_head == "old_head":
            prev_head = old_head
        elif prev_head == "new_head":
            prev_head = commit

        if deleted:
            prev_head.deleted = True
            dbsession.flush()

        b = dbsession.query(Branch).first()
        dbsession.delete(b)
        dbsession.flush()

        branch = BranchFactory(
            repository=commit.repository,
            branch="featureA",
            head=prev_head.commitid,
            authors=branch_authors,
        )
        dbsession.add(branch)
        dbsession.flush()

        result = CommitUpdateTask().run_impl(dbsession, commit.repoid, commit.commitid)
        expected_result = {"was_updated": False}
        assert expected_result == result
        assert commit.message == "commit_msg"
        assert commit.parent_commit_id is None
        assert commit.timestamp == dt.datetime.fromisoformat("2019-02-01T17:59:47")
        assert commit.branch == "featureA"

        dbsession.refresh(pull)
        assert pull.head == commit.commitid

        dbsession.refresh(branch)
        assert branch.head == commit.commitid

        mock_self_app.tasks[
            upload_breadcrumb_task_name
        ].apply_async.assert_called_once_with(
            kwargs={
                "commit_sha": commit.commitid,
                "repo_id": commit.repository.repoid,
                "breadcrumb_data": BreadcrumbData(
                    milestone=Milestones.COMMIT_PROCESSED
                ),
                "sentry_trace_id": None,
            }
        )
