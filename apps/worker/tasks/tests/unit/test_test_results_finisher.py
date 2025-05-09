from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal
from unittest.mock import AsyncMock

import pytest

from database.enums import ReportType
from database.models import (
    CommitReport,
    Flake,
    ReducedError,
    Repository,
    RepositoryFlag,
    Test,
    TestInstance,
    UploadError,
)
from database.tests.factories import (
    CommitFactory,
    OwnerFactory,
    PullFactory,
    UploadFactory,
)
from services.repository import EnrichedPull
from services.test_results import generate_test_id
from shared.plan.constants import DEFAULT_FREE_PLAN, PlanName
from shared.torngit.exceptions import TorngitClientError
from tasks.test_results_finisher import TestResultsFinisherTask
from tests.helpers import mock_all_plans_and_tiers

here = Path(__file__)


@pytest.fixture
def test_results_mock_app(mocker):
    mocked_app = mocker.patch.object(
        TestResultsFinisherTask,
        "app",
        tasks={
            "app.tasks.notify.Notify": mocker.MagicMock(),
            "app.tasks.flakes.ProcessFlakesTask": mocker.MagicMock(),
            "app.tasks.cache_rollup.CacheTestRollupsTask": mocker.MagicMock(),
        },
    )
    return mocked_app


@pytest.fixture
def mock_repo_provider_comments(mocker):
    m = mocker.MagicMock(
        edit_comment=AsyncMock(return_value=True),
        post_comment=AsyncMock(return_value={"id": 1}),
    )
    _ = mocker.patch(
        "helpers.notifier.get_repo_provider_service",
        return_value=m,
    )
    _ = mocker.patch(
        "tasks.test_results_finisher.get_repo_provider_service",
        return_value=m,
    )
    return m


@pytest.fixture
def test_results_setup(mocker, dbsession):
    mocker.patch.object(TestResultsFinisherTask, "hard_time_limit_task", 0)

    commit = CommitFactory.create(
        message="hello world",
        commitid="cd76b0821854a780b60012aed85af0a8263004ad",
        repository__owner__unencrypted_oauth_token="test7lk5ndmtqzxlx06rip65nac9c7epqopclnoy",
        repository__owner__username="test-username",
        repository__owner__service="github",
        repository__owner__plan=PlanName.CODECOV_PRO_MONTHLY.value,
        repository__name="test-repo-name",
    )
    commit.branch = "main"
    dbsession.add(commit)
    dbsession.flush()

    commit.repository.branch = "main"
    dbsession.flush()

    repoid = commit.repoid
    commitid = commit.commitid

    current_report_row = CommitReport(
        commit_id=commit.id_, report_type=ReportType.TEST_RESULTS.value
    )
    dbsession.add(current_report_row)
    dbsession.flush()

    pull = PullFactory.create(repository=commit.repository, head=commit.commitid)
    dbsession.add(pull)
    dbsession.flush()

    enriched_pull = EnrichedPull(
        database_pull=pull,
        provider_pull={},
    )

    _ = mocker.patch(
        "helpers.notifier.fetch_and_update_pull_request_information_from_commit",
        return_value=enriched_pull,
    )
    _ = mocker.patch(
        "tasks.test_results_finisher.fetch_and_update_pull_request_information_from_commit",
        return_value=enriched_pull,
    )

    uploads = [UploadFactory.create() for _ in range(4)]
    uploads[3].created_at += timedelta(0, 3)

    for i, upload in enumerate(uploads):
        upload.report = current_report_row
        upload.report.commit.repoid = repoid
        upload.build_url = f"https://example.com/build_url_{i}"
        dbsession.add(upload)
    dbsession.flush()

    flags = [RepositoryFlag(repository_id=repoid, flag_name=str(i)) for i in range(2)]
    for flag in flags:
        dbsession.add(flag)
    dbsession.flush()

    uploads[0].flags = [flags[0]]
    uploads[1].flags = [flags[1]]
    uploads[2].flags = []
    uploads[3].flags = [flags[0]]
    dbsession.flush()

    test_name = "test_name"
    test_suite = "test_testsuite"

    test_id1 = generate_test_id(repoid, test_name + "0", test_suite, "a")
    test1 = Test(
        id_=test_id1,
        repoid=repoid,
        name="Class Name\x1f" + test_name + "0",
        testsuite=test_suite,
        flags_hash="a",
    )
    dbsession.add(test1)

    test_id2 = generate_test_id(repoid, test_name + "1", test_suite, "b")
    test2 = Test(
        id_=test_id2,
        repoid=repoid,
        name=test_name + "1",
        testsuite=test_suite,
        flags_hash="b",
    )
    dbsession.add(test2)

    test_id3 = generate_test_id(repoid, test_name + "2", test_suite, "")
    test3 = Test(
        id_=test_id3,
        repoid=repoid,
        name="Other Class Name\x1f" + test_name + "2",
        testsuite=test_suite,
        flags_hash="",
    )
    dbsession.add(test3)

    test_id4 = generate_test_id(repoid, test_name + "3", test_suite, "")
    test4 = Test(
        id_=test_id4,
        repoid=repoid,
        name=test_name + "3",
        testsuite=test_suite,
        flags_hash="",
    )
    dbsession.add(test4)

    dbsession.flush()

    test_instances = [
        TestInstance(
            test_id=test_id1,
            outcome="failure",
            failure_message="This should not be in the comment, it will get overwritten by the last test instance",
            duration_seconds=1.0,
            upload_id=uploads[0].id,
            repoid=repoid,
            commitid=commitid,
        ),
        TestInstance(
            test_id=test_id2,
            outcome="failure",
            failure_message="Shared \n\n\n\n <pre> ````````\n \r\n\r\n | test | test | test </pre>failure message",
            duration_seconds=2.0,
            upload_id=uploads[1].id,
            repoid=repoid,
            commitid=commitid,
        ),
        TestInstance(
            test_id=test_id3,
            outcome="failure",
            failure_message="Shared \n\n\n\n <pre> \n  ````````  \n \r\n\r\n | test | test | test </pre>failure message",
            duration_seconds=3.0,
            upload_id=uploads[2].id,
            repoid=repoid,
            commitid=commitid,
        ),
        TestInstance(
            test_id=test_id1,
            outcome="failure",
            failure_message="<pre>Fourth \r\n\r\n</pre> | test  | instance |",
            duration_seconds=4.0,
            upload_id=uploads[3].id,
            repoid=repoid,
            commitid=commitid,
        ),
        TestInstance(
            test_id=test_id4,
            outcome="failure",
            failure_message=None,
            duration_seconds=5.0,
            upload_id=uploads[3].id,
            repoid=repoid,
            commitid=commitid,
        ),
    ]
    for instance in test_instances:
        dbsession.add(instance)
    dbsession.flush()

    return (repoid, commit, pull, test_instances)


@pytest.fixture
def test_results_setup_no_instances(mocker, dbsession):
    mocker.patch.object(TestResultsFinisherTask, "hard_time_limit_task", 0)

    commit = CommitFactory.create(
        message="hello world",
        commitid="cd76b0821854a780b60012aed85af0a8263004ad",
        repository__owner__unencrypted_oauth_token="test7lk5ndmtqzxlx06rip65nac9c7epqopclnoy",
        repository__owner__username="joseph-sentry",
        repository__owner__service="github",
        repository__owner__plan=PlanName.CODECOV_PRO_MONTHLY.value,
        repository__name="codecov-demo",
    )
    commit.branch = "main"
    dbsession.add(commit)
    dbsession.flush()

    repoid = commit.repoid

    current_report_row = CommitReport(
        commit_id=commit.id_, report_type=ReportType.TEST_RESULTS.value
    )
    dbsession.add(current_report_row)
    dbsession.flush()

    pull = PullFactory.create(repository=commit.repository, head=commit.commitid)

    enriched_pull = EnrichedPull(
        database_pull=pull,
        provider_pull={},
    )

    _ = mocker.patch(
        "helpers.notifier.fetch_and_update_pull_request_information_from_commit",
        return_value=enriched_pull,
    )
    _ = mocker.patch(
        "tasks.test_results_finisher.fetch_and_update_pull_request_information_from_commit",
        return_value=enriched_pull,
    )

    uploads = [UploadFactory.create() for _ in range(4)]
    uploads[3].created_at += timedelta(0, 3)

    for upload in uploads:
        upload.report = current_report_row
        upload.report.commit.repoid = repoid
        dbsession.add(upload)
    dbsession.flush()

    flags = [RepositoryFlag(repository_id=repoid, flag_name=str(i)) for i in range(2)]
    for flag in flags:
        dbsession.add(flag)
    dbsession.flush()

    uploads[0].flags = [flags[0]]
    uploads[1].flags = [flags[1]]
    uploads[2].flags = []
    uploads[3].flags = [flags[0]]
    dbsession.flush()

    test_name = "test_name"
    test_suite = "test_testsuite"

    test_id1 = generate_test_id(repoid, test_name + "0", test_suite, "a")
    test1 = Test(
        id_=test_id1,
        repoid=repoid,
        name=test_name + "0",
        testsuite=test_suite,
        flags_hash="a",
    )
    dbsession.add(test1)

    test_id2 = generate_test_id(repoid, test_name + "1", test_suite, "b")
    test2 = Test(
        id_=test_id2,
        repoid=repoid,
        name=test_name + "1",
        testsuite=test_suite,
        flags_hash="b",
    )
    dbsession.add(test2)

    test_id3 = generate_test_id(repoid, test_name + "2", test_suite, "")
    test3 = Test(
        id_=test_id3,
        repoid=repoid,
        name=test_name + "2",
        testsuite=test_suite,
        flags_hash="",
    )
    dbsession.add(test3)

    test_id4 = generate_test_id(repoid, test_name + "3", test_suite, "")
    test4 = Test(
        id_=test_id4,
        repoid=repoid,
        name=test_name + "3",
        testsuite=test_suite,
        flags_hash="",
    )
    dbsession.add(test4)

    dbsession.flush()

    return (repoid, commit, pull, None)


class TestUploadTestFinisherTask:
    @pytest.fixture(autouse=True)
    def setup(self):
        mock_all_plans_and_tiers()

    @pytest.fixture(autouse=True)
    def setup_common_fixtures(
        self,
        mocker,
        mock_configuration,
        dbsession,
        codecov_vcr,
        mock_storage,
        mock_redis,
        celery_app,
        test_results_mock_app,
        mock_repo_provider_comments,
        test_results_setup,
    ):
        self.mocker = mocker
        self.mock_configuration = mock_configuration
        self.dbsession = dbsession
        self.codecov_vcr = codecov_vcr
        self.mock_storage = mock_storage
        self.mock_redis = mock_redis
        self.celery_app = celery_app
        self.test_results_mock_app = test_results_mock_app
        self.mock_repo_provider_comments = mock_repo_provider_comments
        self.test_results_setup = test_results_setup
        repoid, commit, pull, test_instances = test_results_setup
        self.repoid = repoid
        self.commit = commit
        self.pull = pull
        self.test_instances = test_instances

    def assert_notify_called(self, yaml=None):
        if yaml is None:
            yaml = {}

        self.test_results_mock_app.tasks[
            "app.tasks.notify.Notify"
        ].apply_async.assert_called_with(
            args=None,
            kwargs={
                "commitid": self.commit.commitid,
                "current_yaml": yaml,
                "repoid": self.repoid,
            },
        )

    def assert_notify_not_called(self):
        self.test_results_mock_app.tasks["app.tasks.notify.Notify"].assert_not_called()

    def assert_cache_rollup_called(self, impl_type="old"):
        self.test_results_mock_app.tasks[
            "app.tasks.cache_rollup.CacheTestRollupsTask"
        ].apply_async.assert_called_with(
            kwargs={
                "repo_id": self.repoid,
                "branch": "main",
                "impl_type": impl_type,
            },
        )

    def assert_process_flakes_called(self, impl_type="old"):
        self.test_results_mock_app.tasks[
            "app.tasks.flakes.ProcessFlakesTask"
        ].apply_async.assert_called_with(
            kwargs={
                "repo_id": self.repoid,
                "impl_type": impl_type,
            },
        )

    def assert_process_flakes_not_called(self):
        self.test_results_mock_app.tasks[
            "app.tasks.flakes.ProcessFlakesTask"
        ].apply_async.assert_not_called()

    def run_test_results_finisher(
        self,
        chain_result=True,
        commit_yaml={},
        impl_type: Literal["old", "new", "both"] = "old",
    ):
        return TestResultsFinisherTask().run_impl(
            self.dbsession,
            chain_result,
            repoid=self.repoid,
            commitid=self.commit.commitid,
            commit_yaml=commit_yaml,
            impl_type=impl_type,
        )

    @pytest.mark.integration
    @pytest.mark.django_db
    def test_upload_finisher_task_call(self, snapshot):
        result = self.run_test_results_finisher(impl_type="both")

        expected_result = {
            "notify_attempted": True,
            "notify_succeeded": True,
            "queue_notify": False,
        }

        self.assert_cache_rollup_called(impl_type="both")

        assert expected_result == result
        assert (
            snapshot("txt")
            == self.mock_repo_provider_comments.post_comment.call_args[0][1]
        )

    @pytest.mark.integration
    @pytest.mark.django_db
    def test_upload_finisher_task_call_no_failures(self):
        for instance in self.test_instances:
            instance.outcome = "pass"
        self.dbsession.flush()

        result = self.run_test_results_finisher()

        expected_result = {
            "notify_attempted": False,
            "notify_succeeded": False,
            "queue_notify": True,
        }

        self.assert_notify_called()
        self.assert_cache_rollup_called()

        assert expected_result == result

    @pytest.mark.integration
    @pytest.mark.django_db
    def test_upload_finisher_task_call_no_pull(self):
        _ = self.mocker.patch(
            "tasks.test_results_finisher.fetch_and_update_pull_request_information_from_commit",
            return_value=None,
        )

        result = self.run_test_results_finisher()

        expected_result = {
            "notify_attempted": False,
            "notify_succeeded": False,
            "queue_notify": False,
        }

        assert expected_result == result

    @pytest.mark.django_db
    @pytest.mark.integration
    def test_upload_finisher_task_call_error_with_failures(self, snapshot):
        upload_error = UploadError(
            report_upload=self.test_instances[0].upload,
            error_code="unsupported_file_format",
            error_params={"error_message": "parser error message"},
        )

        self.dbsession.add(upload_error)
        self.dbsession.flush()

        result = self.run_test_results_finisher(chain_result=False)

        expected_result = {
            "notify_attempted": True,
            "notify_succeeded": True,
            "queue_notify": False,
        }

        assert expected_result == result
        assert (
            snapshot("txt")
            == self.mock_repo_provider_comments.post_comment.call_args[0][1]
        )
        self.assert_cache_rollup_called()

    @pytest.mark.django_db
    @pytest.mark.integration
    def test_upload_finisher_task_call_error_only(self, snapshot):
        for instance in self.test_instances:
            instance.outcome = "pass"
        self.dbsession.flush()

        upload_error = UploadError(
            report_upload=self.test_instances[0].upload,
            error_code="unsupported_file_format",
            error_params={"error_message": "parser error message"},
        )

        self.dbsession.add(upload_error)
        self.dbsession.flush()

        result = self.run_test_results_finisher(chain_result=False, commit_yaml={})

        expected_result = {
            "notify_attempted": True,
            "notify_succeeded": True,
            "queue_notify": True,
        }

        assert expected_result == result
        assert (
            snapshot("txt")
            == self.mock_repo_provider_comments.post_comment.call_args[0][1]
        )
        self.assert_notify_called(yaml={})
        self.assert_cache_rollup_called()

    @pytest.mark.django_db
    @pytest.mark.integration
    def test_upload_finisher_task_call_warning(self, snapshot):
        for instance in self.test_instances:
            instance.outcome = "pass"
        self.dbsession.flush()

        upload_error = UploadError(
            report_upload=self.test_instances[0].upload,
            error_code="warning",
            error_params={"warning_message": "parser warning message"},
        )

        self.dbsession.add(upload_error)
        self.dbsession.flush()

        result = self.run_test_results_finisher(chain_result=False, commit_yaml={})

        expected_result = {
            "notify_attempted": True,
            "notify_succeeded": True,
            "queue_notify": True,
        }

        assert expected_result == result
        assert (
            snapshot("txt")
            == self.mock_repo_provider_comments.post_comment.call_args[0][1]
        )
        self.assert_notify_called(yaml={})
        self.assert_cache_rollup_called()

    @pytest.mark.integration
    @pytest.mark.django_db
    def test_upload_finisher_task_call_upgrade_comment(self, snapshot):
        repo = (
            self.dbsession.query(Repository)
            .filter(Repository.repoid == self.repoid)
            .first()
        )
        repo.owner.plan_activated_users = []
        repo.owner.plan = PlanName.CODECOV_PRO_MONTHLY.value
        repo.private = True
        self.dbsession.flush()

        pr_author = OwnerFactory(service="github", service_id=100)
        self.dbsession.add(pr_author)
        self.dbsession.flush()

        enriched_pull = EnrichedPull(
            database_pull=self.pull,
            provider_pull={"author": {"id": "100", "username": "test_username"}},
        )
        _ = self.mocker.patch(
            "helpers.notifier.fetch_and_update_pull_request_information_from_commit",
            return_value=enriched_pull,
        )
        _ = self.mocker.patch(
            "tasks.test_results_finisher.fetch_and_update_pull_request_information_from_commit",
            return_value=enriched_pull,
        )

        result = self.run_test_results_finisher()

        assert result == {
            "notify_attempted": True,
            "notify_succeeded": True,
            "queue_notify": False,
        }

        assert (
            snapshot("txt")
            == self.mock_repo_provider_comments.post_comment.call_args[0][1]
        )
        self.assert_notify_not_called()
        self.assert_cache_rollup_called()

    @pytest.mark.integration
    @pytest.mark.django_db
    def test_upload_finisher_task_call_existing_comment(self, snapshot):
        self.pull.commentid = 1
        self.dbsession.flush()

        result = self.run_test_results_finisher()

        expected_result = {
            "notify_attempted": True,
            "notify_succeeded": True,
            "queue_notify": False,
        }

        self.assert_cache_rollup_called()

        assert (
            self.mock_repo_provider_comments.edit_comment.call_args[0][0]
            == self.pull.pullid
        )
        assert self.mock_repo_provider_comments.edit_comment.call_args[0][1] == 1
        assert (
            snapshot("txt")
            == self.mock_repo_provider_comments.edit_comment.call_args[0][2]
        )

        assert expected_result == result

    @pytest.mark.integration
    @pytest.mark.django_db
    def test_upload_finisher_task_call_comment_fails(self):
        self.mock_repo_provider_comments.post_comment.side_effect = TorngitClientError

        result = self.run_test_results_finisher()

        expected_result = {
            "notify_attempted": True,
            "notify_succeeded": False,
            "queue_notify": False,
        }

        self.assert_cache_rollup_called()
        assert expected_result == result

    @pytest.mark.parametrize(
        "fail_count,count,recent_passes_count", [(2, 15, 13), (50, 150, 10)]
    )
    @pytest.mark.integration
    @pytest.mark.django_db
    def test_upload_finisher_task_call_with_flaky(
        self, fail_count, count, recent_passes_count, snapshot
    ):
        for i, instance in enumerate(self.test_instances):
            if i != 2:
                self.dbsession.delete(instance)
        self.dbsession.flush()

        r = ReducedError()
        r.message = "failure_message"

        self.dbsession.add(r)
        self.dbsession.flush()

        f = Flake()
        f.repoid = self.repoid
        f.testid = self.test_instances[2].test_id
        f.reduced_error = r
        f.count = count
        f.fail_count = fail_count
        f.recent_passes_count = recent_passes_count
        f.start_date = datetime.now()
        f.end_date = None

        self.dbsession.add(f)
        self.dbsession.flush()

        result = self.run_test_results_finisher(
            commit_yaml={
                "test_analytics": {"flake_detection": True},
            },
        )

        expected_result = {
            "notify_attempted": True,
            "notify_succeeded": True,
            "queue_notify": False,
        }

        assert expected_result == result
        self.assert_cache_rollup_called()
        assert (
            snapshot("txt")
            == self.mock_repo_provider_comments.post_comment.call_args[0][1]
        )

    @pytest.mark.integration
    @pytest.mark.django_db
    def test_upload_finisher_task_call_main_branch(self):
        self.commit.merged = True

        result = self.run_test_results_finisher(
            commit_yaml={
                "test_analytics": {"flake_detection": True},
            },
        )

        expected_result = {
            "notify_attempted": True,
            "notify_succeeded": True,
            "queue_notify": False,
        }

        assert expected_result == result
        self.assert_process_flakes_called()
        self.assert_cache_rollup_called()

    @pytest.mark.integration
    @pytest.mark.django_db
    def test_upload_finisher_task_call_computed_name(self, snapshot):
        for instance in self.test_instances:
            instance.test.computed_name = f"hello_{instance.test.name}"

        self.dbsession.flush()

        result = self.run_test_results_finisher()

        expected_result = {
            "notify_attempted": True,
            "notify_succeeded": True,
            "queue_notify": False,
        }

        assert expected_result == result
        assert (
            snapshot("txt")
            == self.mock_repo_provider_comments.post_comment.call_args[0][1]
        )

    @pytest.mark.integration
    @pytest.mark.django_db
    @pytest.mark.parametrize("plan", [DEFAULT_FREE_PLAN, "users-pr-inappm"])
    def test_upload_finisher_task_call_main_with_plan(self, plan):
        self.mocker.patch.object(TestResultsFinisherTask, "get_flaky_tests")

        commit_yaml = {
            "test_analytics": {"flake_detection": True},
        }

        self.commit.merged = True

        repo = self.dbsession.query(Repository).filter_by(repoid=self.repoid).first()
        repo.owner.plan = plan
        self.dbsession.flush()

        result = self.run_test_results_finisher(
            commit_yaml=commit_yaml,
        )

        expected_result = {
            "notify_attempted": True,
            "notify_succeeded": True,
            "queue_notify": False,
        }

        assert expected_result == result

        if plan == PlanName.CODECOV_PRO_MONTHLY.value:
            self.assert_process_flakes_called()
        else:
            self.assert_process_flakes_not_called()

    @pytest.mark.integration
    @pytest.mark.django_db
    def test_upload_finisher_new_impl(self):
        new_impl = self.mocker.patch(
            "tasks.test_results_finisher.new_impl",
            return_value={
                "notify_attempted": True,
                "notify_succeeded": True,
                "queue_notify": False,
            },
        )

        result = self.run_test_results_finisher(
            impl_type="new",
        )

        assert result == {
            "notify_attempted": True,
            "notify_succeeded": True,
            "queue_notify": False,
        }

        assert new_impl.call_count == 1
        repo = self.dbsession.query(Repository).filter_by(repoid=self.repoid).first()
        assert (
            self.dbsession,
            repo,
            self.commit,
            self.mocker.ANY,
            "new",
        ) == new_impl.call_args[0]
