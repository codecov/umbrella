from unittest.mock import AsyncMock, MagicMock

import pytest
from celery.exceptions import Retry
from django.utils import timezone

from database.models import Test, TestInstance
from helpers.notifier import NotifierResult
from services.lock_manager import LockRetry
from services.test_analytics.prevent_timeseries import (
    FailedTestInstance,
    insert_testrun,
)
from services.test_results import (
    NotifierTaskResult,
    TestResultsNotificationFailure,
    generate_test_id,
)
from shared.django_apps.core.tests.factories import RepositoryFactory
from shared.django_apps.prevent_timeseries.models import Testrun
from shared.torngit.response_types import ProviderPull
from shared.upload.types import TAUploadContext, UploadPipeline
from shared.yaml.user_yaml import UserYaml
from tasks.test_analytics_notifier import (
    DEBOUNCE_PERIOD_SECONDS,
    TA_NOTIFIER_FENCING_TOKEN,
    TestAnalyticsNotifierTask,
    transform_failures,
)


@pytest.fixture
def mock_lock_manager(mocker):
    lock_manager_mock = MagicMock()
    mocker.patch(
        "tasks.test_analytics_notifier.LockManager",
        return_value=lock_manager_mock,
    )
    return lock_manager_mock


@pytest.fixture
def mock_test_analytics_services(mocker):
    mocks = {
        "should_do_flaky_detection": mocker.patch(
            "tasks.test_analytics_notifier.should_do_flaky_detection",
            return_value=True,
        ),
        "fetch_pull_request_information": mocker.patch(
            "tasks.test_analytics_notifier.fetch_pull_request_information",
            new_callable=AsyncMock,
        ),
        "get_flaky_tests_dict": mocker.patch(
            "tasks.test_analytics_notifier.get_flaky_tests_dict",
            return_value={},
        ),
    }
    return mocks


@pytest.fixture
def test_setup(mocker, dbsession):
    repository = RepositoryFactory.create(
        name="test-repo",
        author__username="test-user",
    )

    repo_id = repository.repoid
    commit_sha = "abc123def456789012345678901234567890abcd"

    test_name = "test_example"
    test_suite = "test_suite"

    test_id1 = generate_test_id(repo_id, test_name + "_1", test_suite, "flag0")
    test1 = Test(
        id_=test_id1,
        repoid=repo_id,
        name=test_name + "_1",
        testsuite=test_suite,
        flags_hash="flag0",
    )
    dbsession.add(test1)

    test_id2 = generate_test_id(repo_id, test_name + "_2", test_suite, "flag1")
    test2 = Test(
        id_=test_id2,
        repoid=repo_id,
        name=test_name + "_2",
        testsuite=test_suite,
        flags_hash="flag1",
    )
    dbsession.add(test2)

    test_id3 = generate_test_id(repo_id, test_name + "_3", test_suite, "")
    test3 = Test(
        id_=test_id3,
        repoid=repo_id,
        name=test_name + "_3",
        testsuite=test_suite,
        flags_hash="",
    )
    dbsession.add(test3)

    test_instances = [
        TestInstance(
            test_id=test_id1,
            outcome="failure",
            failure_message="/very/long/path/to/test.py:10\nTraceback (most recent call last):\nAssertionError: Test failed\r\n",
            duration_seconds=1.5,
            upload_id=12345,  # Random upload ID
            repoid=repo_id,
            commitid=commit_sha,
        ),
        TestInstance(
            test_id=test_id2,
            outcome="failure",
            failure_message="Another failure message\r\nwith carriage returns",
            duration_seconds=2.0,
            upload_id=67890,  # Random upload ID
            repoid=repo_id,
            commitid=commit_sha,
        ),
        TestInstance(
            test_id=test_id3,
            outcome="failure",
            failure_message=None,  # Test case with no failure message
            duration_seconds=0.5,
            upload_id=11111,  # Random upload ID
            repoid=repo_id,
            commitid=commit_sha,
        ),
    ]

    upload_context = TAUploadContext(
        commit_sha=commit_sha,
        branch="feature-branch",
        pull_id=67890,
        pipeline=UploadPipeline.CODECOV,
    )

    for instance in test_instances:
        insert_testrun(
            timestamp=timezone.now(),
            repo_id=repo_id,
            commit_sha=commit_sha,
            branch=upload_context["branch"],
            upload_id=instance.upload_id,
            flags=[
                test.flags_hash
                for test in [test1, test2, test3]
                if test.id_ == instance.test_id
            ],
            parsing_info={
                "framework": "Pytest",
                "testruns": [
                    {
                        "name": instance.test_id,
                        "classname": "test_classname",
                        "computed_name": "computed_name",
                        "duration": instance.duration_seconds,
                        "outcome": instance.outcome,
                        "testsuite": test_suite,
                        "failure_message": instance.failure_message,
                        "filename": "test_filename",
                        "build_url": None,
                    }
                ],
            },
        )

        dbsession.add(instance)

    dbsession.flush()

    return {
        "repository": repository,
        "upload_context": upload_context,
        "test_instances": test_instances,
        "commit_sha": commit_sha,
    }


@pytest.mark.django_db(databases=["default", "ta_timeseries"], transaction=True)
class TestTestAnalyticsNotifierTask:
    def test_run_impl_initial_run_acquires_fencing_token_and_retries(
        self,
        test_setup,
        mock_redis,
        mock_lock_manager,
    ):
        """Test that initial run acquires fencing token and retries"""
        task = TestAnalyticsNotifierTask()
        task.retry = MagicMock(side_effect=Retry("Retrying"))

        mock_pipeline = MagicMock()
        mock_pipeline.execute.return_value = [
            1
        ]  # incr returns 1, expire returns None (implicit)
        mock_redis.pipeline.return_value.__enter__.return_value = mock_pipeline
        mock_redis.pipeline.return_value.__exit__.return_value = None

        mock_lock_manager.locked.return_value.__enter__ = MagicMock()
        mock_lock_manager.locked.return_value.__exit__ = MagicMock(return_value=None)

        with pytest.raises(Retry):
            task.run_impl(
                None,
                repoid=test_setup["repository"].repoid,
                upload_context=test_setup["upload_context"],
            )

        # Verify fencing token was acquired
        expected_token_key = TA_NOTIFIER_FENCING_TOKEN.format(
            test_setup["repository"].repoid,
            test_setup["upload_context"]["commit_sha"],
        )
        mock_pipeline.incr.assert_called_once_with(expected_token_key)
        mock_pipeline.expire.assert_called_once_with(expected_token_key, 24 * 60 * 60)
        mock_pipeline.execute.assert_called_once()

        # Verify retry was called with correct countdown and fencing token
        task.retry.assert_called_once()
        _, retry_kwargs = task.retry.call_args
        task_kwargs = retry_kwargs["kwargs"]
        assert retry_kwargs["countdown"] == DEBOUNCE_PERIOD_SECONDS
        assert task_kwargs["fencing_token"] == 1

    def test_run_impl_with_fencing_token_stale_token_exits_early(
        self,
        test_setup,
        mock_redis,
        mock_lock_manager,
    ):
        """Test that stale fencing token causes early exit"""
        task = TestAnalyticsNotifierTask()

        mock_redis.get.return_value = 2

        mock_lock_manager.locked.return_value.__enter__ = MagicMock()
        mock_lock_manager.locked.return_value.__exit__ = MagicMock(return_value=None)

        result = task.run_impl(
            None,
            repoid=test_setup["repository"].repoid,
            upload_context=test_setup["upload_context"],
            fencing_token=1,  # Lower than current token
        )

        assert result == NotifierTaskResult(attempted=False, succeeded=False)

    def test_run_impl_stale_token_on_second_check(
        self,
        mocker,
        test_setup,
        mock_redis,
        mock_lock_manager,
        mock_test_analytics_services,
    ):
        """Test that stale fencing token on second check (after preparation) causes early exit"""
        task = TestAnalyticsNotifierTask()

        # Mock Redis to return same token on first check, higher token on second check
        mock_redis.get.side_effect = [
            1,
            2,
        ]

        mock_lock_manager.locked.return_value.__enter__ = MagicMock()
        mock_lock_manager.locked.return_value.__exit__ = MagicMock(return_value=None)

        mock_pull = ProviderPull(
            author={"id": "1", "username": "test-user"},
            base={"branch": "main", "commitid": "base-sha"},
            head={
                "branch": "feature",
                "commitid": test_setup["upload_context"]["commit_sha"],
            },
            state="open",
            title="Test PR",
            id="1",
            number="1",
            merge_commit_sha=None,
        )
        mock_test_analytics_services[
            "fetch_pull_request_information"
        ].return_value = mock_pull

        mock_notifier_class = mocker.patch(
            "tasks.test_analytics_notifier.TestResultsNotifier"
        )
        mock_notifier_instance = MagicMock()
        mock_notifier_class.return_value = mock_notifier_instance

        result = task.run_impl(
            None,
            repoid=test_setup["repository"].repoid,
            upload_context=test_setup["upload_context"],
            fencing_token=1,  # Same as first check, but lower than second check
        )

        # Should exit early due to stale token on second check
        assert result == NotifierTaskResult(attempted=False, succeeded=False)

        # Verify that notification preparation was called but notify was not
        mock_notifier_class.assert_called_once()
        mock_notifier_instance.notify.assert_not_called()

    def test_run_impl_notification_preparation_returns_none(
        self,
        mocker,
        dbsession,
        test_setup,
        mock_redis,
        mock_lock_manager,
    ):
        """Test that when notification preparation returns None, task returns appropriate result"""
        task = TestAnalyticsNotifierTask()

        mock_redis.get.return_value = 1

        mock_lock_manager.locked.return_value.__enter__ = MagicMock()
        mock_lock_manager.locked.return_value.__exit__ = MagicMock(return_value=None)

        mocker.patch.object(task, "notification_preparation", return_value=None)

        result = task.run_impl(
            dbsession,
            repoid=test_setup["repository"].repoid,
            upload_context=test_setup["upload_context"],
            fencing_token=1,
        )

        assert result == NotifierTaskResult(attempted=False, succeeded=False)

    def test_run_impl_successful_notification(
        self,
        mocker,
        dbsession,
        test_setup,
        mock_redis,
        mock_lock_manager,
    ):
        """Test successful notification flow"""
        task = TestAnalyticsNotifierTask()

        mock_redis.get.return_value = 1

        mock_lock_manager.locked.return_value.__enter__ = MagicMock()
        mock_lock_manager.locked.return_value.__exit__ = MagicMock(return_value=None)

        mock_notifier = MagicMock()
        mock_notifier.notify.return_value = NotifierResult.COMMENT_POSTED
        mocker.patch.object(
            task, "notification_preparation", return_value=mock_notifier
        )

        result = task.run_impl(
            dbsession,
            repoid=test_setup["repository"].repoid,
            upload_context=test_setup["upload_context"],
            fencing_token=1,
        )

        assert result == NotifierTaskResult(attempted=True, succeeded=True)
        mock_notifier.notify.assert_called_once()

    def test_run_impl_failed_notification(
        self,
        mocker,
        dbsession,
        test_setup,
        mock_redis,
        mock_lock_manager,
    ):
        """Test failed notification flow"""
        task = TestAnalyticsNotifierTask()

        mock_redis.get.return_value = 1

        mock_lock_manager.locked.return_value.__enter__ = MagicMock()
        mock_lock_manager.locked.return_value.__exit__ = MagicMock(return_value=None)

        mock_notifier = MagicMock()
        mock_notifier.notify.return_value = NotifierResult.NO_COMMENT
        mocker.patch.object(
            task, "notification_preparation", return_value=mock_notifier
        )

        result = task.run_impl(
            dbsession,
            repoid=test_setup["repository"].repoid,
            upload_context=test_setup["upload_context"],
            fencing_token=1,
        )

        assert result == NotifierTaskResult(attempted=True, succeeded=False)

    def test_notification_lock_handles_lock_retry(
        self,
        mock_lock_manager,
    ):
        """Test that _notification_lock handles LockRetry exceptions"""
        task = TestAnalyticsNotifierTask()
        task.retry = MagicMock(side_effect=Retry("Retrying"))

        mock_lock_manager.locked.side_effect = LockRetry(countdown=10)

        with pytest.raises(Retry):
            with task._notification_lock(mock_lock_manager):
                pass

        task.retry.assert_called_once_with(max_retries=5, countdown=10)

    def test_check_fencing_token_current_token(
        self,
        test_setup,
        mock_redis,
    ):
        """Test _check_fencing_token with current token"""
        task = TestAnalyticsNotifierTask()

        mock_redis.get.return_value = 1

        result = task._check_fencing_token(
            mock_redis,
            test_setup["repository"].repoid,
            test_setup["upload_context"],
            1,
        )

        assert result is None

    def test_check_fencing_token_stale_token(
        self,
        test_setup,
        mock_redis,
    ):
        """Test _check_fencing_token with stale token"""
        task = TestAnalyticsNotifierTask()
        task.extra_dict = {}  # Initialize for logging

        mock_redis.get.return_value = 2

        result = task._check_fencing_token(
            mock_redis,
            test_setup["repository"].repoid,
            test_setup["upload_context"],
            1,
        )

        assert result == NotifierTaskResult(attempted=False, succeeded=False)

    def test_notification_preparation_comment_disabled(
        self,
        test_setup,
    ):
        """Test notification_preparation when comment is disabled"""
        task = TestAnalyticsNotifierTask()
        task.extra_dict = {}  # Initialize for logging

        commit_yaml = UserYaml({"comment": False})

        result = task.notification_preparation(
            repo_id=test_setup["repository"].repoid,
            upload_context=test_setup["upload_context"],
            commit_yaml=commit_yaml,
        )

        assert result is None

    def test_notification_preparation_no_failures(
        self,
        test_setup,
    ):
        """Test notification_preparation when there are no failures"""
        task = TestAnalyticsNotifierTask()
        task.extra_dict = {}  # Initialize for logging

        # Change all testruns to passing
        Testrun.objects.update(outcome="pass")

        result = task.notification_preparation(
            repo_id=test_setup["repository"].repoid,
            upload_context=test_setup["upload_context"],
        )

        # Should return None since there are no failures
        assert result is None

    def test_notification_preparation_no_pull(
        self,
        test_setup,
        mock_test_analytics_services,
    ):
        """Test notification_preparation when no pull request is found"""
        task = TestAnalyticsNotifierTask()
        task.extra_dict = {}  # Initialize for logging

        mock_test_analytics_services[
            "fetch_pull_request_information"
        ].return_value = None

        result = task.notification_preparation(
            repo_id=test_setup["repository"].repoid,
            upload_context=test_setup["upload_context"],
        )

        assert result is None

    def test_notification_preparation_success(
        self,
        test_setup,
        mock_test_analytics_services,
    ):
        task = TestAnalyticsNotifierTask()
        task.extra_dict = {}  # Initialize for logging

        mock_pull = ProviderPull(
            author={"id": "1", "username": "test-user"},
            base={"branch": "main", "commitid": "base-sha"},
            head={
                "branch": "feature-branch",
                "commitid": test_setup["upload_context"]["commit_sha"],
            },
            state="open",
            title="Test PR",
            id="1",
            number="1",
            merge_commit_sha=None,
        )
        mock_test_analytics_services[
            "fetch_pull_request_information"
        ].return_value = mock_pull

        result = task.notification_preparation(
            repo_id=test_setup["repository"].repoid,
            upload_context=test_setup["upload_context"],
        )

        assert result.repo == test_setup["repository"]
        assert result.payload.failed == 3
        assert result.payload.passed == 0
        assert result.payload.skipped == 0
        assert len(result.payload.info.failures) == 3
        assert result.payload.info.flaky_tests == {}

    def test_notification_preparation_success_sentry_pipeline(
        self,
        mocker,
        test_setup,
        mock_test_analytics_services,
    ):
        """Test successful notification_preparation with Sentry pipeline"""
        task = TestAnalyticsNotifierTask()
        task.extra_dict = {}  # Initialize for logging

        upload_context = TAUploadContext(
            commit_sha=test_setup["upload_context"]["commit_sha"],
            branch=test_setup["upload_context"]["branch"],
            pull_id=test_setup["upload_context"]["pull_id"],
            pipeline=UploadPipeline.SENTRY,
        )

        mock_pull = ProviderPull(
            author={"id": "1", "username": "test-user"},
            base={"branch": "main", "commitid": "base-sha"},
            head={
                "branch": "feature",
                "commitid": test_setup["upload_context"]["commit_sha"],
            },
            state="open",
            title="Test PR",
            id="1",
            number="1",
            merge_commit_sha=None,
        )
        mock_test_analytics_services[
            "fetch_pull_request_information"
        ].return_value = mock_pull

        mocker.patch(
            "tasks.test_analytics_notifier.settings.GITHUB_SENTRY_APP_NAME",
            "sentry-app",
        )

        result = task.notification_preparation(
            repo_id=test_setup["repository"].repoid,
            upload_context=upload_context,
        )

        assert result.repo == test_setup["repository"]
        assert result.payload.failed == 3
        assert result.payload.passed == 0
        assert result.payload.skipped == 0
        assert len(result.payload.info.failures) == 3
        assert result.payload.info.flaky_tests == {}

    def test_notification_preparation_with_flaky_detection(
        self,
        test_setup,
        mock_test_analytics_services,
    ):
        """Test notification_preparation with flaky test detection enabled"""
        task = TestAnalyticsNotifierTask()
        task.extra_dict = {}  # Initialize for logging

        mock_pull = ProviderPull(
            author={"id": "1", "username": "test-user"},
            base={"branch": "main", "commitid": "base-sha"},
            head={
                "branch": "feature",
                "commitid": test_setup["upload_context"]["commit_sha"],
            },
            state="open",
            title="Test PR",
            id="1",
            number="1",
            merge_commit_sha=None,
        )
        mock_test_analytics_services[
            "fetch_pull_request_information"
        ].return_value = mock_pull

        mock_flaky_tests = {"test_id": {"test_name": "flaky_test"}}
        mock_test_analytics_services[
            "get_flaky_tests_dict"
        ].return_value = mock_flaky_tests

        result = task.notification_preparation(
            repo_id=test_setup["repository"].repoid,
            upload_context=test_setup["upload_context"],
        )

        assert result.repo == test_setup["repository"]
        assert result.payload.failed == 3
        assert result.payload.passed == 0
        assert result.payload.skipped == 0
        assert len(result.payload.info.failures) == 3
        assert result.payload.info.flaky_tests == mock_flaky_tests

        # Verify flaky detection was called
        mock_test_analytics_services["should_do_flaky_detection"].assert_called_once()
        mock_test_analytics_services["get_flaky_tests_dict"].assert_called_once_with(
            test_setup["repository"].repoid
        )

    def test_notification_preparation_without_flaky_detection(
        self,
        test_setup,
        mock_test_analytics_services,
    ):
        """Test notification_preparation with flaky test detection disabled"""
        task = TestAnalyticsNotifierTask()
        task.extra_dict = {}  # Initialize for logging

        mock_pull = ProviderPull(
            author={"id": "1", "username": "test-user"},
            base={"branch": "main", "commitid": "base-sha"},
            head={
                "branch": "feature",
                "commitid": test_setup["upload_context"]["commit_sha"],
            },
            state="open",
            title="Test PR",
            id="1",
            number="1",
            merge_commit_sha=None,
        )
        mock_test_analytics_services[
            "fetch_pull_request_information"
        ].return_value = mock_pull

        # Disable flaky detection
        mock_test_analytics_services["should_do_flaky_detection"].return_value = False

        result = task.notification_preparation(
            repo_id=test_setup["repository"].repoid,
            upload_context=test_setup["upload_context"],
        )

        assert result.repo == test_setup["repository"]
        assert result.payload.failed == 3
        assert result.payload.passed == 0
        assert result.payload.skipped == 0
        assert len(result.payload.info.failures) == 3
        assert result.payload.info.flaky_tests == {}

        # Verify flaky detection was checked but get_flaky_tests_dict was not called
        mock_test_analytics_services["should_do_flaky_detection"].assert_called_once()
        mock_test_analytics_services["get_flaky_tests_dict"].assert_not_called()

    def test_integration_flow(
        self,
        mocker,
        test_setup,
        mock_redis,
        mock_test_analytics_services,
    ):
        """Test integration flow from start to finish"""
        task = TestAnalyticsNotifierTask()

        mock_redis.get.return_value = 1

        mock_pull = ProviderPull(
            author={"id": "1", "username": "test-user"},
            base={"branch": "main", "commitid": "base-sha"},
            head={
                "branch": "feature",
                "commitid": test_setup["upload_context"]["commit_sha"],
            },
            state="open",
            title="Test PR",
            id="1",
            number="1",
            merge_commit_sha=None,
        )
        mock_test_analytics_services[
            "fetch_pull_request_information"
        ].return_value = mock_pull

        mock_notifier_class = mocker.patch(
            "tasks.test_analytics_notifier.TestResultsNotifier"
        )
        mock_notifier_instance = MagicMock()
        mock_notifier_instance.notify.return_value = NotifierResult.COMMENT_POSTED
        mock_notifier_class.return_value = mock_notifier_instance

        mock_lock_manager = mocker.patch("tasks.test_analytics_notifier.LockManager")
        mock_lock_context = MagicMock()
        mock_lock_context.__enter__ = MagicMock()
        mock_lock_context.__exit__ = MagicMock(return_value=None)
        mock_lock_manager.return_value.locked.return_value = mock_lock_context

        result = task.run_impl(
            None,
            repoid=test_setup["repository"].repoid,
            upload_context=test_setup["upload_context"],
            fencing_token=1,
        )

        assert result == NotifierTaskResult(attempted=True, succeeded=True)

        # Verify the notification was attempted
        mock_notifier_instance.notify.assert_called_once()


class TestTransformFailures:
    """Test the transform_failures function"""

    def test_transform_failures_empty_list(self):
        result = transform_failures([])
        assert result == []

    def test_transform_failures_single_failure_with_message(self, mocker):
        """Test transform_failures with single failure that has a failure message"""

        mock_shorten = mocker.patch(
            "tasks.test_analytics_notifier.shorten_file_paths",
            return_value="shortened/path/test.py:10\nTraceback message\r\nWith carriage returns",
        )

        failure = FailedTestInstance(
            computed_name="test_example",
            test_id="test_id_123",
            failure_message="/very/long/path/to/test.py:10\nTraceback message\r\nWith carriage returns",
            flags=["flag1", "flag2"],
            duration_seconds=1.5,
        )

        result = transform_failures([failure])

        assert len(result) == 1
        notification_failure = result[0]

        assert isinstance(notification_failure, TestResultsNotificationFailure)
        assert notification_failure.display_name == "test_example"
        assert notification_failure.test_id == "test_id_123"
        assert (
            notification_failure.failure_message
            == "shortened/path/test.py:10\nTraceback message\nWith carriage returns"
        )
        assert notification_failure.envs == ["flag1", "flag2"]
        assert notification_failure.duration_seconds == 1.5
        assert notification_failure.build_url is None

        mock_shorten.assert_called_once_with(
            "/very/long/path/to/test.py:10\nTraceback message\r\nWith carriage returns"
        )

    def test_transform_failures_single_failure_no_message(self):
        """Test transform_failures with single failure that has no failure message"""
        failure = FailedTestInstance(
            computed_name="test_no_failure_message",
            test_id="test_id_456",
            failure_message=None,
            flags=["integration"],
            duration_seconds=2.0,
        )

        result = transform_failures([failure])

        assert len(result) == 1
        notification_failure = result[0]

        assert notification_failure.display_name == "test_no_failure_message"
        assert notification_failure.test_id == "test_id_456"
        assert notification_failure.failure_message is None
        assert notification_failure.envs == ["integration"]
        assert notification_failure.duration_seconds == 2.0
        assert notification_failure.build_url is None

    def test_transform_failures_none_duration(self):
        """Test transform_failures with None duration_seconds"""
        failure = FailedTestInstance(
            computed_name="test_none_duration",
            test_id="test_id_789",
            failure_message="Simple failure message",
            flags=[],
            duration_seconds=None,
        )

        result = transform_failures([failure])

        assert len(result) == 1
        notification_failure = result[0]

        assert notification_failure.duration_seconds == 0  # Should default to 0

    def test_transform_failures_multiple_failures(self, mocker):
        """Test transform_failures with multiple failures"""

        def mock_shorten_side_effect(path):
            return path.replace("/long/path/", "/short/")

        mocker.patch(
            "tasks.test_analytics_notifier.shorten_file_paths",
            side_effect=mock_shorten_side_effect,
        )

        failures = [
            FailedTestInstance(
                computed_name="test_one",
                test_id="id_1",
                failure_message="/long/path/test1.py:5\nError in test one\r",
                flags=["unit"],
                duration_seconds=0.5,
            ),
            FailedTestInstance(
                computed_name="test_two",
                test_id="id_2",
                failure_message=None,
                flags=["integration", "slow"],
                duration_seconds=3.2,
            ),
            FailedTestInstance(
                computed_name="test_three",
                test_id="id_3",
                failure_message="/long/path/test3.py:15\nAnother error\r\n",
                flags=[],
                duration_seconds=None,
            ),
        ]

        result = transform_failures(failures)

        assert len(result) == 3

        # Check first failure
        assert result[0].display_name == "test_one"
        assert result[0].test_id == "id_1"
        assert result[0].failure_message == "/short/test1.py:5\nError in test one"
        assert result[0].envs == ["unit"]
        assert result[0].duration_seconds == 0.5

        # Check second failure
        assert result[1].display_name == "test_two"
        assert result[1].test_id == "id_2"
        assert result[1].failure_message is None
        assert result[1].envs == ["integration", "slow"]
        assert result[1].duration_seconds == 3.2

        # Check third failure
        assert result[2].display_name == "test_three"
        assert result[2].test_id == "id_3"
        assert result[2].failure_message == "/short/test3.py:15\nAnother error\n"
        assert result[2].envs == []
        assert result[2].duration_seconds == 0

    def test_transform_failures_carriage_return_removal(self, mocker):
        """Test that carriage returns are properly removed from failure messages"""
        mocker.patch(
            "tasks.test_analytics_notifier.shorten_file_paths",
            side_effect=lambda x: x,  # Return input unchanged
        )

        failure = FailedTestInstance(
            computed_name="test_carriage_returns",
            test_id="test_cr",
            failure_message="Line 1\r\nLine 2\rLine 3\r\n\rEnd",
            flags=[],
            duration_seconds=1.0,
        )

        result = transform_failures([failure])

        assert result[0].failure_message == "Line 1\nLine 2Line 3\nEnd"

    def test_transform_failures_empty_string_message(self, mocker):
        """Test transform_failures with empty string failure message"""
        mock_shorten = mocker.patch(
            "tasks.test_analytics_notifier.shorten_file_paths",
            return_value="",
        )

        failure = FailedTestInstance(
            computed_name="test_empty_message",
            test_id="test_empty",
            failure_message="",
            flags=["unit"],
            duration_seconds=0.1,
        )

        result = transform_failures([failure])

        assert len(result) == 1
        assert result[0].failure_message == ""
        mock_shorten.assert_called_once_with("")
