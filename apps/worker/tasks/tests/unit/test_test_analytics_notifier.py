from contextlib import contextmanager

import pytest
from celery.exceptions import MaxRetriesExceededError as CeleryMaxRetriesExceededError
from celery.exceptions import Retry

from database.tests.factories import CommitFactory
from helpers.notifier import NotifierResult
from services.lock_manager import LockRetry
from services.notification.debounce import (
    SKIP_DEBOUNCE_TOKEN,
    LockAcquisitionLimitError,
)
from services.test_analytics.ta_timeseries import FailedTestInstance
from services.test_results import NotifierTaskResult
from tasks.test_analytics_notifier import (
    TestAnalyticsNotifierTask,
    transform_failures,
)


@pytest.mark.django_db
class TestTestAnalyticsNotifierTask:
    def test_lock_retry_will_retry(self, mocker, dbsession, mock_redis):
        """Test that LockRetry calls self.retry() when retries are available"""
        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()

        upload_context = {
            "commit_sha": commit.commitid,
            "branch": "test-branch",
            "pull_id": 123,
            "pipeline": "codecov",
        }

        mocker.patch(
            "tasks.test_analytics_notifier.get_redis_connection",
            return_value=mock_redis,
        )

        m = mocker.MagicMock()
        m.return_value.locked.return_value.__enter__.side_effect = LockRetry(60)
        mocker.patch("tasks.test_analytics_notifier.LockManager", m)

        mock_redis.pipeline.return_value.__enter__.return_value.execute.return_value = [
            "1"
        ]

        task = TestAnalyticsNotifierTask()
        task.request.retries = 0
        task.request.headers = {}
        task.max_retries = 5

        with pytest.raises(Retry):
            task.run_impl(
                dbsession,
                repoid=commit.repoid,
                upload_context=upload_context,
            )

    def test_lock_retry_self_retry_raises_max_retries_exceeded(
        self, mocker, dbsession, mock_redis
    ):
        """Test that when self.retry() raises MaxRetriesExceededError inside except LockRetry,
        it returns graceful failure result"""
        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()

        upload_context = {
            "commit_sha": commit.commitid,
            "branch": "test-branch",
            "pull_id": 123,
            "pipeline": "codecov",
        }

        mocker.patch(
            "tasks.test_analytics_notifier.get_redis_connection",
            return_value=mock_redis,
        )

        m = mocker.MagicMock()
        m.return_value.locked.return_value.__enter__.side_effect = LockRetry(60)
        mocker.patch("tasks.test_analytics_notifier.LockManager", m)

        mock_redis.pipeline.return_value.__enter__.return_value.execute.return_value = [
            "1"
        ]

        task = TestAnalyticsNotifierTask()
        task.request.retries = 0
        task.request.headers = {}
        task.max_retries = 5

        def retry_side_effect(*args, **kwargs):
            if kwargs.get("max_retries") == 5:
                raise CeleryMaxRetriesExceededError()
            raise Retry()

        mocker.patch.object(task, "retry", side_effect=retry_side_effect)

        result = task.run_impl(
            dbsession,
            repoid=commit.repoid,
            upload_context=upload_context,
        )

        assert result == NotifierTaskResult(
            attempted=False,
            succeeded=False,
        )

    def test_max_retries_exceeded_from_notification_lock(
        self, mocker, dbsession, mock_redis
    ):
        """Test that LockAcquisitionLimitError from notification_lock returns graceful failure"""
        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()

        upload_context = {
            "commit_sha": commit.commitid,
            "branch": "test-branch",
            "pull_id": 123,
            "pipeline": "codecov",
        }

        mocker.patch(
            "tasks.test_analytics_notifier.get_redis_connection",
            return_value=mock_redis,
        )

        m = mocker.MagicMock()
        lock_retry_with_max_exceeded = LockRetry(
            countdown=0,
            max_retries_exceeded=True,
            retry_num=5,
            max_attempts=5,
            lock_name="notification_lock",
            repoid=commit.repoid,
            commitid=commit.commitid,
        )
        m.return_value.locked.return_value.__enter__.side_effect = (
            lock_retry_with_max_exceeded
        )
        mocker.patch("tasks.test_analytics_notifier.LockManager", m)

        mock_redis.pipeline.return_value.__enter__.return_value.execute.return_value = [
            "1"
        ]

        task = TestAnalyticsNotifierTask()
        task.request.retries = 0
        task.request.headers = {}
        task.max_retries = 5

        result = task.run_impl(
            dbsession,
            repoid=commit.repoid,
            upload_context=upload_context,
        )

        assert result == NotifierTaskResult(
            attempted=False,
            succeeded=False,
        )

    def test_lock_retry_self_retry_raises_max_retries_exceeded_fencing_token_check(
        self, mocker, dbsession, mock_redis
    ):
        """Test that when self.retry() raises MaxRetriesExceededError during fencing token check,
        it returns graceful failure result"""
        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()

        upload_context = {
            "commit_sha": commit.commitid,
            "branch": "test-branch",
            "pull_id": 123,
            "pipeline": "codecov",
        }

        mocker.patch(
            "tasks.test_analytics_notifier.get_redis_connection",
            return_value=mock_redis,
        )

        lock_manager_mock = mocker.MagicMock()
        lock_manager_mock.return_value.locked.return_value.__enter__.side_effect = (
            LockRetry(60)
        )
        mocker.patch("tasks.test_analytics_notifier.LockManager", lock_manager_mock)

        mock_redis.pipeline.return_value.__enter__.return_value.execute.return_value = [
            "1"
        ]
        mock_redis.get.return_value = "1"

        task = TestAnalyticsNotifierTask()
        task.request.retries = 0
        task.request.headers = {}
        task.max_retries = 5

        def retry_side_effect(*args, **kwargs):
            if kwargs.get("max_retries") == 5:
                raise CeleryMaxRetriesExceededError()
            raise Retry()

        mocker.patch.object(task, "retry", side_effect=retry_side_effect)

        result = task.run_impl(
            dbsession,
            repoid=commit.repoid,
            upload_context=upload_context,
            fencing_token=SKIP_DEBOUNCE_TOKEN,
        )

        assert result == NotifierTaskResult(
            attempted=False,
            succeeded=False,
        )

    def test_lock_retry_self_retry_raises_max_retries_exceeded_notification_send(
        self, mocker, dbsession, mock_redis
    ):
        """Test that when self.retry() raises MaxRetriesExceededError during notification send,
        it returns graceful failure result"""
        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()

        upload_context = {
            "commit_sha": commit.commitid,
            "branch": "test-branch",
            "pull_id": 123,
            "pipeline": "codecov",
        }

        mocker.patch(
            "tasks.test_analytics_notifier.get_redis_connection",
            return_value=mock_redis,
        )

        lock_manager_mock = mocker.MagicMock()
        context_manager = mocker.MagicMock()

        def lock_side_effect(*args, **kwargs):
            if not hasattr(lock_side_effect, "call_count"):
                lock_side_effect.call_count = 0
            lock_side_effect.call_count += 1
            if lock_side_effect.call_count == 1:
                return context_manager
            else:
                raise LockRetry(60)

        lock_manager_mock.return_value.locked.return_value.__enter__.side_effect = (
            lock_side_effect
        )
        mocker.patch("tasks.test_analytics_notifier.LockManager", lock_manager_mock)

        mock_redis.pipeline.return_value.__enter__.return_value.execute.return_value = [
            "1"
        ]
        mock_redis.get.return_value = "1"

        task = TestAnalyticsNotifierTask()
        task.request.retries = 0
        task.request.headers = {}
        task.max_retries = 5

        def retry_side_effect(*args, **kwargs):
            if kwargs.get("max_retries") == 5:
                raise CeleryMaxRetriesExceededError()
            raise Retry()

        mocker.patch.object(task, "retry", side_effect=retry_side_effect)

        mock_notifier = mocker.MagicMock()
        mock_notifier.notify.return_value = NotifierResult.COMMENT_POSTED
        mocker.patch.object(
            task, "notification_preparation", return_value=mock_notifier
        )

        result = task.run_impl(
            dbsession,
            repoid=commit.repoid,
            upload_context=upload_context,
            fencing_token=SKIP_DEBOUNCE_TOKEN,
        )

        assert result == NotifierTaskResult(
            attempted=False,
            succeeded=False,
        )

    def test_debounce_retry_includes_all_required_params(
        self, mocker, dbsession, mock_redis
    ):
        """Test that retry kwargs include all required parameters for debounce retry.

        This test ensures that when the debounce logic triggers a retry, all required
        parameters are included in the kwargs passed to self.retry(). This prevents
        TypeError when Celery attempts to retry the task.
        """
        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()

        upload_context = {
            "commit_sha": commit.commitid,
            "branch": "test-branch",
            "pull_id": 123,
            "pipeline": "codecov",
        }

        mocker.patch(
            "tasks.test_analytics_notifier.get_redis_connection",
            return_value=mock_redis,
        )

        # Mock Redis to allow token acquisition
        mock_redis.pipeline.return_value.__enter__.return_value.execute.return_value = [
            "1"
        ]
        mock_redis.get.return_value = False

        task = TestAnalyticsNotifierTask()
        task.request.retries = 0
        task.max_retries = 5

        # Mock debouncer's notification_lock to succeed
        @contextmanager
        def mock_lock_succeeds(*args, **kwargs):
            yield

        mocker.patch.object(
            task.debouncer, "notification_lock", side_effect=mock_lock_succeeds
        )

        # Mock retry to capture kwargs
        mock_retry = mocker.patch.object(task, "retry", side_effect=Retry())

        with pytest.raises(Retry):
            task.run_impl(
                dbsession,
                repoid=commit.repoid,
                upload_context=upload_context,
                # Don't pass fencing_token - let it go through retry path
            )

        # Verify retry was called
        assert mock_retry.called

        # CRITICAL: Verify all required params are in kwargs
        call_kwargs = mock_retry.call_args[1]["kwargs"]
        assert call_kwargs["repoid"] == commit.repoid
        assert call_kwargs["upload_context"] == upload_context
        assert call_kwargs["fencing_token"] == 1  # Token from acquisition
        assert mock_retry.call_args[1]["countdown"] == 30  # DEBOUNCE_PERIOD_SECONDS

    def test_max_retries_exceeded_during_debounce_retry(
        self, mocker, dbsession, mock_redis
    ):
        """Test that max retries exceeded during debounce retry proceeds with notification."""
        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()

        upload_context = {
            "commit_sha": commit.commitid,
            "branch": "test-branch",
            "pull_id": 123,
            "pipeline": "codecov",
        }

        mocker.patch(
            "tasks.test_analytics_notifier.get_redis_connection",
            return_value=mock_redis,
        )

        # Mock Redis to allow token acquisition
        mock_redis.pipeline.return_value.__enter__.return_value.execute.return_value = [
            "1"
        ]
        mock_redis.get.return_value = "1"  # Non-stale token

        task = TestAnalyticsNotifierTask()
        task.request.retries = 0
        task.max_retries = 5

        # Mock debouncer's notification_lock to succeed
        @contextmanager
        def mock_lock_succeeds(*args, **kwargs):
            yield

        mocker.patch.object(
            task.debouncer, "notification_lock", side_effect=mock_lock_succeeds
        )
        mocker.patch.object(
            task.debouncer, "check_fencing_token_stale", return_value=False
        )

        # Mock retry to raise CeleryMaxRetriesExceededError during debounce retry
        def retry_side_effect(*args, **kwargs):
            if kwargs.get("countdown") == 30:  # Debounce retry
                raise CeleryMaxRetriesExceededError()
            raise Retry()

        mocker.patch.object(task, "retry", side_effect=retry_side_effect)

        # Mock notification_preparation to return a mock notifier
        mock_notifier = mocker.MagicMock()
        mock_notifier.notify.return_value = NotifierResult.COMMENT_POSTED
        mocker.patch.object(
            task, "notification_preparation", return_value=mock_notifier
        )

        result = task.run_impl(
            dbsession,
            repoid=commit.repoid,
            upload_context=upload_context,
        )

        # Should proceed with notification despite max retries exceeded
        assert result == NotifierTaskResult(
            attempted=True,
            succeeded=True,
        )

    def test_stale_fencing_token_during_check(self, mocker, dbsession, mock_redis):
        """Test that stale fencing token causes task to exit early."""
        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()

        upload_context = {
            "commit_sha": commit.commitid,
            "branch": "test-branch",
            "pull_id": 123,
            "pipeline": "codecov",
        }

        mocker.patch(
            "tasks.test_analytics_notifier.get_redis_connection",
            return_value=mock_redis,
        )

        # Mock Redis to return stale token (current token > fencing_token)
        mock_redis.get.return_value = "2"  # Current token is 2, but we have token 1

        task = TestAnalyticsNotifierTask()
        task.request.retries = 0
        task.max_retries = 5

        # Mock debouncer's notification_lock to succeed
        @contextmanager
        def mock_lock_succeeds(*args, **kwargs):
            yield

        mocker.patch.object(
            task.debouncer, "notification_lock", side_effect=mock_lock_succeeds
        )

        # Mock check_fencing_token_stale to return True (token is stale)
        mocker.patch.object(
            task.debouncer, "check_fencing_token_stale", return_value=True
        )

        result = task.run_impl(
            dbsession,
            repoid=commit.repoid,
            upload_context=upload_context,
            fencing_token=1,  # Pass fencing token, but it's stale
        )

        assert result == NotifierTaskResult(
            attempted=False,
            succeeded=False,
        )

    def test_stale_fencing_token_during_notification(
        self, mocker, dbsession, mock_redis
    ):
        """Test that stale fencing token during notification causes task to exit early."""
        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()

        upload_context = {
            "commit_sha": commit.commitid,
            "branch": "test-branch",
            "pull_id": 123,
            "pipeline": "codecov",
        }

        mocker.patch(
            "tasks.test_analytics_notifier.get_redis_connection",
            return_value=mock_redis,
        )

        # Mock Redis to return non-stale token initially, then stale token
        mock_redis.get.return_value = "2"  # Current token is 2, but we have token 1

        task = TestAnalyticsNotifierTask()
        task.request.retries = 0
        task.max_retries = 5

        # Mock debouncer's notification_lock to succeed
        @contextmanager
        def mock_lock_succeeds(*args, **kwargs):
            yield

        mocker.patch.object(
            task.debouncer, "notification_lock", side_effect=mock_lock_succeeds
        )

        # Mock check_fencing_token_stale to return False initially, then True
        call_count = {"count": 0}

        def stale_check_side_effect(*args, **kwargs):
            call_count["count"] += 1
            if call_count["count"] == 1:
                return False  # First check passes
            return True  # Second check (during notification) fails

        mocker.patch.object(
            task.debouncer,
            "check_fencing_token_stale",
            side_effect=stale_check_side_effect,
        )

        # Mock notification_preparation to return a mock notifier
        mock_notifier = mocker.MagicMock()
        mock_notifier.notify.return_value = NotifierResult.COMMENT_POSTED
        mocker.patch.object(
            task, "notification_preparation", return_value=mock_notifier
        )

        result = task.run_impl(
            dbsession,
            repoid=commit.repoid,
            upload_context=upload_context,
            fencing_token=1,
        )

        # Should exit early due to stale token
        assert result == NotifierTaskResult(
            attempted=False,
            succeeded=False,
        )

    def test_successful_notification_flow(self, mocker, dbsession, mock_redis):
        """Test successful notification flow with valid fencing token."""
        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()

        upload_context = {
            "commit_sha": commit.commitid,
            "branch": "test-branch",
            "pull_id": 123,
            "pipeline": "codecov",
        }

        mocker.patch(
            "tasks.test_analytics_notifier.get_redis_connection",
            return_value=mock_redis,
        )

        # Mock Redis to return non-stale token
        mock_redis.get.return_value = "1"  # Current token matches fencing_token

        task = TestAnalyticsNotifierTask()
        task.request.retries = 0
        task.max_retries = 5

        # Mock debouncer's notification_lock to succeed
        @contextmanager
        def mock_lock_succeeds(*args, **kwargs):
            yield

        mocker.patch.object(
            task.debouncer, "notification_lock", side_effect=mock_lock_succeeds
        )
        mocker.patch.object(
            task.debouncer, "check_fencing_token_stale", return_value=False
        )

        # Mock notification_preparation to return a mock notifier
        mock_notifier = mocker.MagicMock()
        mock_notifier.notify.return_value = NotifierResult.COMMENT_POSTED
        mocker.patch.object(
            task, "notification_preparation", return_value=mock_notifier
        )

        result = task.run_impl(
            dbsession,
            repoid=commit.repoid,
            upload_context=upload_context,
            fencing_token=1,
        )

        assert result == NotifierTaskResult(
            attempted=True,
            succeeded=True,
        )
        mock_notifier.notify.assert_called_once()

    def test_notification_preparation_returns_none(self, mocker, dbsession, mock_redis):
        """Test that when notification_preparation returns None, task exits gracefully."""
        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()

        upload_context = {
            "commit_sha": commit.commitid,
            "branch": "test-branch",
            "pull_id": 123,
            "pipeline": "codecov",
        }

        mocker.patch(
            "tasks.test_analytics_notifier.get_redis_connection",
            return_value=mock_redis,
        )

        # Mock Redis to return non-stale token
        mock_redis.get.return_value = "1"

        task = TestAnalyticsNotifierTask()
        task.request.retries = 0
        task.max_retries = 5

        # Mock debouncer's notification_lock to succeed
        @contextmanager
        def mock_lock_succeeds(*args, **kwargs):
            yield

        mocker.patch.object(
            task.debouncer, "notification_lock", side_effect=mock_lock_succeeds
        )
        mocker.patch.object(
            task.debouncer, "check_fencing_token_stale", return_value=False
        )

        # Mock notification_preparation to return None
        mocker.patch.object(task, "notification_preparation", return_value=None)

        result = task.run_impl(
            dbsession,
            repoid=commit.repoid,
            upload_context=upload_context,
            fencing_token=1,
        )

        assert result == NotifierTaskResult(
            attempted=False,
            succeeded=False,
        )

    def test_lock_retry_limit_exceeded_during_token_acquisition(
        self, mocker, dbsession, mock_redis
    ):
        """Test that LockAcquisitionLimitError during token acquisition returns graceful failure."""
        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()

        upload_context = {
            "commit_sha": commit.commitid,
            "branch": "test-branch",
            "pull_id": 123,
            "pipeline": "codecov",
        }

        mocker.patch(
            "tasks.test_analytics_notifier.get_redis_connection",
            return_value=mock_redis,
        )

        # Mock Redis to allow token acquisition attempt
        mock_redis.pipeline.return_value.__enter__.return_value.execute.return_value = [
            "1"
        ]

        task = TestAnalyticsNotifierTask()
        task.request.retries = 0
        task.max_retries = 5

        # Mock debouncer's notification_lock to raise LockAcquisitionLimitError
        mocker.patch.object(
            task.debouncer,
            "notification_lock",
            side_effect=LockAcquisitionLimitError("Max retries exceeded"),
        )

        result = task.run_impl(
            dbsession,
            repoid=commit.repoid,
            upload_context=upload_context,
            # Don't pass fencing_token - let it go through token acquisition path
        )

        assert result == NotifierTaskResult(
            attempted=False,
            succeeded=False,
        )

    def test_lock_retry_limit_exceeded_during_notification(
        self, mocker, dbsession, mock_redis
    ):
        """Test that LockAcquisitionLimitError during notification lock returns graceful failure."""
        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()

        upload_context = {
            "commit_sha": commit.commitid,
            "branch": "test-branch",
            "pull_id": 123,
            "pipeline": "codecov",
        }

        mocker.patch(
            "tasks.test_analytics_notifier.get_redis_connection",
            return_value=mock_redis,
        )

        # Mock Redis to return non-stale token
        mock_redis.get.return_value = "1"

        task = TestAnalyticsNotifierTask()
        task.request.retries = 0
        task.max_retries = 5

        # Mock debouncer's notification_lock to raise LockAcquisitionLimitError during notification
        mocker.patch.object(
            task.debouncer,
            "notification_lock",
            side_effect=LockAcquisitionLimitError("Max retries exceeded"),
        )

        # Mock notification_preparation to return a mock notifier
        mock_notifier = mocker.MagicMock()
        mock_notifier.notify.return_value = NotifierResult.COMMENT_POSTED
        mocker.patch.object(
            task, "notification_preparation", return_value=mock_notifier
        )

        result = task.run_impl(
            dbsession,
            repoid=commit.repoid,
            upload_context=upload_context,
            fencing_token=1,
        )

        assert result == NotifierTaskResult(
            attempted=False,
            succeeded=False,
        )

    def test_task_initialization(self, mocker):
        """Test that task initialization sets up debouncer correctly."""
        mocker.patch("tasks.test_analytics_notifier.BaseCodecovTask.__init__")

        task = TestAnalyticsNotifierTask()

        assert task.debouncer is not None
        assert task.debouncer.redis_key_template == "ta_notifier_fence:{}_{}"
        assert task.debouncer.debounce_period_seconds == 30
        assert task.debouncer.lock_type.value == "notify"


class TestTransformFailures:
    """Test the transform_failures helper function."""

    def test_transform_failures_empty_list(self):
        """Test that empty list returns empty list."""
        result = transform_failures([])
        assert result == []

    def test_transform_failures_with_failure_message(self):
        """Test that failure messages are transformed correctly."""
        failures: list[FailedTestInstance] = [
            {
                "computed_name": "test_example.TestClass.test_method",
                "failure_message": "AssertionError: Expected 5, got 3\n  File /path/to/test.py:42",
                "test_id": "test123",
                "flags": ["unit"],
                "duration_seconds": 1.5,
            }
        ]

        result = transform_failures(failures)

        assert len(result) == 1
        assert result[0].display_name == "test_example.TestClass.test_method"
        assert "\r" not in result[0].failure_message
        assert result[0].test_id == "test123"
        assert result[0].envs == ["unit"]
        assert result[0].duration_seconds == 1.5
        assert result[0].build_url is None

    def test_transform_failures_with_none_failure_message(self):
        """Test that None failure_message is handled correctly."""
        failures: list[FailedTestInstance] = [
            {
                "computed_name": "test_example.TestClass.test_method",
                "failure_message": None,
                "test_id": "test123",
                "flags": ["unit"],
                "duration_seconds": None,
            }
        ]

        result = transform_failures(failures)

        assert len(result) == 1
        assert result[0].failure_message is None
        assert result[0].duration_seconds == 0  # None becomes 0

    def test_transform_failures_with_carriage_return(self):
        """Test that carriage returns are removed from failure messages."""
        failures: list[FailedTestInstance] = [
            {
                "computed_name": "test_example.TestClass.test_method",
                "failure_message": "Line 1\rLine 2\r\nLine 3",
                "test_id": "test123",
                "flags": [],
                "duration_seconds": 0.5,
            }
        ]

        result = transform_failures(failures)

        assert "\r" not in result[0].failure_message
        assert "\n" in result[0].failure_message  # \n should remain

    def test_transform_failures_multiple_failures(self):
        """Test that multiple failures are transformed correctly."""
        failures: list[FailedTestInstance] = [
            {
                "computed_name": "test1",
                "failure_message": "Error 1",
                "test_id": "id1",
                "flags": ["flag1"],
                "duration_seconds": 1.0,
            },
            {
                "computed_name": "test2",
                "failure_message": "Error 2",
                "test_id": "id2",
                "flags": ["flag2"],
                "duration_seconds": 2.0,
            },
        ]

        result = transform_failures(failures)

        assert len(result) == 2
        assert result[0].display_name == "test1"
        assert result[1].display_name == "test2"
