import pytest
from celery.exceptions import MaxRetriesExceededError, Retry

from database.tests.factories import CommitFactory
from helpers.notifier import NotifierResult
from services.lock_manager import LockRetry
from services.test_results import NotifierTaskResult
from tasks.test_analytics_notifier import TestAnalyticsNotifierTask


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

        # Mock get_redis_connection to return mock_redis
        mocker.patch(
            "tasks.test_analytics_notifier.get_redis_connection",
            return_value=mock_redis,
        )

        # Mock LockManager to raise LockRetry
        m = mocker.MagicMock()
        m.return_value.locked.return_value.__enter__.side_effect = LockRetry(60)
        mocker.patch("tasks.test_analytics_notifier.LockManager", m)

        # Mock Redis operations
        mock_redis.pipeline.return_value.__enter__.return_value.execute.return_value = [
            "1"
        ]

        task = TestAnalyticsNotifierTask()
        task.request.retries = 0  # Will retry
        task.request.headers = {}
        task.max_retries = 5

        # Task should call self.retry() which raises Retry exception
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

        # Mock get_redis_connection to return mock_redis
        mocker.patch(
            "tasks.test_analytics_notifier.get_redis_connection",
            return_value=mock_redis,
        )

        # Mock LockManager to raise LockRetry
        m = mocker.MagicMock()
        m.return_value.locked.return_value.__enter__.side_effect = LockRetry(60)
        mocker.patch("tasks.test_analytics_notifier.LockManager", m)

        # Mock Redis operations
        mock_redis.pipeline.return_value.__enter__.return_value.execute.return_value = [
            "1"
        ]

        task = TestAnalyticsNotifierTask()
        task.request.retries = 0
        task.request.headers = {}
        task.max_retries = 5

        # Mock self.retry() to raise MaxRetriesExceededError when called from LockRetry handler
        # We distinguish by checking if max_retries=5 is passed (from LockRetry handler)
        def retry_side_effect(*args, **kwargs):
            # Check if this is the retry call from LockRetry handler (has max_retries=5)
            if kwargs.get("max_retries") == 5:
                raise MaxRetriesExceededError()
            # Otherwise, this is the debounce retry - raise Retry exception
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
        """Test that MaxRetriesExceededError from _notification_lock returns graceful failure"""
        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()

        upload_context = {
            "commit_sha": commit.commitid,
            "branch": "test-branch",
            "pull_id": 123,
            "pipeline": "codecov",
        }

        # Mock get_redis_connection to return mock_redis
        mocker.patch(
            "tasks.test_analytics_notifier.get_redis_connection",
            return_value=mock_redis,
        )

        # Mock LockManager to raise LockRetry with max_retries_exceeded=True
        m = mocker.MagicMock()
        lock_retry_with_max_exceeded = LockRetry(
            countdown=0,
            max_retries_exceeded=True,
            retry_num=5,
            max_retries=5,
            lock_name="notification_lock",
            repoid=commit.repoid,
            commitid=commit.commitid,
        )
        m.return_value.locked.return_value.__enter__.side_effect = (
            lock_retry_with_max_exceeded
        )
        mocker.patch("tasks.test_analytics_notifier.LockManager", m)

        # Mock Redis operations
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

        # Mock get_redis_connection to return mock_redis
        mocker.patch(
            "tasks.test_analytics_notifier.get_redis_connection",
            return_value=mock_redis,
        )

        # Mock LockManager to raise LockRetry during fencing token check lock acquisition
        lock_manager_mock = mocker.MagicMock()
        lock_manager_mock.return_value.locked.return_value.__enter__.side_effect = (
            LockRetry(60)
        )
        mocker.patch("tasks.test_analytics_notifier.LockManager", lock_manager_mock)

        # Mock Redis operations
        mock_redis.pipeline.return_value.__enter__.return_value.execute.return_value = [
            "1"
        ]
        mock_redis.get.return_value = "1"  # Fencing token check

        task = TestAnalyticsNotifierTask()
        task.request.retries = 0
        task.request.headers = {}
        task.max_retries = 5

        # Mock self.retry() to raise MaxRetriesExceededError when called from LockRetry handler
        # (identified by max_retries=5 parameter)
        def retry_side_effect(*args, **kwargs):
            # Check if this is the retry call from LockRetry handler (has max_retries=5)
            if kwargs.get("max_retries") == 5:
                raise MaxRetriesExceededError()
            # Otherwise, this is a normal retry - raise Retry exception
            raise Retry()

        mocker.patch.object(task, "retry", side_effect=retry_side_effect)

        result = task.run_impl(
            dbsession,
            repoid=commit.repoid,
            upload_context=upload_context,
            fencing_token=1,  # Pass fencing token to skip first lock acquisition
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

        # Mock get_redis_connection to return mock_redis
        mocker.patch(
            "tasks.test_analytics_notifier.get_redis_connection",
            return_value=mock_redis,
        )

        # First lock acquisition succeeds (fencing token check), second raises LockRetry (notification send)
        lock_manager_mock = mocker.MagicMock()
        context_manager = mocker.MagicMock()

        def lock_side_effect(*args, **kwargs):
            if not hasattr(lock_side_effect, "call_count"):
                lock_side_effect.call_count = 0
            lock_side_effect.call_count += 1
            if lock_side_effect.call_count == 1:
                # First call succeeds (fencing token check)
                return context_manager
            else:
                # Second call raises LockRetry (notification send)
                raise LockRetry(60)

        lock_manager_mock.return_value.locked.return_value.__enter__.side_effect = (
            lock_side_effect
        )
        mocker.patch("tasks.test_analytics_notifier.LockManager", lock_manager_mock)

        # Mock Redis operations
        mock_redis.pipeline.return_value.__enter__.return_value.execute.return_value = [
            "1"
        ]
        mock_redis.get.return_value = "1"  # Fencing token check

        task = TestAnalyticsNotifierTask()
        task.request.retries = 0
        task.request.headers = {}
        task.max_retries = 5

        # Mock self.retry() to raise MaxRetriesExceededError when called from LockRetry handler
        # (identified by max_retries=5 parameter)
        def retry_side_effect(*args, **kwargs):
            # Check if this is the retry call from LockRetry handler (has max_retries=5)
            if kwargs.get("max_retries") == 5:
                raise MaxRetriesExceededError()
            # Otherwise, this is a normal retry - raise Retry exception
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
            fencing_token=1,  # Pass fencing token to skip first lock acquisition
        )

        assert result == NotifierTaskResult(
            attempted=False,
            succeeded=False,
        )
