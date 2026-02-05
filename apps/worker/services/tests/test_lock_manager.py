import logging
import threading
import time
from unittest.mock import MagicMock

import pytest
from redis.exceptions import LockError

from database.enums import ReportType
from services.lock_manager import LockManager, LockRetry, LockType
from tasks.base import BaseCodecovTask


@pytest.fixture
def mock_redis(mocker):
    """Mock Redis connection for LockManager tests"""
    mock_redis_conn = MagicMock()
    mocker.patch(
        "services.lock_manager.get_redis_connection", return_value=mock_redis_conn
    )
    return mock_redis_conn


class TestLockManager:
    def test_init_defaults(self, mock_redis):
        """Test LockManager initialization with default values"""
        manager = LockManager(repoid=123, commitid="abc123")
        assert manager.repoid == 123
        assert manager.commitid == "abc123"
        assert manager.report_type == ReportType.COVERAGE
        assert manager.lock_timeout == 300  # DEFAULT_LOCK_TIMEOUT_SECONDS
        assert manager.blocking_timeout == 5  # DEFAULT_BLOCKING_TIMEOUT_SECONDS
        assert manager.redis_connection is not None

    def test_init_custom_values(self, mock_redis):
        """Test LockManager initialization with custom values"""
        custom_redis = MagicMock()
        manager = LockManager(
            repoid=456,
            commitid="def456",
            report_type=ReportType.BUNDLE_ANALYSIS,
            lock_timeout=600,
            blocking_timeout=10,
            redis_connection=custom_redis,
        )
        assert manager.repoid == 456
        assert manager.commitid == "def456"
        assert manager.report_type == ReportType.BUNDLE_ANALYSIS
        assert manager.lock_timeout == 600
        assert manager.blocking_timeout == 10
        assert manager.redis_connection == custom_redis

    def test_init_blocking_timeout_none(self, mock_redis):
        """Test LockManager initialization with blocking_timeout=None"""
        manager = LockManager(repoid=789, commitid="ghi789", blocking_timeout=None)
        assert manager.blocking_timeout is None

    def test_lock_name_coverage(self):
        """Test lock name generation for COVERAGE report type (backward compat)"""
        manager = LockManager(repoid=123, commitid="abc123")
        name = manager.lock_name(LockType.UPLOAD)
        assert name == "upload_lock_123_abc123"
        assert "coverage" not in name

    def test_lock_name_bundle_analysis(self):
        """Test lock name generation for BUNDLE_ANALYSIS report type"""
        manager = LockManager(
            repoid=123, commitid="abc123", report_type=ReportType.BUNDLE_ANALYSIS
        )
        name = manager.lock_name(LockType.BUNDLE_ANALYSIS_PROCESSING)
        assert name == "bundle_analysis_processing_lock_123_abc123_bundle_analysis"

    def test_lock_name_test_results(self):
        """Test lock name generation for TEST_RESULTS report type"""
        manager = LockManager(
            repoid=123, commitid="abc123", report_type=ReportType.TEST_RESULTS
        )
        name = manager.lock_name(LockType.UPLOAD)
        assert name == "upload_lock_123_abc123_test_results"

    def test_locked_success(self, mock_redis):
        """Test successful lock acquisition"""
        mock_lock = MagicMock()
        mock_lock.__enter__ = MagicMock(return_value=None)
        mock_lock.__exit__ = MagicMock(return_value=None)
        mock_redis.lock.return_value = mock_lock

        manager = LockManager(repoid=123, commitid="abc123")
        with manager.locked(LockType.UPLOAD):
            pass

        mock_redis.lock.assert_called_once_with(
            "upload_lock_123_abc123", timeout=300, blocking_timeout=5
        )

    def test_locked_blocking_timeout_none(self, mock_redis):
        """Test lock acquisition with blocking_timeout=None"""
        mock_lock = MagicMock()
        mock_lock.__enter__ = MagicMock(return_value=None)
        mock_lock.__exit__ = MagicMock(return_value=None)
        mock_redis.lock.return_value = mock_lock

        manager = LockManager(repoid=123, commitid="abc123", blocking_timeout=None)
        with manager.locked(LockType.UPLOAD):
            pass

        mock_redis.lock.assert_called_once_with(
            "upload_lock_123_abc123", timeout=300, blocking_timeout=None
        )

    def test_locked_logs_acquisition(self, mock_redis, caplog):
        """Test that lock acquisition is logged"""
        mock_lock = MagicMock()
        mock_lock.__enter__ = MagicMock(return_value=None)
        mock_lock.__exit__ = MagicMock(return_value=None)
        mock_redis.lock.return_value = mock_lock

        manager = LockManager(repoid=123, commitid="abc123")
        with caplog.at_level(logging.INFO):
            with manager.locked(LockType.UPLOAD):
                pass

        log_messages = [record.message for record in caplog.records]
        assert any("Acquiring lock" in msg for msg in log_messages)
        assert any("Acquired lock" in msg for msg in log_messages)
        assert any("Releasing lock" in msg for msg in log_messages)

    def test_locked_logs_duration(self, mock_redis, caplog):
        """Test that lock duration is logged"""
        mock_lock = MagicMock()
        mock_lock.__enter__ = MagicMock(return_value=None)
        mock_lock.__exit__ = MagicMock(return_value=None)
        mock_redis.lock.return_value = mock_lock

        manager = LockManager(repoid=123, commitid="abc123")
        with caplog.at_level(logging.INFO):
            with manager.locked(LockType.UPLOAD):
                time.sleep(0.01)  # Small delay to ensure duration > 0

        log_records = [r for r in caplog.records if "Releasing lock" in r.message]
        assert len(log_records) == 1
        assert "lock_duration_seconds" in log_records[0].__dict__

    def test_locked_lock_error_raises_lock_retry(self, mock_redis):
        """Test that LockError raises LockRetry exception"""
        mock_redis.lock.side_effect = LockError()
        mock_redis.incr.return_value = 1

        manager = LockManager(repoid=123, commitid="abc123")
        with pytest.raises(LockRetry) as exc_info:
            with manager.locked(LockType.UPLOAD):
                pass

        assert exc_info.value.countdown > 0
        assert isinstance(exc_info.value.countdown, int)
        assert exc_info.value.max_retries_exceeded is False

    def test_locked_exponential_backoff_retry_0(self, mock_redis):
        """Test exponential backoff calculation for retry_num=0"""
        mock_redis.lock.side_effect = LockError()
        mock_redis.incr.return_value = 1

        manager = LockManager(repoid=123, commitid="abc123")
        with pytest.raises(LockRetry) as exc_info:
            with manager.locked(LockType.UPLOAD, retry_num=0):
                pass

        # retry_num=0: 200 * 3^0 = 200, so countdown should be between 100-200
        assert 100 <= exc_info.value.countdown <= 200

    def test_locked_exponential_backoff_retry_1(self, mock_redis):
        """Test exponential backoff calculation for retry_num=1"""
        mock_redis.lock.side_effect = LockError()
        mock_redis.incr.return_value = 1

        manager = LockManager(repoid=123, commitid="abc123")
        with pytest.raises(LockRetry) as exc_info:
            with manager.locked(LockType.UPLOAD, retry_num=1):
                pass

        # retry_num=1: 200 * 3^1 = 600, so countdown should be between 300-600
        assert 300 <= exc_info.value.countdown <= 600

    def test_locked_exponential_backoff_retry_2(self, mock_redis):
        """Test exponential backoff calculation for retry_num=2"""
        mock_redis.lock.side_effect = LockError()
        mock_redis.incr.return_value = 1

        manager = LockManager(repoid=123, commitid="abc123")
        with pytest.raises(LockRetry) as exc_info:
            with manager.locked(LockType.UPLOAD, retry_num=2):
                pass

        # retry_num=2: 200 * 3^2 = 1800, so countdown should be between 900-1800
        assert 900 <= exc_info.value.countdown <= 1800

    def test_locked_exponential_backoff_cap(self, mock_redis):
        """Test that exponential backoff is capped at 5 hours"""
        mock_redis.lock.side_effect = LockError()
        mock_redis.incr.return_value = 1

        manager = LockManager(repoid=123, commitid="abc123")
        # Use a high retry_num that would exceed the cap
        with pytest.raises(LockRetry) as exc_info:
            with manager.locked(LockType.UPLOAD, retry_num=10):
                pass

        # Cap is 60 * 60 * 5 = 18000 seconds (5 hours)
        assert exc_info.value.countdown <= 18000

    def test_locked_max_retries_not_provided(self, mock_redis, caplog):
        """Test that max_retries=None doesn't log error"""
        mock_redis.lock.side_effect = LockError()
        mock_redis.incr.return_value = 1

        manager = LockManager(repoid=123, commitid="abc123")
        with caplog.at_level(logging.ERROR):
            with pytest.raises(LockRetry):
                with manager.locked(LockType.UPLOAD, retry_num=5):
                    pass

        error_logs = [r for r in caplog.records if r.levelname == "ERROR"]
        assert len(error_logs) == 0

    def test_locked_max_retries_not_exceeded(self, mock_redis, caplog):
        """Test that max_retries check doesn't log error when not exceeded"""
        mock_redis.lock.side_effect = LockError()
        mock_redis.incr.return_value = 1

        manager = LockManager(repoid=123, commitid="abc123")
        with caplog.at_level(logging.ERROR):
            with pytest.raises(LockRetry):
                with manager.locked(LockType.UPLOAD, retry_num=3, max_retries=5):
                    pass

        error_logs = [
            r
            for r in caplog.records
            if r.levelname == "ERROR" and "too many retries" in r.message
        ]
        assert len(error_logs) == 0

    def test_locked_max_retries_exceeded(self, mock_redis, caplog):
        """Test that max attempts exceeded raises LockRetry with max_retries_exceeded=True"""
        mock_redis.lock.side_effect = LockError()
        mock_redis.incr.return_value = (
            3  # Redis attempt count >= max_retries (3 = 3 attempts)
        )

        manager = LockManager(repoid=123, commitid="abc123")
        with caplog.at_level(logging.ERROR):
            with pytest.raises(LockRetry) as exc_info:
                with manager.locked(LockType.UPLOAD, retry_num=5, max_retries=3):
                    pass

        assert isinstance(exc_info.value, LockRetry)
        assert exc_info.value.max_retries_exceeded is True
        assert exc_info.value.retry_num == 3  # from Redis incr
        assert exc_info.value.max_retries == 3
        assert exc_info.value.lock_name == "upload_lock_123_abc123"
        assert exc_info.value.repoid == 123
        assert exc_info.value.commitid == "abc123"
        assert exc_info.value.countdown == 0

        error_logs = [
            r
            for r in caplog.records
            if r.levelname == "ERROR" and "too many attempts" in r.message
        ]
        assert len(error_logs) == 1
        assert error_logs[0].__dict__["max_retries"] == 3
        assert error_logs[0].__dict__["attempts"] == 3

    def test_locked_max_retries_exceeded_at_boundary(self, mock_redis, caplog):
        """Test that max attempts boundary (attempts >= max_retries) raises LockRetry"""
        mock_redis.lock.side_effect = LockError()
        mock_redis.incr.return_value = 3  # attempts >= max_retries (3)

        manager = LockManager(repoid=123, commitid="abc123")
        with caplog.at_level(logging.ERROR):
            with pytest.raises(LockRetry) as exc_info:
                with manager.locked(LockType.UPLOAD, retry_num=4, max_retries=3):
                    pass

        assert isinstance(exc_info.value, LockRetry)
        assert exc_info.value.max_retries_exceeded is True
        assert exc_info.value.retry_num == 3
        assert exc_info.value.max_retries == 3
        assert exc_info.value.countdown == 0

        error_logs = [
            r
            for r in caplog.records
            if r.levelname == "ERROR" and "too many attempts" in r.message
        ]
        assert len(error_logs) == 1

    def test_locked_warning_logged_on_lock_error(self, mock_redis, caplog):
        """Test that warning is logged when lock cannot be acquired"""
        mock_redis.lock.side_effect = LockError()
        mock_redis.incr.return_value = 1

        manager = LockManager(repoid=123, commitid="abc123")
        with caplog.at_level(logging.WARNING):
            with pytest.raises(LockRetry):
                with manager.locked(LockType.UPLOAD, retry_num=2):
                    pass

        warning_logs = [
            r
            for r in caplog.records
            if r.levelname == "WARNING" and "Unable to acquire lock" in r.message
        ]
        assert len(warning_logs) == 1
        assert warning_logs[0].__dict__["retry_num"] == 2
        assert "countdown" in warning_logs[0].__dict__

    def test_locked_blocking_timeout_none_causes_indefinite_blocking(self, mock_redis):
        """
        Test that blocking_timeout=None causes indefinite blocking, preventing retry logic.

        This test detects the bug where blocking_timeout=None causes Redis to block
        indefinitely, preventing LockError from being raised and disabling retry logic.
        """
        # Track blocking_timeout and simulate behavior
        blocking_timeouts = []
        lock_acquired = threading.Event()

        def simulate_redis_lock(*args, **kwargs):
            blocking_timeouts.append(kwargs.get("blocking_timeout"))
            if kwargs.get("blocking_timeout") is not None:
                raise LockError()
            # With blocking_timeout=None, Redis blocks indefinitely
            time.sleep(1.0)
            lock_acquired.set()
            mock_lock = MagicMock()
            mock_lock.__enter__ = MagicMock()
            mock_lock.__exit__ = MagicMock(return_value=None)
            return mock_lock

        mock_redis.lock.side_effect = simulate_redis_lock

        manager = LockManager(repoid=123, commitid="abc123", blocking_timeout=None)

        # Run lock acquisition in a thread to detect blocking
        thread = threading.Thread(
            target=lambda: manager.locked(LockType.UPLOAD).__enter__(),
            daemon=True,
        )
        thread.start()
        thread.join(timeout=0.3)

        # Verify blocking_timeout=None was passed and thread is still blocking
        assert None in blocking_timeouts
        assert thread.is_alive(), (
            "Expected thread to hang with blocking_timeout=None. "
            "This documents that blocking_timeout=None prevents LockError from being raised."
        )

    def test_locked_blocking_timeout_enables_retry_logic(self, mock_redis):
        """
        Test that blocking_timeout with a value enables retry logic by raising LockError.

        This is the correct behavior - when blocking_timeout is set, Redis raises
        LockError after the timeout, which enables the retry logic.
        """
        # When blocking_timeout is set, Redis raises LockError after timeout
        mock_redis.lock.side_effect = LockError()
        mock_redis.incr.return_value = 1

        manager = LockManager(repoid=123, commitid="abc123", blocking_timeout=5)

        # This should raise LockRetry immediately (not hang)
        with pytest.raises(LockRetry) as exc_info:
            with manager.locked(LockType.UPLOAD, retry_num=0):
                pass

        # Verify LockRetry was raised with countdown
        assert exc_info.value.countdown > 0
        assert isinstance(exc_info.value.countdown, int)

        # Verify blocking_timeout=5 was passed to Redis
        mock_redis.lock.assert_called_once_with(
            "upload_lock_123_abc123", timeout=300, blocking_timeout=5
        )


class TestLockRetry:
    def test_lock_retry_init(self):
        """Test LockRetry exception initialization"""
        retry = LockRetry(countdown=60)
        assert retry.countdown == 60

    def test_lock_retry_is_exception(self):
        """Test that LockRetry is an Exception"""
        retry = LockRetry(countdown=60)
        assert isinstance(retry, Exception)


class TestGetLockTimeout:
    """Test get_lock_timeout method in BaseCodecovTask"""

    def test_get_lock_timeout_default_larger(self, mocker):
        """Test when default_timeout is larger than hard_time_limit_task"""
        task = BaseCodecovTask()
        # Patch at class level since hard_time_limit_task is a property
        mocker.patch.object(
            BaseCodecovTask,
            "hard_time_limit_task",
            property(lambda self: 100),
        )
        result = task.get_lock_timeout(300)
        assert result == 300

    def test_get_lock_timeout_hard_limit_larger(self, mocker):
        """Test when hard_time_limit_task is larger than default_timeout"""
        task = BaseCodecovTask()
        # Patch at class level since hard_time_limit_task is a property
        mocker.patch.object(
            BaseCodecovTask,
            "hard_time_limit_task",
            property(lambda self: 720),
        )
        result = task.get_lock_timeout(300)
        assert result == 720

    def test_get_lock_timeout_equal(self, mocker):
        """Test when default_timeout equals hard_time_limit_task"""
        task = BaseCodecovTask()
        # Patch at class level since hard_time_limit_task is a property
        mocker.patch.object(
            BaseCodecovTask,
            "hard_time_limit_task",
            property(lambda self: 300),
        )
        result = task.get_lock_timeout(300)
        assert result == 300
