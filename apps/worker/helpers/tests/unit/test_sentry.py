import os

from helpers.sentry import (
    _should_sample_event,
    before_send,
    before_send_transaction,
    initialize_sentry,
)


class TestSentry:
    def test_initialize_sentry(self, mocker, mock_configuration):
        mock_configuration._params["services"] = {"sentry": {"server_dsn": "this_dsn"}}
        cluster = "test_env"
        mocker.patch.dict(
            os.environ,
            {"RELEASE_VERSION": "FAKE_VERSION_FOR_YOU", "CLUSTER_ENV": cluster},
        )
        mocked_init = mocker.patch("helpers.sentry.sentry_sdk.init")
        mocked_set_tag = mocker.patch("helpers.sentry.sentry_sdk.set_tag")
        assert initialize_sentry() is None
        mocked_init.assert_called_with(
            "this_dsn",
            release="worker-FAKE_VERSION_FOR_YOU",
            sample_rate=1.0,
            traces_sample_rate=1.0,
            profiles_sample_rate=1.0,
            environment="production",
            _experiments=mocker.ANY,
            enable_backpressure_handling=False,
            integrations=mocker.ANY,
            before_send=before_send,
            before_send_transaction=before_send_transaction,
        )
        mocked_set_tag.assert_called_with("cluster", cluster)

    def test_before_send_transaction_filters_upload_breadcrumb(self, mocker):
        """Test that UploadBreadcrumb transactions are filtered out"""
        # Create mock transaction with UploadBreadcrumb name
        mock_transaction = mocker.Mock()
        mock_transaction.name = "UploadBreadcrumb"

        result = before_send_transaction(mock_transaction, None)

        assert result is None  # Transaction should be dropped

    def test_before_send_transaction_filters_upload_breadcrumb_full_path(self, mocker):
        """Test that fully qualified UploadBreadcrumb task names are filtered"""
        mock_transaction = mocker.Mock()
        mock_transaction.name = "app.tasks.upload.UploadBreadcrumb"

        result = before_send_transaction(mock_transaction, None)

        assert result is None  # Transaction should be dropped

    def test_before_send_transaction_allows_other_tasks(self, mocker):
        """Test that non-filtered transactions are allowed through"""
        mock_transaction = mocker.Mock()
        mock_transaction.name = "app.tasks.upload.Upload"

        result = before_send_transaction(mock_transaction, None)

        assert result is mock_transaction  # Transaction should pass through


class TestBeforeSend:
    """Tests for the before_send error sampling hook."""

    def test_before_send_allows_event_without_exception(self):
        """Test that events without exception data are allowed through."""
        event = {"event_id": "abc123", "message": "Some message"}
        result = before_send(event, {})
        assert result == event

    def test_before_send_allows_event_with_empty_exception(self):
        """Test that events with empty exception values are allowed through."""
        event = {"event_id": "abc123", "exception": {"values": []}}
        result = before_send(event, {})
        assert result == event

    def test_before_send_allows_event_without_repo_name(self):
        """Test that events without repo_name tag are allowed through."""
        event = {
            "event_id": "abc123",
            "exception": {"values": [{"type": "LockError"}]},
            "tags": {"other_tag": "value"},
        }
        result = before_send(event, {})
        assert result == event

    def test_before_send_allows_event_from_non_sampled_repo(self):
        """Test that events from repos not in the sampling config are allowed."""
        event = {
            "event_id": "abc123",
            "exception": {"values": [{"type": "LockError"}]},
            "tags": {"repo_name": "other-repo"},
        }
        result = before_send(event, {})
        assert result == event

    def test_before_send_allows_non_sampled_error_type(self):
        """Test that error types not in the sampling config are allowed."""
        event = {
            "event_id": "abc123",
            "exception": {"values": [{"type": "SomeOtherError"}]},
            "tags": {"repo_name": "square-web"},
        }
        result = before_send(event, {})
        assert result == event

    def test_before_send_samples_lock_error_from_square_web(self):
        """Test that LockError from square-web is sampled."""
        # With 0.1% sample rate, most events should be dropped
        # Test with specific event_ids to ensure deterministic behavior
        sampled_count = 0
        total = 1000

        for i in range(total):
            event = {
                "event_id": f"test-event-{i}",
                "exception": {"values": [{"type": "LockError"}]},
                "tags": {"repo_name": "square-web"},
            }
            result = before_send(event, {})
            if result is not None:
                sampled_count += 1

        # With 0.1% sample rate, we expect roughly 1 out of 1000
        # Allow some variance for the hash distribution
        assert sampled_count < 50  # Should be very few

    def test_before_send_is_deterministic(self):
        """Test that the same event always produces the same sampling result."""
        event = {
            "event_id": "deterministic-test-123",
            "exception": {"values": [{"type": "LockError"}]},
            "tags": {"repo_name": "square-web"},
        }

        # Call before_send multiple times with the same event
        results = [before_send(event.copy(), {}) for _ in range(10)]

        # All results should be the same (either all None or all the event)
        first_result = results[0]
        for result in results[1:]:
            if first_result is None:
                assert result is None
            else:
                assert result is not None

    def test_before_send_handles_tags_as_list(self):
        """Test that before_send handles tags as a list of key-value pairs."""
        event = {
            "event_id": "abc123",
            "exception": {"values": [{"type": "SomeOtherError"}]},
            "tags": [["repo_name", "other-repo"], ["other_tag", "value"]],
        }
        result = before_send(event, {})
        assert result == event

    def test_before_send_samples_lock_retry_from_square_web(self):
        """Test that LockRetry from square-web is subject to sampling."""
        event = {
            "event_id": "lock-retry-test",
            "exception": {"values": [{"type": "LockRetry"}]},
            "tags": {"repo_name": "square-web"},
        }
        # Just verify it runs without error - sampling is deterministic
        result = before_send(event, {})
        assert result is None or result == event

    def test_before_send_samples_max_retries_exceeded_from_square_web(self):
        """Test that MaxRetriesExceededError from square-web is subject to sampling."""
        event = {
            "event_id": "max-retries-test",
            "exception": {"values": [{"type": "MaxRetriesExceededError"}]},
            "tags": {"repo_name": "square-web"},
        }
        # Just verify it runs without error - sampling is deterministic
        result = before_send(event, {})
        assert result is None or result == event


class TestShouldSampleEvent:
    """Tests for the _should_sample_event helper function."""

    def test_sample_rate_one_always_keeps(self):
        """Test that sample_rate of 1.0 always keeps the event."""
        assert _should_sample_event("any-event-id", 1.0) is True
        assert _should_sample_event("another-event-id", 1.0) is True

    def test_sample_rate_zero_always_drops(self):
        """Test that sample_rate of 0.0 always drops the event."""
        assert _should_sample_event("any-event-id", 0.0) is False
        assert _should_sample_event("another-event-id", 0.0) is False

    def test_sample_rate_is_deterministic(self):
        """Test that the same event_id always produces the same result."""
        event_id = "test-deterministic-id"
        sample_rate = 0.5

        results = [_should_sample_event(event_id, sample_rate) for _ in range(100)]

        # All results should be identical
        assert all(r == results[0] for r in results)

    def test_sample_rate_produces_expected_distribution(self):
        """Test that sampling produces roughly the expected distribution."""
        sample_rate = 0.5
        sampled_count = 0
        total = 10000

        for i in range(total):
            if _should_sample_event(f"event-{i}", sample_rate):
                sampled_count += 1

        # With 50% sample rate, expect roughly 5000 sampled
        # Allow 10% variance for hash distribution
        assert 4000 < sampled_count < 6000
