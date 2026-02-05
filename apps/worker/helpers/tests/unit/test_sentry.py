import os

from helpers.sentry import before_send_transaction, initialize_sentry


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
            before_send_transaction=before_send_transaction,
        )
        mocked_set_tag.assert_called_with("cluster", cluster)

    def test_before_send_transaction_filters_upload_breadcrumb(self, mocker):
        """Test that UploadBreadcrumb transactions are filtered out"""
        # Create transaction dict with UploadBreadcrumb name
        transaction = {"transaction": "UploadBreadcrumb"}

        result = before_send_transaction(transaction, None)

        assert result is None  # Transaction should be dropped

    def test_before_send_transaction_filters_upload_breadcrumb_full_path(self, mocker):
        """Test that fully qualified UploadBreadcrumb task names are filtered"""
        transaction = {"transaction": "app.tasks.upload.UploadBreadcrumb"}

        result = before_send_transaction(transaction, None)

        assert result is None  # Transaction should be dropped

    def test_before_send_transaction_allows_other_tasks(self, mocker):
        """Test that non-filtered transactions are allowed through"""
        transaction = {"transaction": "app.tasks.upload.Upload"}

        result = before_send_transaction(transaction, None)

        assert result is transaction  # Transaction should pass through

    def test_before_send_transaction_handles_missing_transaction_key(self, mocker):
        """Regression test: ensure missing 'transaction' key doesn't crash

        This tests the fix for the Nov 10, 2025 bug where we used transaction.name
        instead of transaction.get("transaction"), which would crash on any transaction.
        """
        # Transaction dict without 'transaction' key (edge case)
        transaction = {"type": "transaction", "contexts": {}}

        # Should not crash and should return the transaction unmodified
        result = before_send_transaction(transaction, None)

        assert result is transaction  # Should pass through without crashing

    def test_before_send_transaction_handles_none_transaction_value(self, mocker):
        """Regression test: ensure None transaction name doesn't crash"""
        transaction = {"transaction": None}

        # Should not crash
        result = before_send_transaction(transaction, None)

        assert result is transaction  # Should pass through

    def test_before_send_transaction_returns_same_object(self, mocker):
        """Regression test: verify we return the exact same object, not a copy"""
        transaction = {"transaction": "app.tasks.notify.Notify", "spans": []}

        result = before_send_transaction(transaction, None)

        # Should be the exact same object (identity check, not just equality)
        assert result is transaction
        assert id(result) == id(transaction)
