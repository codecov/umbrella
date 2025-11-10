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
