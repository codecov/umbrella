"""Tests for Dead Letter Queue (DLQ) recovery task."""

from unittest.mock import MagicMock

import orjson
import pytest

from app import celery_app
from shared.celery_config import DLQ_KEY_PREFIX, upload_task_name
from tasks.dlq_recovery import DLQRecoveryTask


@pytest.fixture
def dlq_task():
    """Create a DLQ recovery task instance."""
    return DLQRecoveryTask()


@pytest.fixture
def mock_redis(mocker):
    """Mock Redis connection."""
    mock_redis = MagicMock()
    mocker.patch("tasks.dlq_recovery.get_redis_connection", return_value=mock_redis)
    return mock_redis


@pytest.mark.django_db(databases={"default", "timeseries"})
class TestDLQRecoveryTask:
    def test_list_dlq_keys(self, dlq_task, mock_redis):
        """Test listing DLQ keys."""
        # Mock scan_iter to return some keys
        mock_redis.scan_iter.return_value = [
            b"task_dlq/app.tasks.upload.Upload/123/abc",
            b"task_dlq/app.tasks.upload.Upload/456/def",
        ]
        mock_redis.llen.side_effect = [5, 3]
        mock_redis.ttl.side_effect = [3600, 7200]

        result = dlq_task.run_impl(
            db_session=None,
            action="list",
        )

        assert result["success"] is True
        assert result["action"] == "list"
        assert result["total_keys"] == 2
        assert len(result["keys"]) == 2
        assert result["keys"][0]["key"] == "task_dlq/app.tasks.upload.Upload/123/abc"
        assert result["keys"][0]["count"] == 5
        assert result["keys"][0]["ttl_seconds"] == 3600

    def test_list_dlq_keys_with_filter(self, dlq_task, mock_redis):
        """Test listing DLQ keys with task name filter."""
        mock_redis.scan_iter.return_value = [
            b"task_dlq/app.tasks.upload.Upload/123/abc",
        ]
        mock_redis.llen.return_value = 5
        mock_redis.ttl.return_value = 3600

        result = dlq_task.run_impl(
            db_session=None,
            action="list",
            task_name_filter="app.tasks.upload.Upload",
        )

        assert result["success"] is True
        # Verify scan_iter was called with filtered pattern
        mock_redis.scan_iter.assert_called_with(
            match=f"{DLQ_KEY_PREFIX}/app.tasks.upload.Upload/*"
        )

    def test_recover_tasks_success(self, dlq_task, mock_redis, mocker):
        """Test recovering tasks from DLQ."""
        # Mock task data in DLQ
        task_data = {
            "task_name": "app.tasks.upload.Upload",
            "args": [],
            "kwargs": {"repoid": 123, "commitid": "abc"},
        }
        serialized_data = orjson.dumps(task_data).decode("utf-8")

        mock_redis.exists.return_value = True
        mock_redis.lpop.side_effect = [
            serialized_data,
            None,
        ]  # First call returns data, second returns None

        mock_send_task = mocker.patch.object(celery_app, "send_task")

        result = dlq_task.run_impl(
            db_session=None,
            action="recover",
            dlq_key="task_dlq/app.tasks.upload.Upload/123/abc",
        )

        assert result["success"] is True
        assert result["action"] == "recover"
        assert result["recovered_count"] == 1
        assert result["failed_count"] == 0
        mock_send_task.assert_called_once_with(
            "app.tasks.upload.Upload",
            args=[],
            kwargs={"repoid": 123, "commitid": "abc"},
        )

    def test_recover_upload_task_restores_arguments(self, dlq_task, mock_redis, mocker):
        """Test that recovering UploadTask restores upload arguments to Redis."""
        # Mock task data in DLQ with upload arguments
        upload_arguments = [
            {"upload_id": 1, "flags": ["flag1"], "url": "http://example.com/report1"},
            {"upload_id": 2, "flags": ["flag2"], "url": "http://example.com/report2"},
        ]
        task_data = {
            "task_name": upload_task_name,
            "args": [],
            "kwargs": {"repoid": 123, "commitid": "abc", "report_type": "coverage"},
            "upload_arguments": upload_arguments,
        }
        serialized_data = orjson.dumps(task_data).decode("utf-8")

        mock_redis.exists.return_value = True
        mock_redis.lpop.side_effect = [
            serialized_data,
            None,
        ]  # First call returns data, second returns None

        # Mock Redis pipeline for restoring arguments
        mock_pipeline = mocker.MagicMock()
        mock_redis.pipeline.return_value.__enter__.return_value = mock_pipeline
        mock_redis.pipeline.return_value.__exit__.return_value = None

        mock_send_task = mocker.patch.object(celery_app, "send_task")

        result = dlq_task.run_impl(
            db_session=None,
            action="recover",
            dlq_key="task_dlq/app.tasks.upload.Upload/123/abc",
        )

        assert result["success"] is True
        assert result["recovered_count"] == 1
        assert result["failed_count"] == 0

        # Verify upload arguments were restored to Redis
        assert mock_pipeline.rpush.call_count == len(upload_arguments)
        mock_pipeline.expire.assert_called_once()
        mock_pipeline.execute.assert_called_once()

        # Verify task was re-queued
        mock_send_task.assert_called_once_with(
            upload_task_name,
            args=[],
            kwargs={"repoid": 123, "commitid": "abc", "report_type": "coverage"},
        )

    def test_recover_tasks_missing_key(self, dlq_task, mock_redis):
        """Test recovering from non-existent DLQ key."""
        mock_redis.exists.return_value = False

        result = dlq_task.run_impl(
            db_session=None,
            action="recover",
            dlq_key="task_dlq/nonexistent/key",
        )

        assert result["success"] is False
        assert "not found" in result["error"].lower()

    def test_recover_tasks_invalid_data(self, dlq_task, mock_redis, mocker):
        """Test recovering tasks with invalid data."""
        mock_redis.exists.return_value = True
        mock_redis.lpop.side_effect = [b"invalid json", None]

        result = dlq_task.run_impl(
            db_session=None,
            action="recover",
            dlq_key="task_dlq/app.tasks.upload.Upload/123/abc",
        )

        assert result["success"] is True
        assert result["recovered_count"] == 0
        assert result["failed_count"] == 1
        assert len(result["errors"]) > 0

    def test_recover_tasks_missing_task_name(self, dlq_task, mock_redis, mocker):
        """Test recovering tasks with missing task_name."""
        task_data = {"args": [], "kwargs": {}}  # Missing task_name
        serialized_data = orjson.dumps(task_data).decode("utf-8")

        mock_redis.exists.return_value = True
        mock_redis.lpop.side_effect = [serialized_data, None]

        result = dlq_task.run_impl(
            db_session=None,
            action="recover",
            dlq_key="task_dlq/app.tasks.upload.Upload/123/abc",
        )

        assert result["success"] is True
        assert result["recovered_count"] == 0
        assert result["failed_count"] == 1

    def test_delete_tasks(self, dlq_task, mock_redis):
        """Test deleting tasks from DLQ."""
        mock_redis.exists.return_value = True
        mock_redis.llen.return_value = 5

        result = dlq_task.run_impl(
            db_session=None,
            action="delete",
            dlq_key="task_dlq/app.tasks.upload.Upload/123/abc",
        )

        assert result["success"] is True
        assert result["action"] == "delete"
        assert result["deleted_count"] == 5
        mock_redis.delete.assert_called_once_with(
            "task_dlq/app.tasks.upload.Upload/123/abc"
        )

    def test_delete_tasks_missing_key(self, dlq_task, mock_redis):
        """Test deleting from non-existent DLQ key."""
        mock_redis.exists.return_value = False

        result = dlq_task.run_impl(
            db_session=None,
            action="delete",
            dlq_key="task_dlq/nonexistent/key",
        )

        assert result["success"] is False
        assert "not found" in result["error"].lower()

    def test_unknown_action(self, dlq_task):
        """Test handling unknown action."""
        result = dlq_task.run_impl(
            db_session=None,
            action="unknown_action",
        )

        assert result["success"] is False
        assert "unknown action" in result["error"].lower()

    def test_recover_action_requires_dlq_key(self, dlq_task):
        """Test that recover action requires dlq_key."""
        result = dlq_task.run_impl(
            db_session=None,
            action="recover",
        )

        assert result["success"] is False
        assert "dlq_key required" in result["error"].lower()

    def test_delete_action_requires_dlq_key(self, dlq_task):
        """Test that delete action requires dlq_key."""
        result = dlq_task.run_impl(
            db_session=None,
            action="delete",
        )

        assert result["success"] is False
        assert "dlq_key required" in result["error"].lower()

    def test_recover_upload_task_with_empty_arguments(
        self, dlq_task, mock_redis, mocker
    ):
        """Test that recovering UploadTask with empty upload_arguments still works."""
        task_data = {
            "task_name": upload_task_name,
            "args": [],
            "kwargs": {"repoid": 123, "commitid": "abc", "report_type": "coverage"},
            "upload_arguments": [],  # Empty list
        }
        serialized_data = orjson.dumps(task_data).decode("utf-8")

        mock_redis.exists.return_value = True
        mock_redis.lpop.side_effect = [serialized_data, None]

        mock_pipeline = mocker.MagicMock()
        mock_redis.pipeline.return_value.__enter__.return_value = mock_pipeline
        mock_redis.pipeline.return_value.__exit__.return_value = None

        mock_send_task = mocker.patch.object(celery_app, "send_task")

        result = dlq_task.run_impl(
            db_session=None,
            action="recover",
            dlq_key="task_dlq/app.tasks.upload.Upload/123/abc",
        )

        assert result["success"] is True
        assert result["recovered_count"] == 1
        # Pipeline should not be called when upload_arguments is empty (falsy check)
        mock_redis.pipeline.assert_not_called()
        mock_send_task.assert_called_once()

    def test_recover_upload_task_restore_failure(self, dlq_task, mock_redis, mocker):
        """Test that recovery continues even if argument restoration fails."""
        upload_arguments = [{"upload_id": 1, "flags": ["flag1"]}]
        task_data = {
            "task_name": upload_task_name,
            "args": [],
            "kwargs": {"repoid": 123, "commitid": "abc", "report_type": "coverage"},
            "upload_arguments": upload_arguments,
        }
        serialized_data = orjson.dumps(task_data).decode("utf-8")

        mock_redis.exists.return_value = True
        mock_redis.lpop.side_effect = [serialized_data, None]

        # Mock pipeline to raise an error
        mock_pipeline = mocker.MagicMock()
        mock_pipeline.execute.side_effect = Exception("Redis error")
        mock_redis.pipeline.return_value.__enter__.return_value = mock_pipeline
        mock_redis.pipeline.return_value.__exit__.return_value = None

        result = dlq_task.run_impl(
            db_session=None,
            action="recover",
            dlq_key="task_dlq/app.tasks.upload.Upload/123/abc",
        )

        assert result["success"] is True
        assert result["recovered_count"] == 0  # Failed due to restore error
        assert result["failed_count"] == 1
        assert len(result["errors"]) > 0

    def test_recover_upload_task_missing_repoid_commitid(
        self, dlq_task, mock_redis, mocker
    ):
        """Test that recovery fails gracefully when repoid/commitid missing."""
        upload_arguments = [{"upload_id": 1}]
        task_data = {
            "task_name": upload_task_name,
            "args": [],
            "kwargs": {},  # Missing repoid and commitid
            "upload_arguments": upload_arguments,
        }
        serialized_data = orjson.dumps(task_data).decode("utf-8")

        mock_redis.exists.return_value = True
        mock_redis.lpop.side_effect = [serialized_data, None]

        result = dlq_task.run_impl(
            db_session=None,
            action="recover",
            dlq_key="task_dlq/app.tasks.upload.Upload/123/abc",
        )

        assert result["success"] is True
        assert result["recovered_count"] == 0
        assert result["failed_count"] == 1
        assert any(
            "repoid and commitid required" in err.lower() for err in result["errors"]
        )

    def test_recover_upload_task_test_results_report_type(
        self, dlq_task, mock_redis, mocker
    ):
        """Test restoring arguments for test_results report type."""
        upload_arguments = [{"upload_id": 1}]
        task_data = {
            "task_name": upload_task_name,
            "args": [],
            "kwargs": {"repoid": 123, "commitid": "abc", "report_type": "test_results"},
            "upload_arguments": upload_arguments,
        }
        serialized_data = orjson.dumps(task_data).decode("utf-8")

        mock_redis.exists.return_value = True
        mock_redis.lpop.side_effect = [serialized_data, None]

        mock_pipeline = mocker.MagicMock()
        mock_redis.pipeline.return_value.__enter__.return_value = mock_pipeline
        mock_redis.pipeline.return_value.__exit__.return_value = None

        mock_send_task = mocker.patch.object(celery_app, "send_task")

        result = dlq_task.run_impl(
            db_session=None,
            action="recover",
            dlq_key="task_dlq/app.tasks.upload.Upload/123/abc/test_results",
        )

        assert result["success"] is True
        assert result["recovered_count"] == 1
        # Verify correct Redis key was used (includes report_type)
        # Check that rpush was called with the correct key
        rpush_calls = mock_pipeline.rpush.call_args_list
        expected_key = "uploads/123/abc/test_results"
        # rpush is called with (key, value), so check first arg of first call
        assert rpush_calls[0][0][0] == expected_key

    def test_recover_upload_task_bundle_analysis_report_type(
        self, dlq_task, mock_redis, mocker
    ):
        """Test restoring arguments for bundle_analysis report type."""
        upload_arguments = [{"upload_id": 1}]
        task_data = {
            "task_name": upload_task_name,
            "args": [],
            "kwargs": {
                "repoid": 123,
                "commitid": "abc",
                "report_type": "bundle_analysis",
            },
            "upload_arguments": upload_arguments,
        }
        serialized_data = orjson.dumps(task_data).decode("utf-8")

        mock_redis.exists.return_value = True
        mock_redis.lpop.side_effect = [serialized_data, None]

        mock_pipeline = mocker.MagicMock()
        mock_redis.pipeline.return_value.__enter__.return_value = mock_pipeline
        mock_redis.pipeline.return_value.__exit__.return_value = None

        mock_send_task = mocker.patch.object(celery_app, "send_task")

        result = dlq_task.run_impl(
            db_session=None,
            action="recover",
            dlq_key="task_dlq/app.tasks.upload.Upload/123/abc/bundle_analysis",
        )

        assert result["success"] is True
        assert result["recovered_count"] == 1
        # Verify correct Redis key was used (includes report_type)
        rpush_calls = mock_pipeline.rpush.call_args_list
        expected_key = "uploads/123/abc/bundle_analysis"
        assert rpush_calls[0][0][0] == expected_key
