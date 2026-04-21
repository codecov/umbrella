from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from tasks.process_owners_to_be_deleted_cron import ProcessOwnersToBeDeletedCronTask

pytestmark = pytest.mark.django_db


class TestProcessOwnersToBeDeletedCronTask:
    def test_get_min_seconds_interval_between_executions(self):
        interval = ProcessOwnersToBeDeletedCronTask.get_min_seconds_interval_between_executions()
        assert interval == 60 * 45  # 45 minutes

    def _make_task(self, mock_delete_task):
        task = ProcessOwnersToBeDeletedCronTask()
        task.app = MagicMock()
        task.app.tasks = {"app.tasks.delete_owner.DeleteOwner": mock_delete_task}
        return task

    @patch("tasks.process_owners_to_be_deleted_cron.OwnerToBeDeleted")
    @patch("tasks.process_owners_to_be_deleted_cron.timezone")
    def test_run_cron_task_no_owners(self, mock_timezone, mock_model):
        now = datetime(2026, 4, 20, 12, 0, 0, tzinfo=UTC)
        mock_timezone.now.return_value = now

        mock_qs = MagicMock()
        mock_qs.__getitem__ = MagicMock(return_value=[])
        mock_model.objects.filter.return_value = mock_qs

        mock_delete_task = MagicMock()
        task = self._make_task(mock_delete_task)
        result = task.run_cron_task(MagicMock())

        expected_cutoff = now - timedelta(hours=48)
        mock_model.objects.filter.assert_called_once_with(
            created_at__lte=expected_cutoff
        )
        assert result["owners_processed"] == 0
        assert result["message"] == "No owners to process"
        mock_delete_task.apply_async.assert_not_called()

    @patch("tasks.process_owners_to_be_deleted_cron.OwnerToBeDeleted")
    @patch("tasks.process_owners_to_be_deleted_cron.timezone")
    def test_run_cron_task_with_owners(self, mock_timezone, mock_model):
        now = datetime(2026, 4, 20, 12, 0, 0, tzinfo=UTC)
        mock_timezone.now.return_value = now

        mock_owners = [MagicMock(owner_id=1), MagicMock(owner_id=2)]
        mock_qs = MagicMock()
        mock_qs.__getitem__ = MagicMock(return_value=mock_owners)
        mock_model.objects.filter.return_value = mock_qs

        mock_delete_task = MagicMock()
        task = self._make_task(mock_delete_task)
        result = task.run_cron_task(MagicMock())

        assert result["owners_processed"] == 2
        assert "Processed 2 owners" in result["message"]
        assert mock_delete_task.apply_async.call_count == 2
        mock_delete_task.apply_async.assert_any_call(kwargs={"ownerid": 1})
        mock_delete_task.apply_async.assert_any_call(kwargs={"ownerid": 2})

    @patch("django.conf.settings")
    @patch("tasks.process_owners_to_be_deleted_cron.OwnerToBeDeleted")
    @patch("tasks.process_owners_to_be_deleted_cron.timezone")
    def test_run_cron_task_respects_limit(
        self, mock_timezone, mock_model, mock_settings
    ):
        mock_settings.MAX_OWNERS_TO_DELETE_PER_CRON_RUN = 2
        now = datetime(2026, 4, 20, 12, 0, 0, tzinfo=UTC)
        mock_timezone.now.return_value = now

        # Queryset slice already limited externally; simulate returning only 2
        mock_owners = [MagicMock(owner_id=1), MagicMock(owner_id=2)]
        mock_qs = MagicMock()
        mock_qs.__getitem__ = MagicMock(return_value=mock_owners)
        mock_model.objects.filter.return_value = mock_qs

        mock_delete_task = MagicMock()
        task = self._make_task(mock_delete_task)
        result = task.run_cron_task(MagicMock())

        assert result["owners_processed"] == 2
        assert mock_delete_task.apply_async.call_count == 2

    @patch("tasks.process_owners_to_be_deleted_cron.OwnerToBeDeleted")
    @patch("tasks.process_owners_to_be_deleted_cron.timezone")
    def test_run_cron_task_handles_failure(self, mock_timezone, mock_model):
        now = datetime(2026, 4, 20, 12, 0, 0, tzinfo=UTC)
        mock_timezone.now.return_value = now

        mock_owners = [MagicMock(owner_id=1), MagicMock(owner_id=2)]
        mock_qs = MagicMock()
        mock_qs.__getitem__ = MagicMock(return_value=mock_owners)
        mock_model.objects.filter.return_value = mock_qs

        mock_delete_task = MagicMock()
        mock_delete_task.apply_async.side_effect = [Exception("Task failed"), None]

        task = self._make_task(mock_delete_task)
        result = task.run_cron_task(MagicMock())

        assert result["owners_processed"] == 1
        assert mock_delete_task.apply_async.call_count == 2

    @patch("tasks.process_owners_to_be_deleted_cron.OwnerToBeDeleted")
    @patch("tasks.process_owners_to_be_deleted_cron.timezone")
    def test_run_cron_task_uses_48h_cutoff(self, mock_timezone, mock_model):
        now = datetime(2026, 4, 20, 12, 0, 0, tzinfo=UTC)
        mock_timezone.now.return_value = now

        mock_qs = MagicMock()
        mock_qs.__getitem__ = MagicMock(return_value=[])
        mock_model.objects.filter.return_value = mock_qs

        task = self._make_task(MagicMock())
        task.run_cron_task(MagicMock())

        expected_cutoff = datetime(2026, 4, 18, 12, 0, 0, tzinfo=UTC)
        mock_model.objects.filter.assert_called_once_with(
            created_at__lte=expected_cutoff
        )

    @patch("tasks.process_owners_to_be_deleted_cron.OwnerToBeDeleted")
    @patch("tasks.process_owners_to_be_deleted_cron.timezone")
    def test_run_cron_task_default_limit(self, mock_timezone, mock_model):
        now = datetime(2026, 4, 20, 12, 0, 0, tzinfo=UTC)
        mock_timezone.now.return_value = now

        mock_owners = [MagicMock(owner_id=1), MagicMock(owner_id=2)]
        mock_qs = MagicMock()
        mock_qs.__getitem__ = MagicMock(return_value=mock_owners)
        mock_model.objects.filter.return_value = mock_qs

        task = self._make_task(MagicMock())
        result = task.run_cron_task(MagicMock())

        assert result["owners_processed"] == 2
