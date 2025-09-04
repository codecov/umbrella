from unittest.mock import MagicMock, patch

import pytest

from tasks.process_owners_to_be_deleted_cron import ProcessOwnersToBeDeletedCronTask

pytestmark = pytest.mark.django_db


class TestProcessOwnersToBeDeletedCronTask:
    def test_get_min_seconds_interval_between_executions(self):
        """Test that the task has the correct minimum interval between executions."""
        interval = ProcessOwnersToBeDeletedCronTask.get_min_seconds_interval_between_executions()
        assert interval == 60 * 60  # 1 hour

    @patch("django.conf.settings")
    def test_run_cron_task_no_owners(self, mock_settings):
        """Test that the task handles the case when no owners are found."""
        mock_settings.MAX_OWNERS_TO_DELETE_PER_CRON_RUN = 10

        # Mock empty queryset
        with patch(
            "shared.django_apps.codecov_auth.models.OwnerToBeDeleted.objects.all"
        ) as mock_queryset:
            mock_queryset.return_value = []

            task = ProcessOwnersToBeDeletedCronTask()
            # Mock the app.tasks access
            mock_delete_task = MagicMock()
            task.app = MagicMock()
            task.app.tasks = {"app.tasks.delete_owner.DeleteOwner": mock_delete_task}

            result = task.run_cron_task(MagicMock())

            assert result["owners_processed"] == 0
            assert result["tasks_started"] == 0
            assert result["message"] == "No owners to process"
            mock_delete_task.apply_async.assert_not_called()

    @patch("django.conf.settings")
    def test_run_cron_task_with_owners(self, mock_settings):
        """Test that the task processes owners correctly."""
        mock_settings.MAX_OWNERS_TO_DELETE_PER_CRON_RUN = 5

        # Create mock owner records
        mock_owners = [
            MagicMock(owner_id=1),
            MagicMock(owner_id=2),
            MagicMock(owner_id=3),
        ]

        with patch(
            "shared.django_apps.codecov_auth.models.OwnerToBeDeleted.objects.all"
        ) as mock_queryset:
            mock_queryset.return_value = mock_owners[:2]  # Limit to 2 owners

            task = ProcessOwnersToBeDeletedCronTask()
            # Mock the app.tasks access
            mock_delete_task = MagicMock()
            task.app = MagicMock()
            task.app.tasks = {"app.tasks.delete_owner.DeleteOwner": mock_delete_task}

            result = task.run_cron_task(MagicMock())

            assert result["owners_processed"] == 2
            assert "Processed 2 owners" in result["message"]

            # Verify delete_owner_task was called for each owner
            assert mock_delete_task.apply_async.call_count == 2
            mock_delete_task.apply_async.assert_any_call(kwargs={"ownerid": 1})
            mock_delete_task.apply_async.assert_any_call(kwargs={"ownerid": 2})

    @patch("django.conf.settings")
    def test_run_cron_task_respects_limit(self, mock_settings):
        """Test that the task respects the maximum owners per run limit."""
        mock_settings.MAX_OWNERS_TO_DELETE_PER_CRON_RUN = 2

        # Create mock owner records
        mock_owners = [
            MagicMock(owner_id=1),
            MagicMock(owner_id=2),
            MagicMock(owner_id=3),
            MagicMock(owner_id=4),
        ]

        with patch(
            "shared.django_apps.codecov_auth.models.OwnerToBeDeleted.objects.all"
        ) as mock_queryset:
            mock_queryset.return_value = mock_owners[:2]  # Limit to 2 owners

            task = ProcessOwnersToBeDeletedCronTask()
            # Mock the app.tasks access
            mock_delete_task = MagicMock()
            task.app = MagicMock()
            task.app.tasks = {"app.tasks.delete_owner.DeleteOwner": mock_delete_task}

            result = task.run_cron_task(MagicMock())

            assert result["owners_processed"] == 2
            assert mock_delete_task.apply_async.call_count == 2

    @patch("django.conf.settings")
    def test_run_cron_task_handles_failure(self, mock_settings):
        """Test that the task handles failures gracefully."""
        mock_settings.MAX_OWNERS_TO_DELETE_PER_CRON_RUN = 10

        # Create mock owner records
        mock_owner1 = MagicMock(owner_id=1)
        mock_owner2 = MagicMock(owner_id=2)
        mock_owners = [mock_owner1, mock_owner2]

        with patch(
            "shared.django_apps.codecov_auth.models.OwnerToBeDeleted.objects.all"
        ) as mock_queryset:
            mock_queryset.return_value = mock_owners

            task = ProcessOwnersToBeDeletedCronTask()
            # Mock the app.tasks access
            mock_delete_task = MagicMock()
            task.app = MagicMock()
            task.app.tasks = {"app.tasks.delete_owner.DeleteOwner": mock_delete_task}

            # Make the first owner fail
            mock_delete_task.apply_async.side_effect = [Exception("Task failed"), None]

            result = task.run_cron_task(MagicMock())

            # Only the second owner should be processed successfully
            assert result["owners_processed"] == 1
            assert mock_delete_task.apply_async.call_count == 2

    @patch("django.conf.settings")
    def test_run_cron_task_default_limit(self, mock_settings):
        """Test that the task uses the default limit when setting is not configured."""
        # Don't set MAX_OWNERS_TO_DELETE_PER_CRON_RUN

        mock_owners = [
            MagicMock(owner_id=1),
            MagicMock(owner_id=2),
        ]

        with patch(
            "shared.django_apps.codecov_auth.models.OwnerToBeDeleted.objects.all"
        ) as mock_queryset:
            mock_queryset.return_value = mock_owners

            task = ProcessOwnersToBeDeletedCronTask()
            # Mock the app.tasks access
            mock_delete_task = MagicMock()
            task.app = MagicMock()
            task.app.tasks = {"app.tasks.delete_owner.DeleteOwner": mock_delete_task}

            result = task.run_cron_task(MagicMock())

            assert result["owners_processed"] == 2
