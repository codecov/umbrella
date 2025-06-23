from unittest.mock import patch

from tasks.partition_management import PartitionManagementTask


class TestPartitionsManagement:
    def __mock_output_with_exception(self, *args, **kwargs):
        print("Mocking sys.stdout")  # noqa: T201
        raise Exception("Test exception")

    @patch("tasks.partition_management.call_command")
    def test_run_cron_task_success(self, mock_call_command, dbsession):
        mock_call_command.return_value = None
        task = PartitionManagementTask()
        result = task.run_cron_task(dbsession)
        assert result == {"successful": True}
        assert mock_call_command.called

    @patch("tasks.partition_management.call_command")
    def test_run_cron_task_failure(self, mock_call_command, dbsession):
        mock_call_command.side_effect = self.__mock_output_with_exception
        task = PartitionManagementTask()
        result = task.run_cron_task(dbsession)
        assert result == {
            "successful": False,
            "reason": "Failed with exception",
            "exception": "Test exception",
            "task_output": "Mocking sys.stdout\n",
        }
        assert mock_call_command.called

    def test_get_min_seconds_interval_between_executions(self, dbsession):
        assert isinstance(
            PartitionManagementTask.get_min_seconds_interval_between_executions(), int
        )
        # The specifics don't matter, but the number needs to be somewhat big
        assert (
            PartitionManagementTask.get_min_seconds_interval_between_executions() > 600
        )
