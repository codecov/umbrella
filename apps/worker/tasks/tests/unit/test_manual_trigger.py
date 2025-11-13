import pytest
from celery.exceptions import Retry

from database.tests.factories import CommitFactory, PullFactory
from database.tests.factories.core import UploadFactory
from services.processing.state import UploadNumbers
from shared.reports.enums import UploadState
from tasks.manual_trigger import ManualTriggerTask


class TestUploadCompletionTask:
    def test_manual_upload_completion_trigger(
        self,
        mocker,
        mock_configuration,
        dbsession,
        mock_storage,
        mock_redis,
        celery_app,
    ):
        # Mock ProcessingState to return no pending uploads in Redis
        mock_processing_state = mocker.patch("tasks.manual_trigger.ProcessingState")
        mock_state_instance = mocker.MagicMock()
        mock_state_instance.get_upload_numbers.return_value = UploadNumbers(
            processing=0, processed=0
        )
        mock_processing_state.return_value = mock_state_instance

        mocked_app = mocker.patch.object(
            ManualTriggerTask,
            "app",
            tasks={
                "app.tasks.notify.Notify": mocker.MagicMock(),
                "app.tasks.pulls.Sync": mocker.MagicMock(),
                "app.tasks.compute_comparison.ComputeComparison": mocker.MagicMock(),
            },
        )
        commit = CommitFactory.create(pullid=None)
        pull = PullFactory.create(repository=commit.repository, head=commit.commitid)
        commit.pullid = pull.pullid
        dbsession.add(pull)
        dbsession.flush()

        dbsession.add(commit)

        upload = UploadFactory.create(report__commit=commit)
        compared_to = CommitFactory.create(repository=commit.repository)
        pull.compared_to = compared_to.commitid

        dbsession.add(upload)
        dbsession.add(compared_to)
        dbsession.add(pull)
        dbsession.flush()
        result = ManualTriggerTask().run_impl(
            dbsession, repoid=commit.repoid, commitid=commit.commitid, current_yaml={}
        )
        assert {
            "notifications_called": True,
            "message": "All uploads are processed. Triggering notifications.",
        } == result
        mocked_app.tasks["app.tasks.notify.Notify"].apply_async.assert_called_with(
            kwargs={
                "commitid": commit.commitid,
                "current_yaml": None,
                "repoid": commit.repoid,
            }
        )
        mocked_app.tasks["app.tasks.pulls.Sync"].apply_async.assert_called_with(
            kwargs={
                "pullid": commit.pullid,
                "repoid": commit.repoid,
                "should_send_notifications": False,
            }
        )
        assert mocked_app.send_task.call_count == 0

        mocked_app.tasks[
            "app.tasks.compute_comparison.ComputeComparison"
        ].apply_async.assert_called_once()

    def test_manual_upload_completion_trigger_uploads_still_processing(
        self,
        mocker,
        mock_configuration,
        dbsession,
        mock_storage,
        mock_redis,
        celery_app,
    ):
        # Mock ProcessingState to return no pending uploads in Redis
        mock_processing_state = mocker.patch("tasks.manual_trigger.ProcessingState")
        mock_state_instance = mocker.MagicMock()
        mock_state_instance.get_upload_numbers.return_value = UploadNumbers(
            processing=0, processed=0
        )
        mock_processing_state.return_value = mock_state_instance

        mocker.patch.object(
            ManualTriggerTask,
            "app",
            celery_app,
        )
        commit = CommitFactory.create()
        upload = UploadFactory.create(report__commit=commit, state="", state_id=None)
        upload2 = UploadFactory.create(
            report__commit=commit,
            state="started",
            state_id=UploadState.UPLOADED.db_id,
        )
        dbsession.add(commit)
        dbsession.add(upload)
        dbsession.add(upload2)
        dbsession.flush()
        with pytest.raises(Retry):
            result = ManualTriggerTask().run_impl(
                dbsession,
                repoid=commit.repoid,
                commitid=commit.commitid,
                current_yaml={},
            )
            assert {
                "notifications_called": False,
                "message": "Uploads are still in process and the task got retired so many times. Not triggering notifications.",
            } == result

    def test_manual_upload_completion_trigger_redis_pending(
        self,
        mocker,
        mock_configuration,
        dbsession,
        mock_storage,
        mock_redis,
        celery_app,
    ):
        """Test that task retries when Redis shows pending uploads even if DB shows complete"""
        # Mock ProcessingState to return pending uploads in Redis
        mock_processing_state = mocker.patch("tasks.manual_trigger.ProcessingState")
        mock_state_instance = mocker.MagicMock()
        mock_state_instance.get_upload_numbers.return_value = UploadNumbers(
            processing=1,
            processed=2,  # Redis shows 3 uploads still being processed/merged
        )
        mock_processing_state.return_value = mock_state_instance

        mocker.patch.object(
            ManualTriggerTask,
            "app",
            celery_app,
        )
        commit = CommitFactory.create()
        # Upload is complete in DB
        upload = UploadFactory.create(
            report__commit=commit,
            state="complete",
            state_id=UploadState.PROCESSED.db_id,
        )
        dbsession.add(commit)
        dbsession.add(upload)
        dbsession.flush()

        # Should retry because Redis shows pending uploads
        with pytest.raises(Retry):
            ManualTriggerTask().run_impl(
                dbsession,
                repoid=commit.repoid,
                commitid=commit.commitid,
                current_yaml={},
            )
