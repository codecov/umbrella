import random

import pytest
from celery.exceptions import MaxRetriesExceededError, Retry

from database.models import Pull
from database.tests.factories import CommitFactory, PullFactory
from database.tests.factories.core import UploadFactory
from services.lock_manager import LockRetry
from shared.celery_config import TASK_MAX_RETRIES_DEFAULT
from shared.reports.enums import UploadState
from tasks.manual_trigger import ManualTriggerTask
from tasks.tests.utils import ensure_hard_time_limit_task_is_numeric


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
        mocked_app = mocker.patch.object(
            ManualTriggerTask,
            "app",
            tasks={
                "app.tasks.notify.Notify": mocker.MagicMock(),
                "app.tasks.pulls.Sync": mocker.MagicMock(),
                "app.tasks.compute_comparison.ComputeComparison": mocker.MagicMock(),
            },
        )
        task = ManualTriggerTask()
        ensure_hard_time_limit_task_is_numeric(mocker, task)
        commit = CommitFactory.create(pullid=None)
        pull = PullFactory.create(repository=commit.repository, head=commit.commitid)
        commit.pullid = pull.pullid
        dbsession.add(pull)
        dbsession.flush()

        dbsession.add(commit)

        # Upload is complete in database (state = PROCESSED)
        upload = UploadFactory.create(
            report__commit=commit,
            state="processed",
            state_id=UploadState.PROCESSED.db_id,
        )
        compared_to = CommitFactory.create(repository=commit.repository)
        pull.compared_to = compared_to.commitid

        dbsession.add(upload)
        dbsession.add(compared_to)
        dbsession.add(pull)
        dbsession.flush()
        result = task.run_impl(
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

    def test_manual_upload_completion_trigger_with_uploaded_state(
        self,
        mocker,
        mock_configuration,
        dbsession,
        mock_storage,
        mock_redis,
        celery_app,
    ):
        """Test that task retries when DB shows uploads in UPLOADED state"""
        mocker.patch.object(
            ManualTriggerTask,
            "app",
            celery_app,
        )
        commit = CommitFactory.create()
        # One upload is still in UPLOADED state (being processed)
        upload1 = UploadFactory.create(
            report__commit=commit,
            state="started",
            state_id=UploadState.UPLOADED.db_id,
        )
        # Another upload is complete
        upload2 = UploadFactory.create(
            report__commit=commit,
            state="processed",
            state_id=UploadState.PROCESSED.db_id,
        )
        dbsession.add(commit)
        dbsession.add(upload1)
        dbsession.add(upload2)
        dbsession.flush()

        # Should retry because DB shows upload still in UPLOADED state
        with pytest.raises(Retry):
            ManualTriggerTask().run_impl(
                dbsession,
                repoid=commit.repoid,
                commitid=commit.commitid,
                current_yaml={},
            )

    def test_lock_retry_max_retries_exceeded(
        self, mocker, mock_configuration, dbsession, mock_storage, mock_redis
    ):
        """Test that LockRetry returns failure dict when max retries exceeded (task attempts)."""
        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()

        # Mock LockManager to raise LockRetry
        m = mocker.MagicMock()
        m.return_value.locked.return_value.__enter__.side_effect = LockRetry(60)
        mocker.patch("tasks.manual_trigger.LockManager", m)

        task = ManualTriggerTask()
        task.request.retries = TASK_MAX_RETRIES_DEFAULT  # Max retries exceeded
        ensure_hard_time_limit_task_is_numeric(mocker, task)

        result = task.run_impl(
            dbsession, repoid=commit.repoid, commitid=commit.commitid, current_yaml={}
        )

        assert result == {
            "notifications_called": False,
            "message": "Unable to acquire lock",
        }

    def test_lock_retry_max_retries_exceeded_from_lock_manager(
        self, mocker, mock_configuration, dbsession, mock_storage, mock_redis
    ):
        """Test that LockRetry with max_retries_exceeded=True returns failure dict (Redis counter path)."""
        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()

        m = mocker.MagicMock()
        m.return_value.locked.return_value.__enter__.side_effect = LockRetry(
            countdown=0,
            max_retries_exceeded=True,
            retry_num=TASK_MAX_RETRIES_DEFAULT,
            max_retries=TASK_MAX_RETRIES_DEFAULT,
        )
        mocker.patch("tasks.manual_trigger.LockManager", m)

        task = ManualTriggerTask()
        task.request.retries = 0
        task.request.headers = {"attempts": 1}
        ensure_hard_time_limit_task_is_numeric(mocker, task)

        result = task.run_impl(
            dbsession, repoid=commit.repoid, commitid=commit.commitid, current_yaml={}
        )

        assert result == {
            "notifications_called": False,
            "message": "Unable to acquire lock",
        }

    def test_lock_retry_will_retry(
        self, mocker, mock_configuration, dbsession, mock_storage, mock_redis
    ):
        """Test that LockRetry calls self.retry() when retries available"""
        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()

        # Mock LockManager to raise LockRetry
        m = mocker.MagicMock()
        m.return_value.locked.return_value.__enter__.side_effect = LockRetry(60)
        mocker.patch("tasks.manual_trigger.LockManager", m)

        task = ManualTriggerTask()
        task.request.retries = 0  # Will retry
        ensure_hard_time_limit_task_is_numeric(mocker, task)

        # Task should call self.retry() which raises Retry exception
        with pytest.raises(Retry):
            task.run_impl(
                dbsession,
                repoid=commit.repoid,
                commitid=commit.commitid,
                current_yaml={},
            )

    def test_max_retries_exceeded_uploads_still_processing(
        self,
        mocker,
        mock_configuration,
        dbsession,
        mock_storage,
        mock_redis,
        celery_app,
    ):
        """Test that MaxRetriesExceededError returns failure dict when uploads still processing"""
        mocker.patch.object(ManualTriggerTask, "app", celery_app)
        commit = CommitFactory.create()
        upload = UploadFactory.create(report__commit=commit, state="", state_id=None)
        dbsession.add(commit)
        dbsession.add(upload)
        dbsession.flush()

        task = ManualTriggerTask()
        task.request.retries = (
            5  # Max retries exceeded (max_retries=5 in process_impl_within_lock)
        )

        # Mock self.retry() to raise MaxRetriesExceededError
        def raise_max_retries(*args, **kwargs):
            raise MaxRetriesExceededError()

        mocker.patch.object(task, "retry", side_effect=raise_max_retries)

        result = task.process_impl_within_lock(
            db_session=dbsession,
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml={},
        )

        assert result == {
            "notifications_called": False,
            "message": "Uploads are still in process and the task got retired so many times. Not triggering notifications.",
        }

    def test_commit_without_pullid_no_pull_sync(
        self,
        mocker,
        mock_configuration,
        dbsession,
        mock_storage,
        mock_redis,
        celery_app,
    ):
        """Test that commit without pullid does not trigger pull sync"""
        mocked_app = mocker.patch.object(
            ManualTriggerTask,
            "app",
            tasks={
                "app.tasks.notify.Notify": mocker.MagicMock(),
                "app.tasks.pulls.Sync": mocker.MagicMock(),
                "app.tasks.compute_comparison.ComputeComparison": mocker.MagicMock(),
            },
        )
        task = ManualTriggerTask()
        ensure_hard_time_limit_task_is_numeric(mocker, task)
        commit = CommitFactory.create(pullid=None)  # No pullid
        dbsession.add(commit)

        # Upload is complete
        upload = UploadFactory.create(
            report__commit=commit,
            state="processed",
            state_id=UploadState.PROCESSED.db_id,
        )
        dbsession.add(upload)
        dbsession.flush()

        result = task.run_impl(
            dbsession, repoid=commit.repoid, commitid=commit.commitid, current_yaml={}
        )

        assert result == {
            "notifications_called": True,
            "message": "All uploads are processed. Triggering notifications.",
        }
        # Should call notify but NOT pull sync
        mocked_app.tasks["app.tasks.notify.Notify"].apply_async.assert_called_once()
        mocked_app.tasks["app.tasks.pulls.Sync"].apply_async.assert_not_called()
        mocked_app.tasks[
            "app.tasks.compute_comparison.ComputeComparison"
        ].apply_async.assert_not_called()

    def test_pull_does_not_exist_no_pull_sync(
        self,
        mocker,
        mock_configuration,
        dbsession,
        mock_storage,
        mock_redis,
        celery_app,
    ):
        """Test that when pull doesn't exist, pull sync is not scheduled"""
        mocked_app = mocker.patch.object(
            ManualTriggerTask,
            "app",
            tasks={
                "app.tasks.notify.Notify": mocker.MagicMock(),
                "app.tasks.pulls.Sync": mocker.MagicMock(),
                "app.tasks.compute_comparison.ComputeComparison": mocker.MagicMock(),
            },
        )
        task = ManualTriggerTask()
        ensure_hard_time_limit_task_is_numeric(mocker, task)
        # Create commit first to get repoid
        commit = CommitFactory.create(pullid=None)
        dbsession.add(commit)
        dbsession.flush()

        # Use a unique pullid that definitely doesn't exist for this repo
        # Keep trying until we find one that doesn't exist
        unique_pullid = None
        for _ in range(10):
            candidate_pullid = random.randint(1000000, 9999999)
            existing_pull = (
                dbsession.query(Pull)
                .filter_by(repoid=commit.repoid, pullid=candidate_pullid)
                .first()
            )
            if existing_pull is None:
                unique_pullid = candidate_pullid
                break

        if unique_pullid is None:
            pytest.skip("Could not find a unique pullid")

        commit.pullid = unique_pullid
        dbsession.add(commit)

        # Upload is complete
        upload = UploadFactory.create(
            report__commit=commit,
            state="processed",
            state_id=UploadState.PROCESSED.db_id,
        )
        dbsession.add(upload)
        dbsession.flush()

        # Verify no pull exists after flush (check with actual repoid)
        pull = (
            dbsession.query(Pull)
            .filter_by(repoid=commit.repoid, pullid=commit.pullid)
            .first()
        )
        # If pull exists, delete it to ensure test correctness
        if pull is not None:
            dbsession.delete(pull)
            dbsession.flush()

        result = task.run_impl(
            dbsession, repoid=commit.repoid, commitid=commit.commitid, current_yaml={}
        )

        assert result == {
            "notifications_called": True,
            "message": "All uploads are processed. Triggering notifications.",
        }
        # Should call notify but NOT pull sync (pull doesn't exist)
        mocked_app.tasks["app.tasks.notify.Notify"].apply_async.assert_called_once()
        mocked_app.tasks["app.tasks.pulls.Sync"].apply_async.assert_not_called()
        mocked_app.tasks[
            "app.tasks.compute_comparison.ComputeComparison"
        ].apply_async.assert_not_called()

    def test_pull_head_newer_timestamp_no_update(
        self,
        mocker,
        mock_configuration,
        dbsession,
        mock_storage,
        mock_redis,
        celery_app,
    ):
        """Test that when pull head commit has newer timestamp, pull.head is not updated"""
        mocked_app = mocker.patch.object(
            ManualTriggerTask,
            "app",
            tasks={
                "app.tasks.notify.Notify": mocker.MagicMock(),
                "app.tasks.pulls.Sync": mocker.MagicMock(),
                "app.tasks.compute_comparison.ComputeComparison": mocker.MagicMock(),
            },
        )
        task = ManualTriggerTask()
        ensure_hard_time_limit_task_is_numeric(mocker, task)

        # Create a newer commit (head of pull)
        newer_commit = CommitFactory.create()
        newer_commit.timestamp = newer_commit.timestamp.replace(year=2025)

        # Create current commit (older)
        commit = CommitFactory.create(repository=newer_commit.repository)
        commit.timestamp = commit.timestamp.replace(year=2024)
        dbsession.add(newer_commit)
        dbsession.add(commit)
        dbsession.flush()

        # Use a unique pullid to avoid conflicts
        unique_pullid = random.randint(1000000, 9999999)
        commit.pullid = unique_pullid
        dbsession.add(commit)
        dbsession.flush()

        # Check if pull exists, if so delete it
        existing_pull = (
            dbsession.query(Pull)
            .filter_by(repoid=commit.repoid, pullid=commit.pullid)
            .first()
        )
        if existing_pull is not None:
            dbsession.delete(existing_pull)
            dbsession.flush()

        pull = PullFactory.create(
            repository=commit.repository,
            pullid=commit.pullid,
            head=newer_commit.commitid,
        )
        dbsession.add(pull)
        dbsession.flush()

        # Upload is complete
        upload = UploadFactory.create(
            report__commit=commit,
            state="processed",
            state_id=UploadState.PROCESSED.db_id,
        )
        dbsession.add(upload)
        dbsession.flush()

        result = task.run_impl(
            dbsession, repoid=commit.repoid, commitid=commit.commitid, current_yaml={}
        )

        assert result == {
            "notifications_called": True,
            "message": "All uploads are processed. Triggering notifications.",
        }
        # pull.head should not be updated (newer commit is head)
        assert pull.head == newer_commit.commitid
        # Should call notify but NOT pull sync (pull.head != commit.commitid)
        mocked_app.tasks["app.tasks.notify.Notify"].apply_async.assert_called_once()
        mocked_app.tasks["app.tasks.pulls.Sync"].apply_async.assert_not_called()
        mocked_app.tasks[
            "app.tasks.compute_comparison.ComputeComparison"
        ].apply_async.assert_not_called()

    def test_pull_head_different_commit_no_pull_sync(
        self,
        mocker,
        mock_configuration,
        dbsession,
        mock_storage,
        mock_redis,
        celery_app,
    ):
        """Test that when pull.head != commit.commitid, pull sync is not scheduled"""
        mocked_app = mocker.patch.object(
            ManualTriggerTask,
            "app",
            tasks={
                "app.tasks.notify.Notify": mocker.MagicMock(),
                "app.tasks.pulls.Sync": mocker.MagicMock(),
                "app.tasks.compute_comparison.ComputeComparison": mocker.MagicMock(),
            },
        )
        task = ManualTriggerTask()
        ensure_hard_time_limit_task_is_numeric(mocker, task)

        # Use a unique pullid to avoid conflicts
        unique_pullid = random.randint(1000000, 9999999)
        commit = CommitFactory.create(pullid=unique_pullid)
        # Create other_commit with newer timestamp so pull.head won't be updated
        other_commit = CommitFactory.create(repository=commit.repository)
        other_commit.timestamp = other_commit.timestamp.replace(year=2025)
        commit.timestamp = commit.timestamp.replace(year=2024)
        dbsession.add(commit)
        dbsession.add(other_commit)
        dbsession.flush()

        # Check if pull exists, if so delete it
        existing_pull = (
            dbsession.query(Pull)
            .filter_by(repoid=commit.repoid, pullid=commit.pullid)
            .first()
        )
        if existing_pull is not None:
            dbsession.delete(existing_pull)
            dbsession.flush()

        pull = PullFactory.create(
            repository=commit.repository,
            pullid=commit.pullid,
            head=other_commit.commitid,
        )
        dbsession.add(pull)
        dbsession.flush()

        # Verify pull.head is still other_commit.commitid (not updated because other_commit.timestamp > commit.timestamp)
        dbsession.refresh(pull)
        assert pull.head == other_commit.commitid, (
            "pull.head should not be updated when head commit timestamp is newer"
        )

        # Upload is complete
        upload = UploadFactory.create(
            report__commit=commit,
            state="processed",
            state_id=UploadState.PROCESSED.db_id,
        )
        dbsession.add(upload)
        dbsession.flush()

        result = task.run_impl(
            dbsession, repoid=commit.repoid, commitid=commit.commitid, current_yaml={}
        )

        assert result == {
            "notifications_called": True,
            "message": "All uploads are processed. Triggering notifications.",
        }
        # Should call notify but NOT pull sync (pull.head != commit.commitid)
        mocked_app.tasks["app.tasks.notify.Notify"].apply_async.assert_called_once()
        mocked_app.tasks["app.tasks.pulls.Sync"].apply_async.assert_not_called()
        mocked_app.tasks[
            "app.tasks.compute_comparison.ComputeComparison"
        ].apply_async.assert_not_called()

    def test_pull_no_compared_to_no_comparison(
        self,
        mocker,
        mock_configuration,
        dbsession,
        mock_storage,
        mock_redis,
        celery_app,
    ):
        """Test that when pull has no compared_to, comparison is not scheduled"""
        mocked_app = mocker.patch.object(
            ManualTriggerTask,
            "app",
            tasks={
                "app.tasks.notify.Notify": mocker.MagicMock(),
                "app.tasks.pulls.Sync": mocker.MagicMock(),
                "app.tasks.compute_comparison.ComputeComparison": mocker.MagicMock(),
            },
        )
        task = ManualTriggerTask()
        ensure_hard_time_limit_task_is_numeric(mocker, task)

        commit = CommitFactory.create(pullid=None)
        dbsession.add(commit)
        dbsession.flush()

        # Create pull and set commit.pullid
        pull = PullFactory.create(
            repository=commit.repository, head=commit.commitid, compared_to=None
        )
        commit.pullid = pull.pullid
        dbsession.add(commit)
        dbsession.flush()

        # Verify pull doesn't already exist with this repoid+pullid combo
        # Query again to check for duplicates
        existing_pulls = (
            dbsession.query(Pull)
            .filter_by(repoid=commit.repoid, pullid=commit.pullid)
            .all()
        )
        # If there are multiple pulls, delete all but the one we just created
        if len(existing_pulls) > 1:
            for existing_pull in existing_pulls:
                if existing_pull.id_ != pull.id_:
                    dbsession.delete(existing_pull)
            dbsession.flush()
        elif len(existing_pulls) == 1 and existing_pulls[0].id_ != pull.id_:
            # If there's one existing pull that's not ours, delete it
            dbsession.delete(existing_pulls[0])
            dbsession.flush()

        # Make sure our pull is added
        if pull not in dbsession:
            dbsession.add(pull)
        dbsession.flush()

        dbsession.add(commit)
        dbsession.add(pull)
        dbsession.flush()

        # Upload is complete
        upload = UploadFactory.create(
            report__commit=commit,
            state="processed",
            state_id=UploadState.PROCESSED.db_id,
        )
        dbsession.add(upload)
        dbsession.flush()

        result = task.run_impl(
            dbsession, repoid=commit.repoid, commitid=commit.commitid, current_yaml={}
        )

        assert result == {
            "notifications_called": True,
            "message": "All uploads are processed. Triggering notifications.",
        }
        # Should call notify and pull sync but NOT comparison (no compared_to)
        mocked_app.tasks["app.tasks.notify.Notify"].apply_async.assert_called_once()
        mocked_app.tasks["app.tasks.pulls.Sync"].apply_async.assert_called_once()
        mocked_app.tasks[
            "app.tasks.compute_comparison.ComputeComparison"
        ].apply_async.assert_not_called()

    def test_no_uploads_triggers_notifications(
        self,
        mocker,
        mock_configuration,
        dbsession,
        mock_storage,
        mock_redis,
        celery_app,
    ):
        """Test that when no uploads exist, notifications are still triggered"""
        mocked_app = mocker.patch.object(
            ManualTriggerTask,
            "app",
            tasks={
                "app.tasks.notify.Notify": mocker.MagicMock(),
                "app.tasks.pulls.Sync": mocker.MagicMock(),
                "app.tasks.compute_comparison.ComputeComparison": mocker.MagicMock(),
            },
        )
        task = ManualTriggerTask()
        ensure_hard_time_limit_task_is_numeric(mocker, task)

        commit = CommitFactory.create(pullid=None)
        dbsession.add(commit)
        dbsession.flush()

        # No uploads created

        result = task.run_impl(
            dbsession, repoid=commit.repoid, commitid=commit.commitid, current_yaml={}
        )

        assert result == {
            "notifications_called": True,
            "message": "All uploads are processed. Triggering notifications.",
        }
        mocked_app.tasks["app.tasks.notify.Notify"].apply_async.assert_called_once()
