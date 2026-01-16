from contextlib import contextmanager

import pytest
from celery.exceptions import Retry

from database.tests.factories import CommitFactory
from services.bundle_analysis.notify import BundleAnalysisNotifyReturn
from services.bundle_analysis.notify.types import (
    NotificationSuccess,
    NotificationType,
)
from services.lock_manager import LockRetry
from services.notification.debounce import (
    SKIP_DEBOUNCE_TOKEN,
    LockAcquisitionLimitError,
)
from tasks.bundle_analysis_notify import BundleAnalysisNotifyTask


def test_bundle_analysis_notify_task(
    mocker,
    dbsession,
    celery_app,
    mock_redis,
):
    mocker.patch.object(BundleAnalysisNotifyTask, "app", celery_app)

    commit = CommitFactory.create()
    dbsession.add(commit)
    dbsession.flush()

    mocker.patch(
        "services.bundle_analysis.notify.BundleAnalysisNotifyService.notify",
        return_value=BundleAnalysisNotifyReturn(
            notifications_configured=(NotificationType.PR_COMMENT,),
            notifications_attempted=(NotificationType.PR_COMMENT,),
            notifications_successful=(NotificationType.PR_COMMENT,),
        ),
    )

    result = BundleAnalysisNotifyTask().run_impl(
        dbsession,
        [{"error": None}],
        repoid=commit.repoid,
        commitid=commit.commitid,
        commit_yaml={},
        fencing_token=SKIP_DEBOUNCE_TOKEN,
    )
    assert result == {
        "notify_attempted": True,
        "notify_succeeded": NotificationSuccess.FULL_SUCCESS,
    }


def test_bundle_analysis_notify_skips_if_all_processing_fail(dbsession):
    commit = CommitFactory.create()
    dbsession.add(commit)
    dbsession.flush()
    result = BundleAnalysisNotifyTask().run_impl(
        dbsession,
        [{"error": True}],
        repoid=commit.repoid,
        commitid=commit.commitid,
        commit_yaml={},
        fencing_token=SKIP_DEBOUNCE_TOKEN,
    )
    assert result == {
        "notify_attempted": False,
        "notify_succeeded": NotificationSuccess.ALL_ERRORED,
    }


def test_debounce_retry_includes_all_required_params(
    mocker, dbsession, celery_app, mock_redis
):
    """Test that retry kwargs include all required parameters for debounce retry.

    This test ensures that when the debounce logic triggers a retry, all required
    parameters are included in the kwargs passed to self.retry(). This prevents
    TypeError when Celery attempts to retry the task.
    """
    mocker.patch.object(BundleAnalysisNotifyTask, "app", celery_app)

    commit = CommitFactory.create()
    dbsession.add(commit)
    dbsession.flush()

    # Mock Redis to allow token acquisition
    mock_redis.pipeline.return_value.__enter__.return_value.execute.return_value = ["1"]
    mock_redis.get.return_value = False

    task = BundleAnalysisNotifyTask()
    task.request.retries = 0
    task.max_retries = 5

    # Mock retry to capture kwargs
    mock_retry = mocker.patch.object(task, "retry", side_effect=Retry())

    previous_result = [{"error": None}]
    commit_yaml = {"codecov": {"flags": ["test"]}}

    with pytest.raises(Retry):
        task.run_impl(
            dbsession,
            previous_result,
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml=commit_yaml,
            # Don't pass fencing_token - let it go through retry path
        )

    # Verify retry was called
    assert mock_retry.called

    # CRITICAL: Verify all required params are in kwargs
    call_kwargs = mock_retry.call_args[1]["kwargs"]
    assert call_kwargs["previous_result"] == previous_result
    assert call_kwargs["repoid"] == commit.repoid
    assert call_kwargs["commitid"] == commit.commitid
    assert call_kwargs["commit_yaml"] == commit_yaml  # Should be dict, not UserYaml
    assert isinstance(call_kwargs["commit_yaml"], dict)  # Verify type
    assert call_kwargs["fencing_token"] == 1  # Token from acquisition
    assert mock_retry.call_args[1]["countdown"] == 30  # DEBOUNCE_PERIOD_SECONDS


def test_bundle_analysis_notify_lock_retry_limit_exceeded_during_token_acquisition(
    mocker, dbsession, celery_app, mock_redis
):
    """Test that LockAcquisitionLimitError during token acquisition returns graceful failure."""
    mocker.patch.object(BundleAnalysisNotifyTask, "app", celery_app)

    commit = CommitFactory.create()
    dbsession.add(commit)
    dbsession.flush()

    # Mock Redis to allow token acquisition attempt
    mock_redis.pipeline.return_value.__enter__.return_value.execute.return_value = ["1"]

    task = BundleAnalysisNotifyTask()
    task.request.retries = 0
    task.max_retries = 5

    # Mock debouncer's notification_lock to raise LockAcquisitionLimitError
    mock_notification_lock = mocker.patch.object(
        task.debouncer,
        "notification_lock",
        side_effect=LockAcquisitionLimitError("Max retries exceeded"),
    )

    result = task.run_impl(
        dbsession,
        [{"error": None}],
        repoid=commit.repoid,
        commitid=commit.commitid,
        commit_yaml={},
        # Don't pass fencing_token - let it go through token acquisition path
    )

    assert result == {
        "notify_attempted": False,
        "notify_succeeded": None,
    }


def test_bundle_analysis_notify_lock_retry_during_token_acquisition(
    mocker, dbsession, celery_app, mock_redis
):
    """Test that LockRetry during token acquisition triggers task retry."""
    mocker.patch.object(BundleAnalysisNotifyTask, "app", celery_app)

    commit = CommitFactory.create()
    dbsession.add(commit)
    dbsession.flush()

    # Mock Redis to allow token acquisition attempt
    mock_redis.pipeline.return_value.__enter__.return_value.execute.return_value = ["1"]

    task = BundleAnalysisNotifyTask()
    task.request.retries = 0
    task.max_retries = 5
    task.request.headers = {}

    # Mock debouncer's notification_lock to raise LockRetry
    @contextmanager
    def mock_lock_raises_retry(*args, **kwargs):
        raise LockRetry(60)

    mocker.patch.object(
        task.debouncer, "notification_lock", side_effect=mock_lock_raises_retry
    )

    # Mock retry to raise Retry exception
    mock_retry = mocker.patch.object(task, "retry", side_effect=Retry())

    with pytest.raises(Retry):
        task.run_impl(
            dbsession,
            [{"error": None}],
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml={},
            # Don't pass fencing_token - let it go through token acquisition path
        )

    # Verify retry was called with max_retries from debouncer
    assert mock_retry.called


def test_bundle_analysis_notify_lock_retry_max_retries_exceeded_during_token_acquisition(
    mocker, dbsession, celery_app, mock_redis
):
    """Test that when max retries exceeded during LockRetry handling, returns graceful failure."""
    mocker.patch.object(BundleAnalysisNotifyTask, "app", celery_app)

    commit = CommitFactory.create()
    dbsession.add(commit)
    dbsession.flush()

    # Mock Redis to allow token acquisition attempt
    mock_redis.pipeline.return_value.__enter__.return_value.execute.return_value = ["1"]

    task = BundleAnalysisNotifyTask()
    task.request.retries = 0
    task.max_retries = 5
    task.request.headers = {}

    # Mock debouncer's notification_lock to raise LockRetry
    @contextmanager
    def mock_lock_raises_retry(*args, **kwargs):
        raise LockRetry(60)

    mocker.patch.object(
        task.debouncer, "notification_lock", side_effect=mock_lock_raises_retry
    )

    # Mock handle_lock_retry to return failure result (simulating max retries exceeded)
    mocker.patch.object(
        task.debouncer,
        "handle_lock_retry",
        return_value={
            "notify_attempted": False,
            "notify_succeeded": None,
        },
    )

    result = task.run_impl(
        dbsession,
        [{"error": None}],
        repoid=commit.repoid,
        commitid=commit.commitid,
        commit_yaml={},
        # Don't pass fencing_token - let it go through token acquisition path
    )

    assert result == {
        "notify_attempted": False,
        "notify_succeeded": None,
    }


def test_bundle_analysis_notify_stale_fencing_token(
    mocker, dbsession, celery_app, mock_redis
):
    """Test that stale fencing token causes task to exit early."""
    mocker.patch.object(BundleAnalysisNotifyTask, "app", celery_app)

    commit = CommitFactory.create()
    dbsession.add(commit)
    dbsession.flush()

    # Mock Redis to return stale token (current token > fencing_token)
    mock_redis.get.return_value = "2"  # Current token is 2, but we have token 1

    task = BundleAnalysisNotifyTask()
    task.request.retries = 0
    task.max_retries = 5

    # Mock debouncer's notification_lock to succeed (for the stale token check lock)
    @contextmanager
    def mock_lock_succeeds(*args, **kwargs):
        yield

    mocker.patch.object(
        task.debouncer, "notification_lock", side_effect=mock_lock_succeeds
    )

    # Mock check_fencing_token_stale to return True (token is stale)
    mocker.patch.object(task.debouncer, "check_fencing_token_stale", return_value=True)

    result = task.run_impl(
        dbsession,
        [{"error": None}],
        repoid=commit.repoid,
        commitid=commit.commitid,
        commit_yaml={},
        fencing_token=1,  # Pass fencing token, but it's stale
    )

    assert result == {
        "notify_attempted": False,
        "notify_succeeded": None,
    }


def test_bundle_analysis_notify_lock_retry_limit_exceeded_during_notification(
    mocker, dbsession, celery_app, mock_redis
):
    """Test that LockAcquisitionLimitError during notification lock returns graceful failure."""
    mocker.patch.object(BundleAnalysisNotifyTask, "app", celery_app)

    commit = CommitFactory.create()
    dbsession.add(commit)
    dbsession.flush()

    # Mock Redis to return non-stale token
    mock_redis.get.return_value = "1"  # Current token matches fencing_token

    task = BundleAnalysisNotifyTask()
    task.request.retries = 0
    task.max_retries = 5

    # Mock debouncer's notification_lock to raise LockAcquisitionLimitError during notification
    mocker.patch.object(
        task.debouncer,
        "notification_lock",
        side_effect=LockAcquisitionLimitError("Max retries exceeded"),
    )

    result = task.run_impl(
        dbsession,
        [{"error": None}],
        repoid=commit.repoid,
        commitid=commit.commitid,
        commit_yaml={},
        fencing_token=1,
    )

    assert result == {
        "notify_attempted": False,
        "notify_succeeded": None,
    }


def test_bundle_analysis_notify_lock_retry_during_notification(
    mocker, dbsession, celery_app, mock_redis
):
    """Test that LockRetry during notification lock triggers task retry."""
    mocker.patch.object(BundleAnalysisNotifyTask, "app", celery_app)

    commit = CommitFactory.create()
    dbsession.add(commit)
    dbsession.flush()

    # Mock Redis to return non-stale token
    mock_redis.get.return_value = "1"  # Current token matches fencing_token

    task = BundleAnalysisNotifyTask()
    task.request.retries = 0
    task.max_retries = 5
    task.request.headers = {}

    # Mock debouncer's notification_lock to raise LockRetry during notification
    @contextmanager
    def mock_lock_raises_retry(*args, **kwargs):
        raise LockRetry(60)

    mocker.patch.object(
        task.debouncer, "notification_lock", side_effect=mock_lock_raises_retry
    )

    # Mock retry to raise Retry exception
    mock_retry = mocker.patch.object(task, "retry", side_effect=Retry())

    with pytest.raises(Retry):
        task.run_impl(
            dbsession,
            [{"error": None}],
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml={},
            fencing_token=1,
        )

    # Verify retry was called
    assert mock_retry.called


def test_bundle_analysis_notify_lock_retry_max_retries_exceeded_during_notification(
    mocker, dbsession, celery_app, mock_redis
):
    """Test that when max retries exceeded during LockRetry handling in notification, returns graceful failure."""
    mocker.patch.object(BundleAnalysisNotifyTask, "app", celery_app)

    commit = CommitFactory.create()
    dbsession.add(commit)
    dbsession.flush()

    # Mock Redis to return non-stale token
    mock_redis.get.return_value = "1"  # Current token matches fencing_token

    task = BundleAnalysisNotifyTask()
    task.request.retries = 0
    task.max_retries = 5
    task.request.headers = {}

    # Mock debouncer's notification_lock to raise LockRetry during notification
    @contextmanager
    def mock_lock_raises_retry(*args, **kwargs):
        raise LockRetry(60)

    mocker.patch.object(
        task.debouncer, "notification_lock", side_effect=mock_lock_raises_retry
    )

    # Mock handle_lock_retry to return failure result (simulating max retries exceeded)
    mocker.patch.object(
        task.debouncer,
        "handle_lock_retry",
        return_value={
            "notify_attempted": False,
            "notify_succeeded": None,
        },
    )

    result = task.run_impl(
        dbsession,
        [{"error": None}],
        repoid=commit.repoid,
        commitid=commit.commitid,
        commit_yaml={},
        fencing_token=1,
    )

    assert result == {
        "notify_attempted": False,
        "notify_succeeded": None,
    }


def test_bundle_analysis_notify_commit_not_found(
    mocker, dbsession, celery_app, mock_redis
):
    """Test that missing commit raises assertion error."""
    mocker.patch.object(BundleAnalysisNotifyTask, "app", celery_app)

    # Create a commit but don't add it to session (or use non-existent commitid)
    commit = CommitFactory.build()  # build() doesn't add to session

    # Mock Redis to return non-stale token
    mock_redis.get.return_value = "1"

    # Mock LockManager to succeed
    lock_manager_mock = mocker.MagicMock()
    lock_manager_mock.return_value.locked.return_value.__enter__.return_value = None
    mocker.patch("tasks.bundle_analysis_notify.LockManager", lock_manager_mock)

    task = BundleAnalysisNotifyTask()
    task.request.retries = 0
    task.max_retries = 5

    with pytest.raises(AssertionError, match="commit not found"):
        task.process_impl_within_lock(
            db_session=dbsession,
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml={},
            previous_result=[{"error": None}],
        )


def test_bundle_analysis_notify_previous_result_not_list(
    mocker, dbsession, celery_app, mock_redis
):
    """Test that previous_result that's not a list is handled gracefully."""
    mocker.patch.object(BundleAnalysisNotifyTask, "app", celery_app)

    commit = CommitFactory.create()
    dbsession.add(commit)
    dbsession.flush()

    # Mock Redis to return non-stale token
    mock_redis.get.return_value = "1"

    # Mock LockManager to succeed
    lock_manager_mock = mocker.MagicMock()
    lock_manager_mock.return_value.locked.return_value.__enter__.return_value = None
    mocker.patch("tasks.bundle_analysis_notify.LockManager", lock_manager_mock)

    task = BundleAnalysisNotifyTask()
    task.request.retries = 0
    task.max_retries = 5

    # Pass previous_result as a dict instead of list
    # When not a list, it becomes empty list [], and all([]) is True,
    # so it returns ALL_ERRORED
    result = task.run_impl(
        dbsession,
        {"error": None},  # Not a list
        repoid=commit.repoid,
        commitid=commit.commitid,
        commit_yaml={},
        fencing_token=1,
    )

    # Should handle gracefully and treat as empty list, which means all errored
    assert result == {
        "notify_attempted": False,
        "notify_succeeded": NotificationSuccess.ALL_ERRORED,
    }


def test_bundle_analysis_notify_task_initialization(mocker):
    """Test that task initialization sets up debouncer correctly."""
    mocker.patch("tasks.bundle_analysis_notify.BaseCodecovTask.__init__")

    task = BundleAnalysisNotifyTask()

    assert task.debouncer is not None
    assert task.debouncer.redis_key_template == "bundle_analysis_notifier_fence:{}_{}"
    assert task.debouncer.debounce_period_seconds == 30
    assert task.debouncer.max_lock_retries == task.max_retries


def test_bundle_analysis_notify_partial_success(
    mocker, dbsession, celery_app, mock_redis
):
    """Test notification with partial success."""
    mocker.patch.object(BundleAnalysisNotifyTask, "app", celery_app)

    commit = CommitFactory.create()
    dbsession.add(commit)
    dbsession.flush()

    # Mock Redis to return non-stale token
    mock_redis.get.return_value = "1"

    # Mock LockManager to succeed
    lock_manager_mock = mocker.MagicMock()
    lock_manager_mock.return_value.locked.return_value.__enter__.return_value = None
    mocker.patch("tasks.bundle_analysis_notify.LockManager", lock_manager_mock)

    mocker.patch(
        "services.bundle_analysis.notify.BundleAnalysisNotifyService.notify",
        return_value=BundleAnalysisNotifyReturn(
            notifications_configured=(
                NotificationType.PR_COMMENT,
                NotificationType.COMMIT_STATUS,
            ),
            notifications_attempted=(
                NotificationType.PR_COMMENT,
                NotificationType.COMMIT_STATUS,
            ),
            notifications_successful=(
                NotificationType.PR_COMMENT,
            ),  # Only one succeeded
        ),
    )

    task = BundleAnalysisNotifyTask()
    task.request.retries = 0
    task.max_retries = 5

    result = task.run_impl(
        dbsession,
        [{"error": None}],
        repoid=commit.repoid,
        commitid=commit.commitid,
        commit_yaml={},
        fencing_token=1,
    )

    assert result == {
        "notify_attempted": True,
        "notify_succeeded": NotificationSuccess.PARTIAL_SUCCESS,
    }


def test_bundle_analysis_notify_no_notifications_configured(
    mocker, dbsession, celery_app, mock_redis
):
    """Test notification when no notifications are configured."""
    mocker.patch.object(BundleAnalysisNotifyTask, "app", celery_app)

    commit = CommitFactory.create()
    dbsession.add(commit)
    dbsession.flush()

    # Mock Redis to return non-stale token
    mock_redis.get.return_value = "1"

    # Mock LockManager to succeed
    lock_manager_mock = mocker.MagicMock()
    lock_manager_mock.return_value.locked.return_value.__enter__.return_value = None
    mocker.patch("tasks.bundle_analysis_notify.LockManager", lock_manager_mock)

    mocker.patch(
        "services.bundle_analysis.notify.BundleAnalysisNotifyService.notify",
        return_value=BundleAnalysisNotifyReturn(
            notifications_configured=(),  # No notifications configured
            notifications_attempted=(),
            notifications_successful=(),
        ),
    )

    task = BundleAnalysisNotifyTask()
    task.request.retries = 0
    task.max_retries = 5

    result = task.run_impl(
        dbsession,
        [{"error": None}],
        repoid=commit.repoid,
        commitid=commit.commitid,
        commit_yaml={},
        fencing_token=1,
    )

    assert result == {
        "notify_attempted": True,
        "notify_succeeded": NotificationSuccess.NOTHING_TO_NOTIFY,
    }
