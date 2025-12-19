import pytest
from celery.exceptions import Retry

from database.tests.factories import CommitFactory
from services.bundle_analysis.notify import BundleAnalysisNotifyReturn
from services.bundle_analysis.notify.types import (
    NotificationSuccess,
    NotificationType,
)
from services.notification.debounce import SKIP_DEBOUNCE_TOKEN
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
