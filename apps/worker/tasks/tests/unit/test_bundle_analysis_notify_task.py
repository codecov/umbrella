from contextlib import contextmanager

import pytest

from database.tests.factories import CommitFactory
from services.bundle_analysis.notify import BundleAnalysisNotifyReturn
from services.bundle_analysis.notify.types import (
    NotificationSuccess,
    NotificationType,
)
from services.lock_manager import LockRetry
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
    )
    assert result == {
        "notify_attempted": False,
        "notify_succeeded": NotificationSuccess.ALL_ERRORED,
    }


@pytest.mark.parametrize(
    "lock_countdown, expected_countdown",
    [
        # Below the cap: pass through unchanged.
        (60, 60),
        (300, 300),
        (869, 869),
        # At and above the cap: pinned at 870.
        (870, 870),
        (900, 870),
        (1800, 870),
        # LockManager.MAX_RETRY_COUNTDOWN_SECONDS upper bound (5 hours).
        (18000, 870),
    ],
)
def test_bundle_analysis_notify_task_lock_retry_countdown_capped(
    mocker,
    dbsession,
    lock_countdown,
    expected_countdown,
):
    """The LockRetry retry path must cap its countdown below the 900s Redis
    visibility timeout so the broker never redelivers the still-unacked ETA
    message to additional workers (which would amplify notify-side tasks the
    same way file_not_in_storage amplified processor-side tasks)."""
    commit = CommitFactory.create()
    dbsession.add(commit)
    dbsession.flush()

    @contextmanager
    def _raise_lock_retry(*args, **kwargs):
        raise LockRetry(
            countdown=lock_countdown,
            max_retries_exceeded=False,
            retry_num=1,
            max_retries=10,
            lock_name="bundle_analysis_notify",
            repoid=commit.repoid,
            commitid=commit.commitid,
        )
        yield  # pragma: no cover

    mocker.patch(
        "tasks.bundle_analysis_notify.get_bundle_analysis_lock_manager",
        return_value=mocker.MagicMock(locked=_raise_lock_retry),
    )

    task = BundleAnalysisNotifyTask()
    retry_call = mocker.patch.object(task, "retry")

    task.run_impl(
        dbsession,
        [{"error": None}],
        repoid=commit.repoid,
        commitid=commit.commitid,
        commit_yaml={},
    )

    retry_call.assert_called_once()
    assert retry_call.call_args.kwargs["countdown"] == expected_countdown
    assert retry_call.call_args.kwargs["countdown"] < 900
