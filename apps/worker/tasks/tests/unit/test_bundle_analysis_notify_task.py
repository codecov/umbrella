import logging

import pytest
from celery.exceptions import MaxRetriesExceededError, Retry
from redis.exceptions import LockError

from database.tests.factories import CommitFactory
from services.bundle_analysis.notify import BundleAnalysisNotifyReturn
from services.bundle_analysis.notify.types import NotificationSuccess, NotificationType
from shared.celery_config import BUNDLE_ANALYSIS_NOTIFY_MAX_RETRIES
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


def test_bundle_analysis_notify_task_max_retries_exceeded(
    caplog,
    celery_app,
    dbsession,
    mocker,
    mock_redis,
):
    """Test that bundle analysis notify does not retry infinitely when max retries exceeded"""
    mocker.patch.object(BundleAnalysisNotifyTask, "app", celery_app)

    commit = CommitFactory.create()
    dbsession.add(commit)
    dbsession.flush()

    # Mock Redis to simulate lock failure - this will cause the real LockManager to raise LockRetry
    mock_redis.lock.return_value.__enter__.side_effect = LockError()

    task = BundleAnalysisNotifyTask()
    # Set retries to max_retries to simulate max retries exceeded scenario
    task.request.retries = BUNDLE_ANALYSIS_NOTIFY_MAX_RETRIES

    # Task should raise MaxRetriesExceededError (from self.retry()) instead of retrying infinitely
    with caplog.at_level(logging.ERROR):
        with pytest.raises(MaxRetriesExceededError):
            task.run_impl(
                dbsession,
                [{"error": None}],
                repoid=commit.repoid,
                commitid=commit.commitid,
                commit_yaml={},
            )

    error_logs = [
        r
        for r in caplog.records
        if r.levelname == "ERROR"
        and "Task" in r.message
        and "exceeded max retries" in r.message
    ]
    assert len(error_logs) == 1
    assert error_logs[0].__dict__["max_retries"] == BUNDLE_ANALYSIS_NOTIFY_MAX_RETRIES
    assert (
        error_logs[0].__dict__["current_retries"] == BUNDLE_ANALYSIS_NOTIFY_MAX_RETRIES
    )


def test_bundle_analysis_notify_task_max_retries_not_exceeded(
    celery_app,
    dbsession,
    mocker,
    mock_redis,
):
    """Test that bundle analysis notify retries when retries are available"""
    mocker.patch.object(BundleAnalysisNotifyTask, "app", celery_app)

    commit = CommitFactory.create()
    dbsession.add(commit)
    dbsession.flush()

    mock_redis.lock.return_value.__enter__.side_effect = LockError()

    task = BundleAnalysisNotifyTask()
    task.request.retries = 0

    with pytest.raises(Retry):
        task.run_impl(
            dbsession,
            [{"error": None}],
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml={},
        )
