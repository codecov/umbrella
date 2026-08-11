import logging

from sqlalchemy.orm.session import Session

from database.enums import NotificationState
from database.models import CommitNotification, Pull
from services.comparison import ComparisonProxy
from services.notification.notifiers.base import (
    AbstractBaseNotifier,
    NotificationResult,
)

log = logging.getLogger(__name__)


def fetch_commit_notifications_for_comparison(
    comparison: ComparisonProxy,
) -> dict | None:
    """Pre-fetches all CommitNotification rows for the head commit.

    Returns a dict keyed by notification_type, or None if no commit is available.
    This is intended to be called once before iterating over notifiers so that
    individual notifier calls can do a dict lookup instead of a per-call DB query.
    """
    commit = comparison.head.commit if comparison.head else None
    if not commit:
        return None

    db_session: Session = commit.get_db_session()
    rows = (
        db_session.query(CommitNotification)
        .filter(CommitNotification.commit_id == commit.id_)
        .all()
    )
    return {row.notification_type: row for row in rows}


def create_or_update_commit_notification_from_notification_result(
    comparison: ComparisonProxy,
    notifier: AbstractBaseNotifier,
    notification_result: NotificationResult | None,
    preloaded_commit_notifications: dict | None = None,
) -> CommitNotification | None:
    """Saves a CommitNotification entry in the database.
    We save an entry in the following scenarios:
        - We save all notification attempts for commits that are part of a PullRequest
        - We save _successful_ notification attempt _with_ a github app

    `preloaded_commit_notifications` is an optional dict (keyed by notification_type)
    returned by `fetch_commit_notifications_for_comparison`. When provided, it avoids
    an extra per-notifier SELECT query against commit_notifications.
    """
    pull: Pull | None = comparison.pull
    not_pull = pull is None
    not_head_commit = comparison.head is None or comparison.head.commit is None
    not_github_app_info = (
        notification_result is None or notification_result.github_app_used is None
    )
    failed = (
        notification_result is None
        or notification_result.notification_successful == False
    )
    if not_pull and (not_head_commit or not_github_app_info or failed):
        return None

    # Use the already-loaded commit from the comparison rather than re-querying
    # via pull.get_head_commit(), which would issue an extra SELECT per notifier.
    commit = comparison.head.commit if comparison.head else None
    if not commit:
        log.warning("Head commit not found for pull", extra={"pull": pull})
        return None

    db_session: Session = commit.get_db_session()

    if preloaded_commit_notifications is not None:
        commit_notification = preloaded_commit_notifications.get(
            notifier.notification_type
        )
    else:
        commit_notification = (
            db_session.query(CommitNotification)
            .filter(
                CommitNotification.commit_id == commit.id_,
                CommitNotification.notification_type == notifier.notification_type,
            )
            .first()
        )

    notification_state = (
        NotificationState.error if failed else NotificationState.success
    )
    github_app_used = (
        notification_result.github_app_used if notification_result else None
    )

    if not commit_notification:
        commit_notification = CommitNotification(
            commit_id=commit.id_,
            notification_type=notifier.notification_type,
            decoration_type=notifier.decoration_type,
            gh_app_id=github_app_used,
            state=notification_state,
        )
        db_session.add(commit_notification)
        db_session.flush()
        # Add to the preloaded dict so subsequent notifiers with the same type
        # see this newly-inserted row.
        if preloaded_commit_notifications is not None:
            preloaded_commit_notifications[notifier.notification_type] = (
                commit_notification
            )
        return commit_notification

    commit_notification.decoration_type = notifier.decoration_type
    commit_notification.state = notification_state
    return commit_notification
