import logging

from sqlalchemy.orm.session import Session

from database.enums import NotificationState
from database.models import CommitNotification, Pull
from database.models.core import GithubAppInstallation
from services.comparison import ComparisonProxy
from services.notification.notifiers.base import (
    AbstractBaseNotifier,
    NotificationResult,
)

log = logging.getLogger(__name__)


def _lock_github_app_installation(db_session: Session, ghapp_id: int) -> bool:
    """Returns whether the GithubAppInstallation exists, holding a FOR KEY SHARE lock on it.

    The lock prevents the row from being deleted concurrently until the current
    transaction ends, so the (deferred) FK from commit_notifications stays valid.
    """
    installation_id = (
        db_session.query(GithubAppInstallation.id_)
        .filter(GithubAppInstallation.id_ == ghapp_id)
        .with_for_update(key_share=True)
        .scalar()
    )
    return installation_id is not None


def create_or_update_commit_notification_from_notification_result(
    comparison: ComparisonProxy,
    notifier: AbstractBaseNotifier,
    notification_result: NotificationResult | None,
) -> CommitNotification | None:
    """Saves a CommitNotification entry in the database.
    We save an entry in the following scenarios:
        - We save all notification attempts for commits that are part of a PullRequest
        - We save _successful_ notification attempt _with_ a github app
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

    commit = pull.get_head_commit() if pull else comparison.head.commit
    if not commit:
        log.warning("Head commit not found for pull", extra={"pull": pull})
        return None

    db_session: Session = commit.get_db_session()

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
        if github_app_used is not None and not _lock_github_app_installation(
            db_session, github_app_used
        ):
            # The installation was deleted (e.g. uninstall webhook or invalid
            # installation cleanup) while we were notifying. Saving the stale id
            # would violate the FK at commit time and roll back the whole task.
            log.warning(
                "GitHub app installation used for notification no longer exists",
                extra={
                    "ghapp_id": github_app_used,
                    "commit_id": commit.id_,
                    "commitid": commit.commitid,
                    "notification_type": notifier.notification_type,
                },
            )
            github_app_used = None
        commit_notification = CommitNotification(
            commit_id=commit.id_,
            notification_type=notifier.notification_type,
            decoration_type=notifier.decoration_type,
            gh_app_id=github_app_used,
            state=notification_state,
        )
        db_session.add(commit_notification)
        db_session.flush()
        return commit_notification

    commit_notification.decoration_type = notifier.decoration_type
    commit_notification.state = notification_state
    return commit_notification
