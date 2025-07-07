import logging

from asgiref.sync import async_to_sync

from database.models import Commit
from helpers.notifier import NotifierResult
from services.repository import (
    fetch_and_update_pull_request_information_from_commit,
    get_repo_provider_service,
)
from shared.torngit.exceptions import TorngitClientError

log = logging.getLogger(__name__)


def post_comment(commit: Commit, message: str) -> NotifierResult:
    repo_service = get_repo_provider_service(commit.repository)

    pull = async_to_sync(fetch_and_update_pull_request_information_from_commit)(
        repo_service, commit, None
    )

    if pull is None:
        log.info(
            "Not notifying since there is no pull request associated with this commit",
            extra={"commitid": commit.commitid},
        )
        return NotifierResult.NO_PULL

    pullid = pull.database_pull.pullid
    try:
        comment_id = pull.database_pull.commentid
        if comment_id:
            async_to_sync(repo_service.edit_comment)(pullid, comment_id, message)
        else:
            res = async_to_sync(repo_service.post_comment)(pullid, message)
            pull.database_pull.commentid = res["id"]
        return NotifierResult.COMMENT_POSTED
    except TorngitClientError:
        log.error(
            "Error creating/updating PR comment",
            extra={
                "commitid": commit.commitid,
                "pullid": pullid,
            },
        )
        return NotifierResult.TORNGIT_ERROR
