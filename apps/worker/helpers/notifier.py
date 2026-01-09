import logging
from dataclasses import dataclass
from enum import Enum
from typing import Literal

from asgiref.sync import async_to_sync

from database.models import Commit
from database.models import Repository as SQLAlchemyRepository
from services.repository import (
    EnrichedPull,
    fetch_and_update_pull_request_information_from_commit,
    fetch_pull_request_information,
    get_repo_provider_service,
)
from services.yaml import UserYaml
from shared.django_apps.core.models import Repository
from shared.django_apps.test_analytics.models import TAPullComment
from shared.torngit.base import TorngitBaseAdapter
from shared.torngit.exceptions import TorngitClientError
from shared.torngit.response_types import ProviderPull
from shared.upload.types import TAUploadContext

log = logging.getLogger(__name__)


class NotifierResult(Enum):
    COMMENT_POSTED = "comment_posted"
    TORNGIT_ERROR = "torngit_error"
    NO_PULL = "no_pull"
    NO_COMMENT = "no_comment"


@dataclass
class BaseNotifier:
    """
    Base class for notifiers

    This class is responsible for building and sending notifications related to
    a specific commit.

    Note that `commit` supports both a Commit object and a TAUploadContext
    object so that it can be used in the new TA pipeline that is compliant with
    Sentry's retention policies. Depending on which is passed in, a slightly
    different code path is taken. Repository is also required as a parameter
    since it is not given with TAUploadContext like it is with Commit.
    """

    repo: Repository | SQLAlchemyRepository
    # TODO: Deprecate database-reliant code path after old TA pipeline is removed
    commit: Commit | TAUploadContext
    commit_yaml: UserYaml | None
    _pull: EnrichedPull | ProviderPull | None | Literal[False] = False
    _repo_service: TorngitBaseAdapter | None = None

    def get_pull(self, do_log=True) -> EnrichedPull | ProviderPull | None:
        repo_service = self.get_repo_service()

        if self._pull is False:
            if isinstance(self.commit, Commit):
                self._pull = async_to_sync(
                    fetch_and_update_pull_request_information_from_commit
                )(repo_service, self.commit, self.commit_yaml)
            else:
                self._pull = async_to_sync(fetch_pull_request_information)(
                    repo_service,
                    self.repo.repoid,
                    self.commit["commit_sha"],
                    self.commit["branch"],
                    self.commit["pull_id"],
                )

        if self._pull is None and do_log:
            log.info(
                "Not notifying since there is no pull request associated with this commit",
                extra={
                    "commitid": self.commit.commitid
                    if isinstance(self.commit, Commit)
                    else self.commit["commit_sha"],
                    "repoid": self.repo.repoid,
                },
            )

        return self._pull

    def get_repo_service(self) -> TorngitBaseAdapter:
        if self._repo_service is None:
            self._repo_service = get_repo_provider_service(self.repo)

        return self._repo_service

    def send_to_provider(self, pull: EnrichedPull | ProviderPull, message: str) -> bool:
        """
        Send a notification message to the appropriate provider for the given
        pull request.

        Note that `pull` can be either an `EnrichedPull` or a `ProviderPull`
        object. This allows for a Sentry-compliant code path that stores no
        user event data.
        """
        repo_service = self.get_repo_service()
        assert repo_service

        try:
            # TODO: Deprecate EnrichedPull-reliant code path after old TA pipeline is removed
            if isinstance(pull, EnrichedPull):
                pullid = pull.database_pull.pullid
                comment_id = pull.database_pull.commentid
            else:
                pullid = pull.id
                try:
                    comment_id = TAPullComment.objects.get(
                        repo_id=self.repo.repoid, pull_id=pullid
                    ).comment_id
                except TAPullComment.DoesNotExist:
                    comment_id = None

            if comment_id:
                async_to_sync(repo_service.edit_comment)(pullid, comment_id, message)
            else:
                res = async_to_sync(repo_service.post_comment)(pullid, message)

                if isinstance(pull, EnrichedPull):
                    pull.database_pull.commentid = res["id"]
                else:
                    TAPullComment.objects.create(
                        repo_id=self.repo.repoid,
                        pull_id=pullid,
                        comment_id=res["id"],
                    )
            return True

        except TorngitClientError as e:
            log.error(
                "Error creating/updating PR comment",
                extra={
                    "commitid": self.commit.commitid
                    if isinstance(self.commit, Commit)
                    else self.commit["commit_sha"],
                    "pullid": pullid,
                    "error_code": getattr(e, "code", None),
                    "error_message": str(e),
                },
            )
            return False

    def build_message(self) -> str:
        raise NotImplementedError

    def notify(self) -> NotifierResult:
        pull = self.get_pull()
        if pull is None:
            return NotifierResult.NO_PULL

        message = self.build_message()

        sent_to_provider = self.send_to_provider(pull, message)
        if sent_to_provider == False:
            return NotifierResult.TORNGIT_ERROR

        return NotifierResult.COMMENT_POSTED
