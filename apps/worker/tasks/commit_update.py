import datetime as dt
import logging

from app import celery_app
from database.models import Branch, Commit, Pull
from helpers.exceptions import RepositoryWithoutValidBotError
from helpers.github_installation import get_installation_name_for_owner_for_task
from services.repository import (
    get_repo_provider_service,
    possibly_update_commit_from_provider_info,
)
from shared.celery_config import commit_update_task_name, upload_breadcrumb_task_name
from shared.django_apps.upload_breadcrumbs.models import (
    BreadcrumbData,
    Errors,
    Milestones,
)
from shared.torngit.exceptions import TorngitClientError, TorngitRepoNotFoundError
from shared.utils.sentry import current_sentry_trace_id
from tasks.base import BaseCodecovTask

log = logging.getLogger(__name__)


class CommitUpdateTask(BaseCodecovTask, name=commit_update_task_name):
    def run_impl(
        self,
        db_session,
        repoid: int,
        commitid: str,
        **kwargs,
    ):
        commit = None
        commits = db_session.query(Commit).filter(
            Commit.repoid == repoid, Commit.commitid == commitid
        )
        commit = commits.first()
        assert commit, "Commit not found in database."
        repository = commit.repository
        repository_service = None
        was_updated = False
        error = None
        try:
            installation_name_to_use = get_installation_name_for_owner_for_task(
                self.name, repository.owner
            )
            repository_service = get_repo_provider_service(
                repository, installation_name_to_use=installation_name_to_use
            )
            was_updated = possibly_update_commit_from_provider_info(
                commit, repository_service
            )

            if isinstance(commit.timestamp, str):
                commit.timestamp = dt.datetime.fromisoformat(commit.timestamp).replace(
                    tzinfo=None
                )

            if commit.pullid is not None:
                # upsert pull
                pull = (
                    db_session.query(Pull)
                    .filter(Pull.repoid == repoid, Pull.pullid == commit.pullid)
                    .first()
                )

                if pull is None:
                    pull = Pull(
                        repoid=repoid,
                        pullid=commit.pullid,
                        author_id=commit.author_id,
                        head=commit.commitid,
                    )
                    db_session.add(pull)
                else:
                    previous_pull_head = (
                        db_session.query(Commit)
                        .filter(Commit.repoid == repoid, Commit.commitid == pull.head)
                        .first()
                    )
                    if (
                        previous_pull_head is None
                        or previous_pull_head.deleted == True
                        or previous_pull_head.timestamp < commit.timestamp
                    ):
                        pull.head = commit.commitid

                db_session.flush()

            if commit.branch is not None:
                # upsert branch
                branch = (
                    db_session.query(Branch)
                    .filter(Branch.repoid == repoid, Branch.branch == commit.branch)
                    .first()
                )

                if branch is None:
                    branch = Branch(
                        repoid=repoid,
                        branch=commit.branch,
                        head=commit.commitid,
                        authors=[commit.author_id],
                    )
                    db_session.add(branch)
                else:
                    if commit.author_id is not None:
                        if branch.authors is None:
                            branch.authors = [commit.author_id]
                        elif commit.author_id not in branch.authors:
                            branch.authors.append(commit.author_id)

                    previous_branch_head = (
                        db_session.query(Commit)
                        .filter(Commit.repoid == repoid, Commit.commitid == branch.head)
                        .first()
                    )

                    if (
                        previous_branch_head is None
                        or previous_branch_head.deleted == True
                        or previous_branch_head.timestamp < commit.timestamp
                    ):
                        branch.head = commit.commitid

                db_session.flush()

        except RepositoryWithoutValidBotError:
            log.warning(
                "Unable to reach git provider because repo doesn't have a valid bot",
                extra={"repoid": repoid, "commit": commitid},
            )
            error = Errors.REPO_MISSING_VALID_BOT
        except TorngitRepoNotFoundError:
            log.warning(
                "Unable to reach git provider because this specific bot/integration can't see that repository",
                extra={"repoid": repoid, "commit": commitid},
            )
            error = Errors.REPO_NOT_FOUND
        except TorngitClientError:
            log.warning(
                "Unable to reach git provider because there was a 4xx error",
                extra={"repoid": repoid, "commit": commitid},
                exc_info=True,
            )
            error = Errors.GIT_CLIENT_ERROR
        if was_updated:
            log.info(
                "Commit updated successfully",
                extra={"commitid": commitid, "repoid": repoid},
            )

        self.app.tasks[upload_breadcrumb_task_name].apply_async(
            kwargs={
                "commit_sha": commitid,
                "repo_id": repoid,
                "breadcrumb_data": BreadcrumbData(
                    milestone=Milestones.COMMIT_PROCESSED,
                    error=error,
                ),
                "sentry_trace_id": current_sentry_trace_id(),
            }
        )
        return {"was_updated": was_updated}


RegisteredCommitUpdateTask = celery_app.register_task(CommitUpdateTask())
commit_update_task = celery_app.tasks[RegisteredCommitUpdateTask.name]
