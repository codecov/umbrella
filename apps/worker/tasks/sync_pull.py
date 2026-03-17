import json
import logging
import os
import time
from collections import deque
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from typing import Any

import sentry_sdk
import sqlalchemy.orm
from asgiref.sync import async_to_sync
from redis.exceptions import LockError

from app import celery_app
from database.models import Commit, Pull, Repository
from helpers.exceptions import NoConfiguredAppsAvailable, RepositoryWithoutValidBotError
from helpers.github_installation import get_installation_name_for_owner_for_task
from helpers.metrics import metrics
from services.comparison.changes import get_changes
from services.report import Report, ReportService
from services.repository import (
    EnrichedPull,
    fetch_and_update_pull_request_information,
    get_repo_provider_service,
)
from services.test_analytics.ta_process_flakes import KEY_NAME
from services.test_results import should_do_flaky_detection
from services.yaml.reader import read_yaml_field
from shared.celery_config import (
    ai_pr_review_task_name,
    notify_task_name,
    pulls_task_name,
)
from shared.helpers.redis import get_redis_connection
from shared.reports.types import Change
from shared.torngit.base import TorngitBaseAdapter
from shared.torngit.exceptions import TorngitClientError
from shared.yaml import UserYaml
from shared.yaml.user_yaml import OwnerContext
from tasks.base import BaseCodecovTask
from tasks.process_flakes import process_flakes_task_name

log = logging.getLogger(__name__)


class PullSyncTask(BaseCodecovTask, name=pulls_task_name):
    """
        This is the task that syncs pull with the information the Git Provider gives us

    The most characteristic piece of this task is that it centers around the PR.
        We receive a (repoid, pullid) pair. And we fetch all the information
            from the git provider to update it as needed.

        This mostly includes
            - Updating basic database fields around the PR, like author
            - Updating `base` and `head` of the PR
            - Updating the `diff` and `flare` information of the PR using the `report` of its head
            - Updating all the commits that point to this pull in case the pull is being merged
            - Clear the caches we have around this PR

        At the end we call the notify task to do notifications with the new information we have
    """

    def run_impl(
        self,
        db_session: sqlalchemy.orm.Session,
        *,
        repoid: int = None,
        pullid: int = None,
        should_send_notifications: bool = True,
        **kwargs,
    ):
        redis_connection = get_redis_connection()
        pullid = int(pullid)
        repoid = int(repoid)
        lock_name = f"pullsync_{repoid}_{pullid}"
        start_wait = time.monotonic()
        try:
            with self.with_logged_lock(
                redis_connection.lock(
                    lock_name,
                    timeout=60 * 5,
                    blocking_timeout=0.5,
                ),
                lock_name=lock_name,
                repoid=repoid,
                pullid=pullid,
            ):
                return self.run_impl_within_lock(
                    db_session,
                    redis_connection,
                    repoid=repoid,
                    pullid=pullid,
                    should_send_notifications=should_send_notifications,
                    **kwargs,
                )
        except LockError:
            log.info(
                "Unable to acquire PullSync lock. Not retrying because pull is being synced already",
                extra={"pullid": pullid, "repoid": repoid},
            )
            return {
                "notifier_called": False,
                "commit_updates_done": {"merged_count": 0, "soft_deleted_count": 0},
                "pull_updated": False,
                "reason": "unable_fetch_lock",
            }

    def run_impl_within_lock(
        self,
        db_session: sqlalchemy.orm.Session,
        redis_connection,
        *,
        repoid: int = None,
        pullid: int = None,
        should_send_notifications: bool = True,
        **kwargs,
    ):
        commit_updates_done = {"merged_count": 0, "soft_deleted_count": 0}
        repository = db_session.query(Repository).filter_by(repoid=repoid).first()
        assert repository
        extra_info = {"pullid": pullid, "repoid": repoid}
        try:
            installation_name_to_use = get_installation_name_for_owner_for_task(
                self.name, repository.author
            )
            repository_service = get_repo_provider_service(
                repository, installation_name_to_use=installation_name_to_use
            )
        except RepositoryWithoutValidBotError:
            log.warning(
                "Could not sync pull because there is no valid bot found for that repo",
                extra=extra_info,
                exc_info=True,
            )
            return {
                "notifier_called": False,
                "commit_updates_done": {"merged_count": 0, "soft_deleted_count": 0},
                "pull_updated": False,
                "reason": "no_bot",
            }
        except NoConfiguredAppsAvailable as err:
            log.error(
                "Could not sync pull because there are no configured apps available",
                extra={
                    **extra_info,
                    "suspended_app_count": err.suspended_count,
                    "rate_limited_count": err.rate_limited_count,
                },
            )
            if err.rate_limited_count > 0:
                log.info("Apps are rate limited. Retrying in 60s", extra=extra_info)
                self.retry(max_retries=1, countdown=60)
            return {
                "notifier_called": False,
                "commit_updates_done": {"merged_count": 0, "soft_deleted_count": 0},
                "pull_updated": False,
                "reason": "no_configured_apps_available",
            }
        context = OwnerContext(
            owner_onboarding_date=repository.author.createstamp,
            owner_plan=repository.author.plan,
            ownerid=repository.ownerid,
        )
        current_yaml = UserYaml.get_final_yaml(
            owner_yaml=repository.author.yaml,
            repo_yaml=repository.yaml,
            owner_context=context,
        )
        with metrics.timer(f"{self.metrics_prefix}.fetch_pull"):
            enriched_pull = async_to_sync(fetch_and_update_pull_request_information)(
                repository_service, db_session, repoid, pullid, current_yaml
            )
        pull = enriched_pull.database_pull
        if pull is None:
            log.info(
                "Not syncing pull since we can't find it in the database nor in the provider",
                extra=extra_info,
            )
            return {
                "notifier_called": False,
                "commit_updates_done": {"merged_count": 0, "soft_deleted_count": 0},
                "pull_updated": False,
                "reason": "no_db_pull",
            }
        if enriched_pull.provider_pull is None:
            log.info(
                "Not syncing pull since we can't find it in the provider. There is nothing to sync",
                extra=extra_info,
            )
            return {
                "notifier_called": False,
                "commit_updates_done": {"merged_count": 0, "soft_deleted_count": 0},
                "pull_updated": False,
                "reason": "not_in_provider",
            }
        self.trigger_ai_pr_review(enriched_pull, current_yaml)

        # Check for related commits to improve PR context
        self.analyze_related_commits_for_context(db_session, pull, current_yaml)

        report_service = ReportService(
            current_yaml, gh_app_installation_name=installation_name_to_use
        )
        head_commit = pull.get_head_commit()
        if head_commit is None:
            log.info(
                "Not syncing pull since there is no head in our database",
                extra={"pullid": pullid, "repoid": repoid},
            )
            return {
                "notifier_called": False,
                "commit_updates_done": {"merged_count": 0, "soft_deleted_count": 0},
                "pull_updated": False,
                "reason": "no_head",
            }
        compared_to = pull.get_comparedto_commit()
        head_report = report_service.get_existing_report_for_commit(head_commit)
        if compared_to is not None:
            base_report = report_service.get_existing_report_for_commit(compared_to)
        else:
            base_report = None
        commits = None
        db_session.commit()
        try:
            commits = async_to_sync(repository_service.get_pull_request_commits)(
                pull.pullid
            )
            base_ancestors_tree = async_to_sync(repository_service.get_ancestors_tree)(
                enriched_pull.provider_pull["base"]["branch"]
            )
            commit_updates_done = self.update_pull_commits(
                repository_service,
                enriched_pull,
                commits,
                base_ancestors_tree,
                current_yaml,
                repository,
            )
            db_session.commit()
        except TorngitClientError:
            log.warning(
                "Unable to fetch information about pull commits",
                extra={"pullid": pullid, "repoid": repoid},
            )
        self.update_pull_from_reports(
            pull, repository_service, base_report, head_report, current_yaml
        )
        db_session.commit()
        notifier_was_called = False
        if should_send_notifications:
            notifier_was_called = True
            self.app.tasks[notify_task_name].apply_async(
                kwargs={"repoid": repoid, "commitid": pull.head}
            )
        self.clear_pull_related_caches(redis_connection, enriched_pull)
        return {
            "notifier_called": notifier_was_called,
            "commit_updates_done": commit_updates_done,
            "pull_updated": True,
            "reason": "success",
        }

    def cache_changes(self, pull: Pull, changes: list[Change]):
        """
        Caches the list of files with changes for a given comparison.
        This information will be used API-side to speed up responses.
        """
        owners_with_cached_changes = [
            int(ownerid.strip())
            for ownerid in os.getenv("OWNERS_WITH_CACHED_CHANGES", "").split(",")
            if ownerid != ""
        ]
        if pull.repository.author.ownerid in owners_with_cached_changes:
            log.info(
                "Caching files with changes",
                extra={"pullid": pull.pullid, "repoid": pull.repoid},
            )
            redis = get_redis_connection()
            key = "/".join(
                (
                    "compare-changed-files",
                    pull.repository.author.service,
                    pull.repository.author.username,
                    pull.repository.name,
                    f"{pull.pullid}",
                )
            )
            redis.set(
                key,
                json.dumps([change.path for change in changes]),
                ex=86400,  # 1 day in seconds
            )
            log.info(
                "Finished caching files with changes",
                extra={"pullid": pull.pullid, "repoid": pull.repoid},
            )

    def update_pull_from_reports(
        self,
        pull: Pull,
        repository_service,
        base_report: Report,
        head_report: Report,
        current_yaml,
    ):
        try:
            compare_dict = async_to_sync(repository_service.get_compare)(
                pull.base, pull.head, with_commits=False
            )
            diff = compare_dict["diff"]
            changes = get_changes(base_report, head_report, diff)
            if changes:
                self.cache_changes(pull, changes)
            if head_report:
                color = read_yaml_field(current_yaml, ("coverage", "range"))
                pull.diff = head_report.apply_diff(diff)
                pull.flare = (
                    head_report.flare(changes, color=color) if head_report else None
                )
            return True
        except TorngitClientError:
            log.warning(
                "Unable to fetch information about diff",
                extra={"pullid": pull.pullid, "repoid": pull.repoid},
            )
            return False

    def clear_pull_related_caches(self, redis_connection, enriched_pull: EnrichedPull):
        pull = enriched_pull.database_pull
        pull_dict = enriched_pull.provider_pull
        repository = pull.repository
        key = ":".join(
            (repository.service, repository.author.username, repository.name)
        )
        if pull.state == "merged":
            base_branch = pull_dict["base"]["branch"]
            if base_branch:
                redis_connection.hdel("badge", (f"{key}:{base_branch}").lower())
                if base_branch == repository.branch:
                    redis_connection.hdel("badge", (f"{key}:").lower())

    def update_pull_commits(
        self,
        repository_service,
        enriched_pull: EnrichedPull,
        commits_on_pr: Sequence,
        ancestors_tree_on_base: dict[str, Any],
        current_yaml,
        repository: Repository,
    ) -> dict:
        """Updates commits considering what the new PR situation is.

            For example, if a pull is merged, it makes sense that their commits switch to
                `merged` mode and start being part of the `base` branch.

            On squash merge we have a bit of an issue, since we can't move the commits to the
                new branch (they didn't go there), but they don't go deleted. So they just hang
                there for a while. Theoretically a branch deletion should trigger something
                and the system will deal with those commits, but I dont think this is implemented
                yet

        Args:
            enriched_pull (EnrichedPull): The pull that changed state
            commits_on_pr (Sequence): The commits that were on the PR we might want to update
            ancestors_tree_on_base (Dict[str, Any]): Ancestor tree of commits starting at the base

        Returns:
            dict: A dict of the changes that were made
        """
        pull = enriched_pull.database_pull
        pull_dict = enriched_pull.provider_pull
        repoid = pull.repoid
        pullid = pull.pullid
        db_session = pull.get_db_session()
        merged_count, deleted_count = 0, 0
        if commits_on_pr:
            if pull.state == "merged":
                is_squash_merge = self.was_pr_merged_with_squash(
                    pullid,
                    repository_service,
                    pull_dict,
                    commits_on_pr,
                    ancestors_tree_on_base,
                )
                if not is_squash_merge:
                    log.info(
                        "Moving commits to base branch",
                        extra={
                            "commits_on_pr": commits_on_pr,
                            "repoid": repoid,
                            "pullid": pullid,
                            "new_branch": pull_dict["base"]["branch"],
                        },
                    )
                    merged_count = (
                        db_session.query(Commit)
                        .filter(
                            Commit.repoid == repoid,
                            Commit.pullid == pullid,
                            Commit.commitid.in_(commits_on_pr + [pull.base, pull.head]),
                            ~Commit.merged,
                        )
                        .update(
                            {
                                Commit.branch: pull_dict["base"]["branch"],
                                Commit.updatestamp: datetime.now(),
                                Commit.merged: True,
                                Commit.deleted: False,
                            },
                            synchronize_session=False,
                        )
                    )

                    self.trigger_process_flakes(
                        db_session,
                        repository,
                        pull.head,
                        current_yaml,
                    )

            # set the rest of the commits to deleted (do not show in the UI)
            deleted_count = (
                db_session.query(Commit)
                .filter(
                    Commit.repoid == repoid,
                    Commit.pullid == pullid,
                    ~Commit.commitid.in_(commits_on_pr + [pull.base, pull.head]),
                )
                .update({Commit.deleted: True}, synchronize_session=False)
            )
        return {"soft_deleted_count": deleted_count, "merged_count": merged_count}

    def was_squash_via_merge_commit(
        self, repository_service: TorngitBaseAdapter, pull_dict: dict[str, Any]
    ):
        """
        the goal of this function is to determine whether we should update the branch and
        merged of the commits that are on the PR.

        we shouldn't do this on squash merge because in that case the commits never made it onto
        the base branch of the PR, but on rebase or normal merge, we should

        since it's not easy to tell whether a PR was a squash merge since the commit on the base branch
        might be completely unrelated, we'll check the merge commit for 2 things:
        - if it has more than a single parent, it's a merge commit
        - if it's equal to the head of the PR, it's a rebase commit

        so if we rule those 2 scenarios out, it's a squash commit

        NOTE: we don't support checking for rebase merges but anyways if it's a rebase merge and the
        merge commit != head commit, or we can't find the merge commit, we should assume that it's the
        same as if we had a squash commit
        """

        merge_commit_sha = pull_dict.get("merge_commit_sha")

        if merge_commit_sha is None:
            return False

        merge_commit = async_to_sync(repository_service.get_commit)(merge_commit_sha)
        is_merge = len(merge_commit["parents"]) > 1
        is_rebase = merge_commit_sha == pull_dict["head"]["commitid"]
        return not is_merge and not is_rebase

    def was_squash_via_ancestor_tree(self, commits_on_pr, base_ancestors_tree):
        """
            Determines if commit was merged with squash merge or not, by looking at the commits
                that were on the commit and the commits that are on the base branch now

            The logic here is that, if at least one commit of the PR made it into the base branch,
                then we know no ways of merging just that one commit without the other ones. So it
                is probable that the commit was merged with merge-commit (or some other strategy
                that moves all commits to the base branch).

            Notice that the reason we don't require all commits to be on the base branch now is
                because it's possible that the base branch moves so fast that by the time
                we check it part of the PR commits could have been merged via merge-commit, but
                are already out of the first page of commit listing.

            This also brings us the the limitation of this logic: if the base branch moves so fast
                that ALL the commits are out of the first page of listing when we check, we
                will assume (wrongly) that this was a squash commit

        Args:
            commits_on_pr (Sequence[str]): Description
            base_ancestors_tree (Dict[str, Any]): Description
        """
        commits_on_pr_set = set(commits_on_pr)
        current_level = deque([base_ancestors_tree])
        while current_level:
            el = current_level.popleft()
            if el["commitid"] in commits_on_pr_set:
                log.info(
                    "Commit currently on base tree was also on PR. Calling it a normal merge",
                    extra={
                        "commits_on_pr": commits_on_pr,
                        "commit_on_base": el["commitid"],
                        "base_head": base_ancestors_tree["commitid"],
                    },
                )
                return False
            for p in el.get("parents", []):
                current_level.append(p)
        log.info(
            "Commits from PR not found on base tree. Calling it a squash merge",
            extra={
                "commits_on_pr": commits_on_pr,
                "base_head": base_ancestors_tree["commitid"],
            },
        )
        return True

    def was_pr_merged_with_squash(
        self,
        pullid: int,
        repository_service,
        pull_dict: Mapping[str, Any],
        commits_on_pr: Sequence[str],
        base_ancestors_tree: dict[str, Any],
    ) -> bool:
        try:
            experiment_was_squash = self.was_squash_via_merge_commit(
                repository_service, pull_dict
            )
        except Exception as e:
            log.warning(
                "was_pr_merged_with_squash: error running merge commit sha check",
                extra={
                    "pullid": pullid,
                    "pull_dict": pull_dict,
                    "merge_commit_sha": pull_dict.get("merge_commit_sha"),
                },
                exc_info=e,
            )
            sentry_sdk.capture_exception(e)
            experiment_was_squash = None

        regular_was_squash = self.was_squash_via_ancestor_tree(
            commits_on_pr, base_ancestors_tree
        )

        if experiment_was_squash != regular_was_squash:
            log.info(
                "was_pr_merged_with_squash: merge commit sha check failed",
                extra={
                    "pullid": pullid,
                    "pull_dict": pull_dict,
                    "merge_commit_sha": pull_dict.get("merge_commit_sha"),
                    "regular_was_squash": regular_was_squash,
                    "experiment_was_squash": experiment_was_squash,
                    "success": experiment_was_squash == regular_was_squash,
                },
            )

        return regular_was_squash

    def trigger_process_flakes(
        self,
        db_session: sqlalchemy.orm.Session,
        repository: Repository,
        pull_head: str,
        current_yaml: UserYaml,
    ):
        if should_do_flaky_detection(repository, current_yaml):
            redis_client = get_redis_connection()
            redis_client.set(f"flake_uploads:{repository.repoid}", 0)
            redis_client.lpush(KEY_NAME.format(repository.repoid), pull_head)
            self.app.tasks[process_flakes_task_name].apply_async(
                kwargs={"repo_id": repository.repoid, "commit_id": pull_head}
            )

    def trigger_ai_pr_review(self, enriched_pull: EnrichedPull, current_yaml: UserYaml):
        pull = enriched_pull.database_pull
        if pull.state == "open" and read_yaml_field(
            current_yaml, ("ai_pr_review", "enabled"), False
        ):
            review_method = read_yaml_field(
                current_yaml, ("ai_pr_review", "method"), "auto"
            )
            label_name = read_yaml_field(
                current_yaml, ("ai_pr_review", "label_name"), None
            )
            pull_labels = enriched_pull.provider_pull.get("labels", [])
            if review_method == "auto" or (
                review_method == "label" and label_name in pull_labels
            ):
                log.info(
                    "Triggering AI PR review task",
                    extra={
                        "repoid": pull.repoid,
                        "pullid": pull.pullid,
                        "review_method": review_method,
                        "lbale_name": label_name,
                        "pull_labels": pull_labels,
                    },
                )
                self.app.tasks[ai_pr_review_task_name].apply_async(
                    kwargs={"repoid": pull.repoid, "pullid": pull.pullid}
                )

    def analyze_related_commits_for_context(self, db_session, pull, current_yaml):
        """
        Analyzes related commits to provide better context for the pull request.
        This helps identify patterns and dependencies in the codebase.
        """
        repoid = pull.repoid
        head_commit = pull.get_head_commit()

        if not head_commit:
            return

        # Look for recent commits on the same branch for context
        recent_cutoff = head_commit.timestamp - timedelta(days=30)

        related_commits = (
            db_session.query(Commit)
            .filter(
                Commit.repoid == repoid,
                Commit.branch == head_commit.branch,
                Commit.timestamp.between(recent_cutoff, head_commit.timestamp),
                (Commit.pullid.is_(None) | (Commit.pullid != pull.pullid)),
                Commit.deleted == False,
            )
            .order_by(Commit.timestamp.desc())
            .limit(100)
            .all()
        )

        if related_commits:
            log.info(
                "Found related commits for pull request context",
                extra={
                    "repoid": repoid,
                    "pullid": pull.pullid,
                    "related_commits_count": len(related_commits),
                    "head_commit": head_commit.commitid,
                },
            )


RegisteredPullSyncTask = celery_app.register_task(PullSyncTask())
pull_sync_task = celery_app.tasks[RegisteredPullSyncTask.name]
