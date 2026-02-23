import logging

from app import celery_app
from database.enums import CommitErrorTypes
from database.models import Commit
from helpers.exceptions import RepositoryWithoutValidBotError
from helpers.github_installation import get_installation_name_for_owner_for_task
from helpers.save_commit_error import save_commit_error
from services.lock_manager import LockManager, LockRetry, LockType
from services.report import ReportService
from services.repository import (
    fetch_commit_yaml_and_possibly_store,
    get_repo_provider_service,
    possibly_update_commit_from_provider_info,
)
from shared.celery_config import (
    DEFAULT_LOCK_TIMEOUT_SECONDS,
    PREPROCESS_UPLOAD_MAX_RETRIES,
    pre_process_upload_task_name,
)
from shared.django_apps.upload_breadcrumbs.models import Errors, Milestones
from shared.helpers.redis import get_redis_connection
from shared.torngit.base import TorngitBaseAdapter
from tasks.base import BaseCodecovTask, clamp_retry_countdown

log = logging.getLogger(__name__)


class PreProcessUpload(BaseCodecovTask, name=pre_process_upload_task_name):
    """
    The main goal for this task is to carry forward flags from previous uploads
    and save the new carry-forwarded upload in the db, as a pre-step for
    uploading a report to codecov
    """

    def run_impl(self, db_session, *, repoid: int, commitid: str, **kwargs):
        log.info(
            "Received preprocess upload task",
            extra={"repoid": repoid, "commit": commitid},
        )
        lock_manager = LockManager(
            repoid=repoid,
            commitid=commitid,
            lock_timeout=self.get_lock_timeout(DEFAULT_LOCK_TIMEOUT_SECONDS),
            blocking_timeout=None,
        )
        lock_name = lock_manager.lock_name(LockType.PREPROCESS_UPLOAD)
        redis_connection = get_redis_connection()
        # This task only needs to run once per commit to generate the report.
        # So if one is already running we don't need another.
        if redis_connection.get(lock_name):
            log.info(
                "PreProcess task is already running",
                extra={"commit": commitid, "repoid": repoid},
            )
            # We consider this a success, as the task is already running
            self._call_upload_breadcrumb_task(
                commit_sha=commitid,
                repo_id=repoid,
                milestone=Milestones.READY_FOR_REPORT,
            )
            return {"preprocessed_upload": False, "reason": "already_running"}
        try:
            with lock_manager.locked(
                LockType.PREPROCESS_UPLOAD,
                max_retries=PREPROCESS_UPLOAD_MAX_RETRIES,
                retry_num=self.attempts,
            ):
                return self.process_impl_within_lock(
                    db_session=db_session,
                    repoid=repoid,
                    commitid=commitid,
                )
        except LockRetry as retry:
            self._call_upload_breadcrumb_task(
                commit_sha=commitid,
                repo_id=repoid,
                milestone=Milestones.READY_FOR_REPORT,
                error=Errors.INTERNAL_LOCK_ERROR,
            )
            if retry.max_retries_exceeded or self._has_exceeded_max_attempts(
                PREPROCESS_UPLOAD_MAX_RETRIES
            ):
                return {
                    "preprocessed_upload": False,
                    "reason": "unable_to_acquire_lock",
                }
            self._call_upload_breadcrumb_task(
                commit_sha=commitid,
                repo_id=repoid,
                milestone=Milestones.READY_FOR_REPORT,
                error=Errors.INTERNAL_RETRYING,
            )
            self.retry(
                max_retries=PREPROCESS_UPLOAD_MAX_RETRIES,
                countdown=clamp_retry_countdown(retry.countdown),
            )

    def process_impl_within_lock(self, db_session, repoid, commitid):
        commit = (
            db_session.query(Commit)
            .filter(Commit.repoid == repoid, Commit.commitid == commitid)
            .first()
        )
        assert commit, "Commit not found in database."
        installation_name_to_use = get_installation_name_for_owner_for_task(
            self.name, commit.repository.author
        )
        repository_service = self.get_repo_service(commit, installation_name_to_use)
        if repository_service is None:
            log.warning(
                "Failed to get repository_service",
                extra={"commit": commitid, "repo": repoid},
            )
            return {
                "preprocessed_upload": False,
                "updated_commit": False,
                "error": "Failed to get repository_service",
            }
        # Makes sure that we can properly carry forward reports
        # By populating the commit info (if needed)
        updated_commit = possibly_update_commit_from_provider_info(
            commit=commit, repository_service=repository_service
        )
        commit_yaml = fetch_commit_yaml_and_possibly_store(commit, repository_service)
        report_service = ReportService(
            commit_yaml, gh_app_installation_name=installation_name_to_use
        )
        commit_report = report_service.initialize_and_save_report(commit)
        # Persist changes from within the lock
        db_session.commit()
        self._call_upload_breadcrumb_task(
            commit_sha=commitid,
            repo_id=repoid,
            milestone=Milestones.READY_FOR_REPORT,
        )
        return {
            "preprocessed_upload": True,
            "reportid": str(commit_report.external_id),
            "updated_commit": updated_commit,
        }

    def get_repo_service(
        self, commit: Commit, installation_name_to_use: str
    ) -> TorngitBaseAdapter | None:
        repository_service = None
        try:
            repository_service = get_repo_provider_service(
                commit.repository,
                installation_name_to_use=installation_name_to_use,
            )
        except RepositoryWithoutValidBotError:
            save_commit_error(
                commit,
                error_code=CommitErrorTypes.REPO_BOT_INVALID.value,
                error_params={
                    "repoid": commit.repoid,
                    "repository_service": repository_service,
                },
            )
            self._call_upload_breadcrumb_task(
                commit_sha=commit.commitid,
                repo_id=commit.repoid,
                milestone=Milestones.READY_FOR_REPORT,
                error=Errors.REPO_MISSING_VALID_BOT,
            )
            log.warning(
                "Unable to reach git provider because repo doesn't have a valid bot",
                extra={"repoid": commit.repoid, "commit": commit.commitid},
            )

        return repository_service


RegisteredUploadTask = celery_app.register_task(PreProcessUpload())
preprocess_upload_task = celery_app.tasks[RegisteredUploadTask.name]
