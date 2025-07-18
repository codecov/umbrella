import logging
from typing import Any, Literal

from asgiref.sync import async_to_sync
from sqlalchemy.orm import Session

from app import celery_app
from database.enums import ReportType
from database.models import (
    Commit,
    CommitReport,
    Flake,
    Repository,
    TestResultReportTotals,
    UploadError,
)
from helpers.checkpoint_logger.flows import TestResultsFlow
from helpers.notifier import NotifierResult
from helpers.string import EscapeEnum, Replacement, StringEscaper, shorten_file_paths
from services.activation import activate_user, schedule_new_user_activated_task
from services.lock_manager import LockManager, LockRetry, LockType
from services.repository import (
    fetch_and_update_pull_request_information_from_commit,
    get_repo_provider_service,
)
from services.seats import ShouldActivateSeat, determine_seat_activation
from services.test_analytics.ta_finish_upload import new_impl
from services.test_analytics.ta_metrics import (
    read_failures_summary,
    read_tests_totals_summary,
)
from services.test_analytics.ta_process_flakes import KEY_NAME
from services.test_results import (
    ErrorPayload,
    FinisherResult,
    FlakeInfo,
    TACommentInDepthInfo,
    TestResultsNotificationFailure,
    TestResultsNotificationPayload,
    TestResultsNotifier,
    get_test_summary_for_commit,
    latest_failures_for_commit,
    should_do_flaky_detection,
)
from shared.celery_config import test_results_finisher_task_name
from shared.helpers.redis import get_redis_connection
from shared.reports.types import UploadType
from shared.typings.torngit import AdditionalData
from shared.yaml import UserYaml
from tasks.base import BaseCodecovTask
from tasks.cache_test_rollups import cache_test_rollups_task_name
from tasks.notify import notify_task_name
from tasks.process_flakes import NEW_KEY, process_flakes_task_name

log = logging.getLogger(__name__)

ESCAPE_FAILURE_MESSAGE_DEFN = [
    Replacement(["\r"], "", EscapeEnum.REPLACE),
]


class TestResultsFinisherTask(BaseCodecovTask, name=test_results_finisher_task_name):
    def run_impl(
        self,
        db_session: Session,
        _chain_result: bool,
        *,
        repoid: int,
        commitid: str,
        commit_yaml: dict,
        impl_type: Literal["old", "new", "both"] = "old",
        **kwargs,
    ):
        repoid = int(repoid)

        self.extra_dict: dict[str, Any] = {
            "commit_yaml": commit_yaml,
            "impl_type": impl_type,
        }
        log.info("Starting test results finisher task", extra=self.extra_dict)

        lock_manager = LockManager(
            repoid=repoid,
            commitid=commitid,
            report_type=ReportType.COVERAGE,
            lock_timeout=max(80, self.hard_time_limit_task),
        )

        try:
            # this needs to be the coverage notification lock
            # since both tests post/edit the same comment
            with lock_manager.locked(
                LockType.NOTIFICATION,
                retry_num=self.request.retries,
            ):
                finisher_result = self.process_impl_within_lock(
                    db_session=db_session,
                    repoid=repoid,
                    commitid=commitid,
                    commit_yaml=UserYaml.from_dict(commit_yaml),
                    impl_type=impl_type,
                    **kwargs,
                )
            if finisher_result["queue_notify"]:
                self.app.tasks[notify_task_name].apply_async(
                    args=None,
                    kwargs={
                        "repoid": repoid,
                        "commitid": commitid,
                        "current_yaml": commit_yaml,
                    },
                )

            return finisher_result

        except LockRetry as retry:
            self.retry(max_retries=5, countdown=retry.countdown)

    def process_impl_within_lock(
        self,
        *,
        db_session: Session,
        repoid: int,
        commitid: str,
        commit_yaml: UserYaml,
        impl_type: Literal["old", "new", "both"],
        **kwargs,
    ) -> FinisherResult:
        log.info("Running test results finishers", extra=self.extra_dict)
        TestResultsFlow.log(TestResultsFlow.TEST_RESULTS_FINISHER_BEGIN)

        commit: Commit = (
            db_session.query(Commit).filter_by(repoid=repoid, commitid=commitid).first()
        )
        assert commit, "commit not found"
        repo = commit.repository

        if impl_type == "old" or impl_type == "both":
            return self.old_impl(db_session, repo, commit, commit_yaml, impl_type)
        else:
            return new_impl(db_session, repo, commit, commit_yaml, impl_type)

    def optional_tasks(
        self,
        repo: Repository,
        commit: Commit,
        commit_yaml: UserYaml,
        impl_type: Literal["old", "both"],
    ):
        redis_client = get_redis_connection()

        if should_do_flaky_detection(repo, commit_yaml):
            if commit.merged is True or commit.branch == repo.branch:
                redis_client.lpush(NEW_KEY.format(repo.repoid), commit.commitid)
                if impl_type == "both":
                    redis_client.lpush(KEY_NAME.format(repo.repoid), commit.commitid)
                self.app.tasks[process_flakes_task_name].apply_async(
                    kwargs={
                        "repo_id": repo.repoid,
                        "impl_type": impl_type,
                    }
                )

        if commit.branch is not None:
            self.app.tasks[cache_test_rollups_task_name].apply_async(
                kwargs={
                    "repo_id": repo.repoid,
                    "branch": commit.branch,
                    "impl_type": impl_type,
                },
            )

    def get_totals(
        self, db_session: Session, commit_report: CommitReport
    ) -> TestResultReportTotals:
        totals = commit_report.test_result_totals
        if totals is None:
            totals = TestResultReportTotals(
                report_id=commit_report.id,
            )
            totals.passed = 0
            totals.skipped = 0
            totals.failed = 0
            db_session.add(totals)
            db_session.flush()

        return totals

    def get_upload_error(
        self, db_session: Session, commit_report: CommitReport
    ) -> ErrorPayload | None:
        upload_ids = [upload.id for upload in commit_report.uploads]
        upload_error = (
            db_session.query(UploadError)
            .filter(UploadError.upload_id.in_(upload_ids))
            .first()
        )
        if upload_error is None:
            return None

        match upload_error.error_code:
            case "unsupported_file_format":
                return ErrorPayload(
                    upload_error.error_code,
                    upload_error.error_params.get("error_message"),
                )
            case "file_not_in_storage":
                return ErrorPayload(upload_error.error_code, None)
            case "warning":
                return ErrorPayload(
                    upload_error.error_code,
                    upload_error.error_params.get("warning_message"),
                )

    def old_impl(
        self,
        db_session: Session,
        repo: Repository,
        commit: Commit,
        commit_yaml: UserYaml,
        impl_type: Literal["old", "both"],
    ) -> FinisherResult:
        repoid = repo.repoid
        commitid = commit.commitid

        self.optional_tasks(repo, commit, commit_yaml, impl_type)

        commit_report = commit.commit_report(ReportType.TEST_RESULTS)
        totals = self.get_totals(db_session, commit_report)
        cached_uploads: dict[int, dict] = {}
        escaper = StringEscaper(ESCAPE_FAILURE_MESSAGE_DEFN)
        shorten_paths = commit_yaml.read_yaml_field(
            "test_analytics", "shorten_paths", _else=True
        )

        with read_tests_totals_summary.labels("old").time():
            test_summary = get_test_summary_for_commit(db_session, repoid, commitid)

        failed_tests = test_summary.get("error", 0) + test_summary.get("failure", 0)
        passed_tests = test_summary.get("pass", 0)
        skipped_tests = test_summary.get("skip", 0)

        failures = []
        if failed_tests:
            with read_failures_summary.labels("old").time():
                failed_test_instances = latest_failures_for_commit(
                    db_session, repoid, commitid
                )

            for test_instance in failed_test_instances:
                failure_message = test_instance.failure_message
                if failure_message is not None:
                    if shorten_paths:
                        failure_message = shorten_file_paths(failure_message)
                    failure_message = escaper.replace(failure_message)

                if test_instance.upload_id not in cached_uploads:
                    upload = test_instance.upload
                    cached_uploads[test_instance.upload_id] = {
                        "flag_names": sorted(upload.flag_names),
                        "build_url": upload.build_url,
                    }
                upload = cached_uploads[test_instance.upload_id]

                failures.append(
                    TestResultsNotificationFailure(
                        display_name=test_instance.test.computed_name
                        if test_instance.test.computed_name is not None
                        else test_instance.test.name,
                        failure_message=failure_message,
                        test_id=test_instance.test_id,
                        envs=upload["flag_names"],
                        duration_seconds=test_instance.duration_seconds,
                        build_url=upload["build_url"],
                    )
                )

        totals.passed = passed_tests
        totals.skipped = skipped_tests
        totals.failed = failed_tests
        db_session.flush()

        error_payload = self.get_upload_error(db_session, commit_report)

        additional_data: AdditionalData = {"upload_type": UploadType.TEST_RESULTS}
        repo_service = get_repo_provider_service(repo, additional_data=additional_data)
        pull = async_to_sync(fetch_and_update_pull_request_information_from_commit)(
            repo_service, commit, commit_yaml
        )

        if failed_tests == 0:
            if error_payload is None:
                return {
                    "notify_attempted": False,
                    "notify_succeeded": False,
                    "queue_notify": True,
                }
            else:
                payload = TestResultsNotificationPayload(
                    failed_tests, passed_tests, skipped_tests, None
                )
                notifier = TestResultsNotifier(
                    commit,
                    commit_yaml,
                    payload=payload,
                    _pull=pull,
                    _repo_service=repo_service,
                    error=error_payload,
                )
                notifier_result = notifier.notify()
                success = (
                    True if notifier_result is NotifierResult.COMMENT_POSTED else False
                )
                TestResultsFlow.log(TestResultsFlow.TEST_RESULTS_NOTIFY)
                return {
                    "notify_attempted": True,
                    "notify_succeeded": success,
                    "queue_notify": True,
                }

        if not pull:
            success = False
            attempted = False
            notifier_result = NotifierResult.NO_PULL
        elif not commit_yaml.read_yaml_field("comment", _else=True):
            success = False
            attempted = False
            notifier_result = NotifierResult.NO_COMMENT
        else:
            activate_seat_info = determine_seat_activation(pull)

            should_show_upgrade_message = True

            match activate_seat_info.should_activate_seat:
                case ShouldActivateSeat.AUTO_ACTIVATE:
                    assert activate_seat_info.owner_id
                    assert activate_seat_info.author_id
                    successful_activation = activate_user(
                        db_session=db_session,
                        org_ownerid=activate_seat_info.owner_id,
                        user_ownerid=activate_seat_info.author_id,
                    )
                    if successful_activation:
                        schedule_new_user_activated_task(
                            activate_seat_info.owner_id,
                            activate_seat_info.author_id,
                        )
                        should_show_upgrade_message = False
                case ShouldActivateSeat.MANUAL_ACTIVATE:
                    pass
                case ShouldActivateSeat.NO_ACTIVATE:
                    should_show_upgrade_message = False

            if should_show_upgrade_message:
                notifier = TestResultsNotifier(
                    commit, commit_yaml, _pull=pull, _repo_service=repo_service
                )
                success, reason = notifier.upgrade_comment()

                self.extra_dict["success"] = success
                self.extra_dict["reason"] = reason
                log.info("Made upgrade comment", extra=self.extra_dict)

                return {
                    "notify_attempted": True,
                    "notify_succeeded": success,
                    "queue_notify": False,
                }

            flaky_tests = {}
            if should_do_flaky_detection(repo, commit_yaml):
                flaky_tests = self.get_flaky_tests(db_session, repoid, failures)

            failures = sorted(failures, key=lambda x: x.duration_seconds)[:3]
            info = TACommentInDepthInfo(failures, flaky_tests)
            payload = TestResultsNotificationPayload(
                failed_tests, passed_tests, skipped_tests, info
            )
            notifier = TestResultsNotifier(
                commit,
                commit_yaml,
                payload=payload,
                _pull=pull,
                _repo_service=repo_service,
                error=error_payload,
            )

            if repo.private == False:
                log.info(
                    "making TA comment",
                    extra={
                        "pullid": pull.database_pull.pullid,
                        "service": repo.service,
                        "slug": f"{repo.owner.username}/{repo.name}",
                    },
                )
            notifier_result = notifier.notify()
            success = (
                True if notifier_result is NotifierResult.COMMENT_POSTED else False
            )
            TestResultsFlow.log(TestResultsFlow.TEST_RESULTS_NOTIFY)
            if len(flaky_tests):
                log.info(
                    "Detected failure on test that has been identified as flaky",
                    extra={
                        "success": success,
                        "notifier_result": notifier_result.value,
                        "test_ids": list(flaky_tests.keys()),
                    },
                )
            attempted = True

        self.extra_dict["success"] = success
        self.extra_dict["notifier_result"] = notifier_result.value
        log.info("Finished test results notify", extra=self.extra_dict)

        return {
            "notify_attempted": attempted,
            "notify_succeeded": success,
            "queue_notify": False,
        }

    def get_flaky_tests(
        self,
        db_session: Session,
        repoid: int,
        failures: list[TestResultsNotificationFailure[str]],
    ) -> dict[str, FlakeInfo]:
        failure_test_ids = [failure.test_id for failure in failures]

        matching_flakes = list(
            db_session.query(Flake)
            .filter(
                Flake.repoid == repoid,
                Flake.testid.in_(failure_test_ids),
                Flake.end_date.is_(None),
                Flake.count != (Flake.recent_passes_count + Flake.fail_count),
            )
            .limit(100)
            .all()
        )

        flaky_test_ids = {
            flake.testid: FlakeInfo(flake.fail_count, flake.count)
            for flake in matching_flakes
        }
        return flaky_test_ids


RegisteredTestResultsFinisherTask = celery_app.register_task(TestResultsFinisherTask())
test_results_finisher_task = celery_app.tasks[RegisteredTestResultsFinisherTask.name]
