import html
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from hashlib import sha256
from typing import Literal, TypedDict, TypeVar
from urllib.parse import quote_plus

import sentry_sdk

from database.enums import ReportType
from database.models import (
    Commit,
    CommitReport,
    Repository,
    RepositoryFlag,
    Upload,
)
from helpers.environment import is_enterprise
from helpers.notifier import BaseNotifier
from services.processing.types import UploadArguments
from services.report import BaseReportService
from services.repository import EnrichedPull
from services.urls import get_members_url, get_test_analytics_url
from services.yaml import read_yaml_field
from shared.django_apps.codecov_auth.models import Plan
from shared.plan.constants import TierName
from shared.yaml import UserYaml


class NotifierTaskResult(TypedDict):
    attempted: bool
    succeeded: bool


class FinisherResult(TypedDict):
    notify_attempted: bool
    notify_succeeded: bool
    queue_notify: bool


log = logging.getLogger(__name__)


class TestResultsReportService(BaseReportService):
    def __init__(self, current_yaml: UserYaml):
        super().__init__(current_yaml)
        self.flag_dict = None

    def initialize_and_save_report(self, commit: Commit) -> CommitReport:
        db_session = commit.get_db_session()
        current_report_row = (
            db_session.query(CommitReport)
            .filter_by(
                commit_id=commit.id_,
                code=None,
                report_type=ReportType.TEST_RESULTS.value,
            )
            .first()
        )
        if not current_report_row:
            # This happens if the commit report is being created for the first time
            # or backfilled
            current_report_row = CommitReport(
                commit_id=commit.id_,
                code=None,
                report_type=ReportType.TEST_RESULTS.value,
            )
            db_session.add(current_report_row)
            db_session.flush()

        return current_report_row

    # support flags in test results
    def create_report_upload(
        self, arguments: UploadArguments, commit_report: CommitReport
    ) -> Upload:
        upload = super().create_report_upload(arguments, commit_report)
        self._attach_flags_to_upload(upload, arguments["flags"])
        return upload

    def _attach_flags_to_upload(self, upload: Upload, flag_names: Sequence[str]):
        """Internal function that manages creating the proper `RepositoryFlag`s and attach the sessions to them

        Args:
            upload (Upload): Description
            flag_names (Sequence[str]): Description

        Returns:
            TYPE: Description
        """
        all_flags = []
        db_session = upload.get_db_session()
        repoid = upload.report.commit.repoid

        if self.flag_dict is None:
            self.fetch_repo_flags(db_session, repoid)

        for individual_flag in flag_names:
            flag_obj = self.flag_dict.get(individual_flag, None)
            if flag_obj is None:
                flag_obj = RepositoryFlag(
                    repository_id=repoid, flag_name=individual_flag
                )
                db_session.add(flag_obj)
                db_session.flush()
                self.flag_dict[individual_flag] = flag_obj
            all_flags.append(flag_obj)
        upload.flags = all_flags
        db_session.flush()
        return all_flags

    def fetch_repo_flags(self, db_session, repoid):
        existing_flags_on_repo = (
            db_session.query(RepositoryFlag).filter_by(repository_id=repoid).all()
        )
        self.flag_dict = {flag.flag_name: flag for flag in existing_flags_on_repo}


def generate_flags_hash(flag_names: list[str]) -> str:
    return sha256((" ".join(sorted(flag_names))).encode("utf-8")).hexdigest()


def generate_test_id(repoid, testsuite, name, flags_hash):
    return sha256(
        (" ".join([str(x) for x in [repoid, testsuite, name, flags_hash]])).encode(
            "utf-8"
        )
    ).hexdigest()


T = TypeVar("T", str, bytes)


@dataclass
class TestResultsNotificationFailure[T: (str, bytes)]:
    __test__ = False
    failure_message: str
    display_name: str
    envs: list[str]
    test_id: T
    duration_seconds: float
    build_url: str | None = None


@dataclass
class FlakeInfo:
    failed: int
    count: int


@dataclass
class TACommentInDepthInfo[T: (str, bytes)]:
    failures: list[TestResultsNotificationFailure[T]]
    flaky_tests: dict[T, FlakeInfo]


@dataclass
class TestResultsNotificationPayload[T: (str, bytes)]:
    failed: int
    passed: int
    skipped: int
    info: TACommentInDepthInfo[T] | None = None


@dataclass
class ErrorPayload:
    error_code: Literal["unsupported_file_format", "file_not_in_storage", "warning"]
    error_message: str | None = None


def wrap_in_details(summary: str, content: str):
    result = f"<details><summary>{summary}</summary>\n{content}\n</details>"
    return result


def make_quoted(content: str) -> str:
    lines = content.splitlines()
    result = "\n".join("> " + line for line in lines)
    return f"\n{result}\n"


def wrap_in_code(content: str, language: str = "python") -> str:
    escaped_content = html.escape(content)
    return f'<pre><code class="language-{language}">{escaped_content}</code></pre>'


def display_duration(f: float) -> str:
    split_duration = str(f).split(".")
    before_dot = split_duration[0]
    if len(before_dot) > 3:
        return before_dot
    else:
        return f"{f:.3g}"


def generate_failure_info[T: (str, bytes)](
    fail: TestResultsNotificationFailure[T],
):
    if fail.failure_message is not None:
        failure_message = fail.failure_message
    else:
        failure_message = "No failure message available"

    failure_message = wrap_in_code(failure_message)
    if fail.build_url:
        return f"{failure_message}\n[View]({fail.build_url}) the CI Build"
    else:
        return failure_message


def generate_view_test_analytics_line(
    repo: Repository, commit_branch: str | None
) -> str:
    test_analytics_url = get_test_analytics_url(repo, commit_branch)
    return f"\nTo view more test analytics, go to the [Test Analytics Dashboard]({test_analytics_url})\n<sub>📋 Got 3 mins? [Take this short survey](https://forms.gle/22i53Qa1CySZjA6c7) to help us improve Test Analytics.</sub>"


def generate_view_prevent_analytics_line(
    repo: Repository, commit_branch: str | None
) -> str | None:
    owner = getattr(repo, "author", None)
    account = getattr(owner, "account", None) if owner else None
    account_name = getattr(account, "name", None) if account else None
    owner_username = getattr(owner, "username", None) if owner else None
    repo_name = getattr(repo, "name", None)

    if not all([account_name, owner_username, repo_name, commit_branch]):
        return None

    prevent_url = (
        f"https://{account_name}.sentry.io/prevent/tests/?preventPeriod=30d"
        f"&integratedOrgName={quote_plus(owner_username)}"
        f"&repository={quote_plus(repo_name)}"
        f"&branch={quote_plus(commit_branch)}"
    )

    return (
        "\nTo view more test analytics, go to the "
        f"[Prevent Tests Dashboard]({prevent_url})"
    )


def messagify_failure[T: (str, bytes)](
    failure: TestResultsNotificationFailure[T],
) -> str:
    test_name = wrap_in_code(failure.display_name.replace("\x1f", " "))
    formatted_duration = display_duration(failure.duration_seconds)
    stack_trace_summary = f"Stack Traces | {formatted_duration}s run time"
    stack_trace = wrap_in_details(
        stack_trace_summary,
        make_quoted(generate_failure_info(failure)),
    )
    return make_quoted(f"{test_name}\n{stack_trace}")


def messagify_flake[T: (str, bytes)](
    flaky_failure: TestResultsNotificationFailure[T],
    flake_info: FlakeInfo,
) -> str:
    test_name = wrap_in_code(flaky_failure.display_name.replace("\x1f", " "))
    formatted_duration = display_duration(flaky_failure.duration_seconds)
    flake_rate = flake_info.failed / flake_info.count * 100
    flake_rate_section = f"**Flake rate in main:** {flake_rate:.2f}% (Passed {flake_info.count - flake_info.failed} times, Failed {flake_info.failed} times)"
    stack_trace_summary = f"Stack Traces | {formatted_duration}s run time"
    stack_trace = wrap_in_details(
        stack_trace_summary,
        make_quoted(generate_failure_info(flaky_failure)),
    )
    return make_quoted(f"{test_name}\n{flake_rate_section}\n{stack_trace}")


def short_error_message(error: ErrorPayload) -> str | None:
    if error.error_code == "unsupported_file_format":
        return "Test Analytics upload error: Unsupported file format"
    elif error.error_code == "file_not_in_storage":
        return "Test Analytics upload error: File not in storage"

    return None


def specific_error_message(error: ErrorPayload) -> str:
    if error.error_code == "unsupported_file_format":
        title = "### :warning: Unsupported file format"

        assert error.error_message is not None

        message = [
            wrap_in_details(
                "Upload processing failed due to unsupported file format. Please review the parser error message:",
                wrap_in_code(error.error_message),
            ),
            "",
            "For more help, visit our [troubleshooting guide](https://docs.codecov.com/docs/test-analytics#troubleshooting).",
        ]
        description = "\n".join(message)
    elif error.error_code == "file_not_in_storage":
        title = "### :warning: JUnit XML file not found"
        description = "\n".join(
            [
                "The CLI was unable to find any JUnit XML files to upload.",
                "For more help, visit our [troubleshooting guide](https://docs.codecov.com/docs/test-analytics#troubleshooting).",
            ]
        )
    elif error.error_code == "warning":
        title = "### :warning: Parser warning"

        # we will always expect a specific error message with a warning
        assert error.error_message is not None

        message = [
            wrap_in_details(
                "The parser emitted a warning. Please review your JUnit XML file:",
                wrap_in_code(error.error_message),
            ),
            "",
            "For more help, visit our [troubleshooting guide](https://docs.codecov.com/docs/test-analytics#troubleshooting).",
        ]
        description = "\n".join(message)
    else:
        sentry_sdk.capture_message(
            f"Unrecognized error code: {error.error_code}",
            level="error",
        )
        return "Error processing test results"

    message = [
        title,
        description,
    ]
    return "\n".join(message)


@dataclass
class TestResultsNotifier[T: (str, bytes)](BaseNotifier):
    payload: TestResultsNotificationPayload[T] | None = None
    error: ErrorPayload | None = None
    for_prevent: bool = False

    def build_message(self) -> str:
        if self.payload is None:
            raise ValueError("Payload passed to notifier is None, cannot build message")

        message = []

        # commenting out the upload error code because we it's too noisy and confusing for now
        # if self.error:
        #     message.append(specific_error_message(self.error))

        # if self.error and self.payload.info:
        #     message += ["", "---", ""]

        if self.payload.info:
            message.append(f"### :x: {self.payload.failed} Tests Failed:")

            completed = self.payload.failed + self.payload.passed

            message += [
                "| Tests completed | Failed | Passed | Skipped |",
                "|---|---|---|---|",
                f"| {completed} | {self.payload.failed} | {self.payload.passed} | {self.payload.skipped} |",
            ]

            failures = sorted(
                (
                    failure
                    for failure in self.payload.info.failures
                    if failure.test_id not in self.payload.info.flaky_tests
                ),
                key=lambda x: (x.duration_seconds, x.display_name),
            )
            if failures:
                failure_content = [
                    f"{messagify_failure(failure)}" for failure in failures
                ]

                top_3_failed_section = wrap_in_details(
                    f"View the top {min(3, len(failures))} failed test(s) by shortest run time",
                    "\n".join(failure_content),
                )

                message.append(top_3_failed_section)

            flaky_failures = [
                failure
                for failure in self.payload.info.failures
                if failure.test_id in self.payload.info.flaky_tests
            ]
            if flaky_failures:
                flake_content = [
                    f"{messagify_flake(flaky_failure, self.payload.info.flaky_tests[flaky_failure.test_id])}"
                    for flaky_failure in flaky_failures
                ]

                flaky_section = wrap_in_details(
                    f"View the full list of {len(flaky_failures)} :snowflake: flaky test(s)",
                    "\n".join(flake_content),
                )

                message.append(flaky_section)

            commit_branch = self.commit.branch
            if not self.for_prevent:
                message.append(
                    generate_view_test_analytics_line(
                        # TODO: Deprecate database-reliant code path after old TA pipeline is removed
                        self.repo,
                        commit_branch,
                    )
                )
            else:
                prevent_line = generate_view_prevent_analytics_line(
                    self.repo, commit_branch
                )
                if prevent_line:
                    message.append(prevent_line)
        return "\n".join(message)

    def error_comment(self):
        message: str

        if self.error is None:
            message = "Test Analytics upload error: We are unable to process any of the uploaded JUnit XML files. Please ensure your files are in the right format."
        else:
            message = specific_error_message(self.error)

        pull = self.get_pull()
        if pull is None:
            return False, "no_pull"

        sent_to_provider = self.send_to_provider(pull, message)

        if sent_to_provider is False:
            return False, "torngit_error"

        return True, "comment_posted"

    def all_passed_comment(self, duration_seconds: float | None = None):
        # TODO: use something like:
        # https://github.com/getsentry/sentry/blob/ca40c14646767fdcd06dd90a8089c116bd8f6cde/static/app/utils/duration/getDuration.tsx
        # to format the duration nicely
        if duration_seconds is not None:
            message = f":white_check_mark: All tests passed in {duration_seconds:.2f}s."
        else:
            message = ":white_check_mark: All tests passed."

        pull = self.get_pull()
        if pull is None:
            return False, "no_pull"

        sent_to_provider = self.send_to_provider(pull, message)
        if sent_to_provider is False:
            return False, "torngit_error"

        return True, "comment_posted"

    def upgrade_comment(self):
        pull = self.get_pull()
        if pull is None:
            return False, "no_pull"

        # TODO: this will need to change when seat activation in the new pipeline is added
        assert isinstance(pull, EnrichedPull), "Expected EnrichedPull type"

        db_pull = pull.database_pull
        provider_pull = pull.provider_pull
        if provider_pull is None:
            return False, "missing_provider_pull"

        link = get_members_url(db_pull)

        author_username = provider_pull["author"].get("username")

        if not is_enterprise():
            message = "\n".join(
                [
                    f"The author of this PR, {author_username}, is not an activated member of this organization on Codecov.",
                    f"Please [activate this user on Codecov]({link}) to display this PR comment.",
                    "Coverage data is still being uploaded to Codecov.io for purposes of overall coverage calculations.",
                    "Please don't hesitate to email us at support@codecov.io with any questions.",
                ]
            )
        else:
            message = "\n".join(
                [
                    f"The author of this PR, {author_username}, is not activated in your Codecov Self-Hosted installation.",
                    f"Please [activate this user]({link}) to display this PR comment.",
                    "Coverage data is still being uploaded to Codecov Self-Hosted for the purposes of overall coverage calculations.",
                    "Please contact your Codecov On-Premises installation administrator with any questions.",
                ]
            )

        sent_to_provider = self.send_to_provider(pull, message)
        if sent_to_provider == False:
            return False, "torngit_error"

        return True, "comment_posted"


def not_private_and_free_or_team(repo: Repository):
    plan_name = repo.author.plan
    if repo.author.account:
        plan_name = repo.author.account.plan

    plan = Plan.objects.select_related("tier").get(name=plan_name)

    return not (
        repo.private
        and plan
        and plan.tier.tier_name in {TierName.BASIC.value, TierName.TEAM.value}
    )


def should_do_flaky_detection(repo: Repository, commit_yaml: UserYaml) -> bool:
    has_flaky_configured = read_yaml_field(
        commit_yaml, ("test_analytics", "flake_detection"), True
    )
    has_valid_plan_repo_or_owner = not_private_and_free_or_team(repo)

    return has_flaky_configured and has_valid_plan_repo_or_owner
