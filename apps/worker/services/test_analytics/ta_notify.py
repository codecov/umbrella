from pathlib import Path
from typing import TypeVar

import sentry_sdk
from jinja2 import Environment, FileSystemLoader

from database.models.core import Commit
from services.license import requires_license
from services.test_analytics.ta_types import (
    ErrorPayload,
    TestResultsNotificationPayload,
)
from services.urls import get_test_analytics_url

T = TypeVar("T", str, bytes)

# Set up Jinja2 environment
template_dir = Path(__file__).parent / "templates"
jinja_env = Environment(loader=FileSystemLoader(template_dir))


def display_duration(f: float) -> str:
    split_duration = str(f).split(".")
    before_dot = split_duration[0]
    if len(before_dot) > 3:
        return before_dot
    else:
        return f"{f:.3g}"


def generate_view_test_analytics_line(commit: Commit) -> str:
    repo = commit.repository
    test_analytics_url = get_test_analytics_url(repo, commit)
    return f"\nTo view more test analytics, go to the [Test Analytics Dashboard]({test_analytics_url})\n<sub>📋 Got 3 mins? [Take this short survey](https://forms.gle/BpocVj23nhr2Y45G7) to help us improve Test Analytics.</sub>"


def build_upgrade_comment_message(author_username: str, members_url: str) -> str:
    """Build upgrade comment message using Jinja template"""
    template = jinja_env.get_template("upgrade_comment.j2")
    return template.render(
        author_username=author_username,
        members_url=members_url,
        requires_license=requires_license(),
    )


def build_error_comment_message(error: ErrorPayload | None = None) -> str:
    """Build error comment message using Jinja template"""
    if error is None:
        # Use default error message
        template = jinja_env.get_template("error_comment_default.j2")
        return template.render()
    else:
        # Use specific error message template (reuse existing error template)
        if error.error_code not in [
            "unsupported_file_format",
            "file_not_in_storage",
            "warning",
        ]:
            sentry_sdk.capture_message(
                f"Unrecognized error code: {error.error_code}",
                level="error",
            )
            return "Error processing test results"
        else:
            template = jinja_env.get_template("error_message.j2")
            return template.render(error=error)


def build_test_results_message[T: (str, bytes)](
    payload: TestResultsNotificationPayload[T],
    error: ErrorPayload | None,
    commit: Commit,
) -> str:
    # Handle error case with Sentry logging for unrecognized error codes
    error_message = None
    if error:
        if error.error_code not in [
            "unsupported_file_format",
            "file_not_in_storage",
            "warning",
        ]:
            sentry_sdk.capture_message(
                f"Unrecognized error code: {error.error_code}",
                level="error",
            )
            error_message = "Error processing test results"
        else:
            error_template = jinja_env.get_template("error_message.j2")
            error_message = error_template.render(error=error)

    test_results_section = None
    if payload.info:
        failures = sorted(
            (
                failure
                for failure in payload.info.failures
                if failure.test_id not in payload.info.flaky_tests
            ),
            key=lambda x: (x.duration_seconds, x.display_name),
        )

        flaky_failures = [
            failure
            for failure in payload.info.failures
            if failure.test_id in payload.info.flaky_tests
        ]

        # Create formatted duration mappings for template use
        formatted_durations = {}
        for failure in failures + flaky_failures:
            formatted_durations[failure.test_id] = display_duration(
                failure.duration_seconds
            )

        view_test_analytics_line = generate_view_test_analytics_line(commit)

        test_results_template = jinja_env.get_template("test_results.j2")
        test_results_section = test_results_template.render(
            payload=payload,
            failures=failures,
            flaky_failures=flaky_failures,
            flaky_tests=payload.info.flaky_tests,
            formatted_durations=formatted_durations,
            view_test_analytics_line=view_test_analytics_line,
        )

    main_template = jinja_env.get_template("main.j2")
    return main_template.render(
        error=error,
        error_message=error_message,
        payload=payload,
        test_results_section=test_results_section,
    )
