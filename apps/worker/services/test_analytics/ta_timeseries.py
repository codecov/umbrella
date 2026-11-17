from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import TypedDict

import test_results_parser
from django.db import connections
from django.db.models import Q

from services.test_results import FlakeInfo
from shared.django_apps.ta_timeseries.models import (
    Testrun,
    calc_test_id,
)
from shared.django_apps.test_analytics.models import Flake

log = logging.getLogger(__name__)

# Postgres rejects any single allocation >= 1GB, and psycopg2 inlines all
# parameters into a single query string, so we must bound both the size of
# individual failure messages and the total payload of each INSERT.
MAX_FAILURE_MESSAGE_LEN = 64 * 1024
MAX_INSERT_BATCH_BYTES = 64 * 1024 * 1024
MAX_INSERT_BATCH_ROWS = 500
_TRUNCATION_MARKER = "\n...[truncated {} chars]...\n"


def truncate_failure_message(message: str | None) -> str | None:
    if message is None or len(message) <= MAX_FAILURE_MESSAGE_LEN:
        return message
    removed = len(message) - MAX_FAILURE_MESSAGE_LEN
    marker = _TRUNCATION_MARKER.format(removed)
    keep = MAX_FAILURE_MESSAGE_LEN - len(marker)
    head = keep // 2
    tail = keep - head
    return message[:head] + marker + message[len(message) - tail :]


def _estimate_testrun_size(testrun: Testrun) -> int:
    size = 256  # fixed-width columns + overhead
    for value in (
        testrun.name,
        testrun.classname,
        testrun.testsuite,
        testrun.computed_name,
        testrun.failure_message,
        testrun.filename,
        testrun.framework,
        testrun.branch,
        testrun.commit_sha,
    ):
        if value:
            size += len(value)
    if testrun.properties:
        size += len(str(testrun.properties))
    if testrun.flags:
        size += sum(len(f) for f in testrun.flags)
    return size


def _bulk_create_in_size_bounded_batches(testruns: list[Testrun]) -> None:
    batch: list[Testrun] = []
    batch_bytes = 0
    for testrun in testruns:
        row_bytes = _estimate_testrun_size(testrun)
        if batch and (
            batch_bytes + row_bytes > MAX_INSERT_BATCH_BYTES
            or len(batch) >= MAX_INSERT_BATCH_ROWS
        ):
            Testrun.objects.bulk_create(batch)
            batch = []
            batch_bytes = 0
        batch.append(testrun)
        batch_bytes += row_bytes
    if batch:
        Testrun.objects.bulk_create(batch)


def get_flaky_tests_set(repo_id: int) -> set[bytes]:
    return {
        bytes(test_id)
        for test_id in Flake.objects.filter(repoid=repo_id, end_date__isnull=True)
        .values_list("test_id", flat=True)
        .distinct()
    }


def get_flaky_tests_dict(repo_id: int) -> dict[bytes, FlakeInfo]:
    return {
        bytes(flake.test_id): FlakeInfo(flake.fail_count, flake.count)
        for flake in Flake.objects.filter(repoid=repo_id, end_date__isnull=True)
    }


def insert_testrun(
    timestamp: datetime,
    repo_id: int | None,
    commit_sha: str | None,
    branch: str | None,
    upload_id: int | None,
    flags: list[str] | None,
    parsing_info: test_results_parser.ParsingInfo,
    flaky_test_ids: set[bytes] | None = None,
):
    testruns_to_create = []
    for testrun in parsing_info["testruns"]:
        test_id = calc_test_id(
            testrun["name"], testrun["classname"], testrun["testsuite"]
        )
        outcome = testrun["outcome"]

        if outcome == "error":
            outcome = "failure"

        if outcome == "failure" and flaky_test_ids and test_id in flaky_test_ids:
            outcome = "flaky_fail"

        failure_message = testrun["failure_message"]
        truncated_failure_message = truncate_failure_message(failure_message)
        if truncated_failure_message is not failure_message:
            log.warning(
                "Truncated oversized test failure message",
                extra={
                    "repo_id": repo_id,
                    "upload_id": upload_id,
                    "computed_name": testrun["computed_name"],
                    "original_length": len(failure_message),
                },
            )

        testruns_to_create.append(
            Testrun(
                timestamp=timestamp,
                test_id=test_id,
                name=testrun["name"],
                classname=testrun["classname"],
                testsuite=testrun["testsuite"],
                computed_name=testrun["computed_name"]
                or f"{testrun['classname']}::{testrun['name']}",
                outcome=outcome,
                duration_seconds=testrun["duration"],
                failure_message=truncated_failure_message,
                framework=parsing_info["framework"],
                filename=testrun["filename"],
                properties=testrun.get("properties"),
                repo_id=repo_id,
                commit_sha=commit_sha,
                branch=branch,
                flags=flags,
                upload_id=upload_id,
            )
        )
    _bulk_create_in_size_bounded_batches(testruns_to_create)


class FailedTestInstance(TypedDict):
    test_id: bytes
    computed_name: str
    failure_message: str
    upload_id: int
    duration_seconds: float | None
    flags: list[str]


def get_pr_comment_failures(
    repo_id: int, commit_sha: str, lower_bound_timestamp: datetime | None = None
) -> list[FailedTestInstance]:
    with connections["ta_timeseries"].cursor() as cursor:
        if lower_bound_timestamp is not None:
            query = """
                SELECT
                    test_id,
                    LAST(computed_name, timestamp) as computed_name,
                    LAST(failure_message, timestamp) as failure_message,
                    LAST(upload_id, timestamp) as upload_id,
                    LAST(duration_seconds, timestamp) as duration_seconds,
                    LAST(flags, timestamp) as flags
                FROM ta_timeseries_testrun
                WHERE repo_id = %s AND commit_sha = %s AND outcome IN ('failure', 'flaky_fail') AND timestamp >= %s
                GROUP BY test_id
                ORDER BY computed_name ASC
            """
            params = [repo_id, commit_sha, lower_bound_timestamp]
        else:
            query = """
                SELECT
                    test_id,
                    LAST(computed_name, timestamp) as computed_name,
                    LAST(failure_message, timestamp) as failure_message,
                    LAST(upload_id, timestamp) as upload_id,
                    LAST(duration_seconds, timestamp) as duration_seconds,
                    LAST(flags, timestamp) as flags
                FROM ta_timeseries_testrun
                WHERE repo_id = %s AND commit_sha = %s AND outcome IN ('failure', 'flaky_fail')
                GROUP BY test_id
                ORDER BY computed_name ASC
            """
            params = [repo_id, commit_sha]

        cursor.execute(query, params)
        return [
            {
                "test_id": bytes(test_id),
                "computed_name": computed_name,
                "failure_message": failure_message,
                "upload_id": upload_id,
                "duration_seconds": duration_seconds,
                "flags": flags,
            }
            for test_id, computed_name, failure_message, upload_id, duration_seconds, flags in cursor.fetchall()
        ]


class PRCommentAgg(TypedDict):
    passed: int
    failed: int
    skipped: int


def get_pr_comment_duration(
    repo_id: int, commit_sha: str, lower_bound_timestamp: datetime | None = None
) -> float | None:
    with connections["ta_timeseries"].cursor() as cursor:
        if lower_bound_timestamp is not None:
            query = """
                SELECT
                    SUM(duration_seconds) as duration_seconds
                FROM (
                    SELECT
                        test_id,
                        LAST(duration_seconds, timestamp) as duration_seconds
                    FROM ta_timeseries_testrun
                    WHERE repo_id = %s AND commit_sha = %s AND timestamp >= %s
                    GROUP BY test_id
                    ) AS t
            """
            params = [repo_id, commit_sha, lower_bound_timestamp]
        else:
            query = """
                SELECT
                    SUM(duration_seconds) as duration_seconds
                FROM (
                    SELECT
                        test_id,
                        LAST(duration_seconds, timestamp) as duration_seconds
                    FROM ta_timeseries_testrun
                    WHERE repo_id = %s AND commit_sha = %s
                    GROUP BY test_id
                    ) AS t
            """
            params = [repo_id, commit_sha]

        cursor.execute(query, params)
        result = cursor.fetchone()
        if result is None:
            return None
        duration = result[0]
        return duration


def get_pr_comment_agg(
    repo_id: int, commit_sha: str, lower_bound_timestamp: datetime | None = None
) -> PRCommentAgg:
    # Always provide a timestamp lower bound so TimescaleDB can eliminate
    # irrelevant chunks. Fall back to 90 days ago if no timestamp is available,
    # which matches the typical data retention window and avoids full table scans.
    if lower_bound_timestamp is None:
        lower_bound_timestamp = datetime.now(tz=UTC) - timedelta(days=90)

    with connections["ta_timeseries"].cursor() as cursor:
        query = """
            SELECT outcome, count(*) FROM (
                SELECT
                    test_id,
                    LAST(outcome, timestamp) as outcome
                FROM ta_timeseries_testrun
                WHERE repo_id = %s AND commit_sha = %s AND timestamp >= %s
                GROUP BY test_id
            ) AS t
            GROUP BY outcome
        """
        params = [repo_id, commit_sha, lower_bound_timestamp]

        cursor.execute(query, params)
        outcome_dict = dict(cursor.fetchall())

        return {
            "passed": outcome_dict.get("pass", 0),
            "failed": outcome_dict.get("failure", 0)
            + outcome_dict.get("flaky_fail", 0),
            "skipped": outcome_dict.get("skip", 0),
        }


def get_testruns_for_flake_detection(
    upload_id: int,
    flaky_test_ids: set[bytes],
) -> list[Testrun]:
    return list(
        Testrun.objects.filter(
            Q(upload_id=upload_id)
            & (
                Q(outcome="failure")
                | Q(outcome="flaky_fail")
                | (Q(outcome="pass") & Q(test_id__in=flaky_test_ids))
            )
        )
    )


def update_testrun_to_flaky(timestamp: datetime, test_id: bytes):
    with connections["ta_timeseries"].cursor() as cursor:
        cursor.execute(
            "UPDATE ta_timeseries_testrun SET outcome = %s WHERE timestamp = %s AND test_id = %s",
            ["flaky_fail", timestamp, test_id],
        )
