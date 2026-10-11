from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TypedDict

import test_results_parser
from django.db import connections
from django.db.models import Q

from services.test_results import FlakeInfo
from shared.django_apps.ta_timeseries.models import (
    Testrun,
    TestrunCommitSummary,
    calc_test_id,
)
from shared.django_apps.test_analytics.models import Flake


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
                failure_message=testrun["failure_message"],
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
    Testrun.objects.bulk_create(testruns_to_create)

    # Upsert into the commit summary table so finisher queries can read
    # pre-aggregated data instead of running LAST() over raw timeseries rows.
    # Only overwrite when the incoming row has a later timestamp so we preserve
    # the same "last by timestamp" semantics as the original LAST() aggregate.
    rows = [t for t in testruns_to_create if t.commit_sha is not None]
    if rows:
        with connections["ta_timeseries"].cursor() as cursor:
            params = []
            for t in rows:
                params.extend(
                    [
                        t.repo_id,
                        t.commit_sha,
                        bytes(t.test_id),
                        t.timestamp,
                        t.outcome,
                        t.computed_name,
                        t.failure_message,
                        t.upload_id,
                        t.duration_seconds,
                        t.flags,
                    ]
                )
            cursor.execute(
                f"""
                INSERT INTO ta_testrun_commit_summary
                    (repo_id, commit_sha, test_id, last_timestamp, outcome,
                     computed_name, failure_message, upload_id, duration_seconds, flags)
                VALUES
                    {", ".join(
                        ["(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"] * len(rows)
                    )}
                ON CONFLICT (repo_id, commit_sha, test_id)
                DO UPDATE SET
                    last_timestamp   = EXCLUDED.last_timestamp,
                    outcome          = EXCLUDED.outcome,
                    computed_name    = EXCLUDED.computed_name,
                    failure_message  = EXCLUDED.failure_message,
                    upload_id        = EXCLUDED.upload_id,
                    duration_seconds = EXCLUDED.duration_seconds,
                    flags            = EXCLUDED.flags
                WHERE EXCLUDED.last_timestamp > ta_testrun_commit_summary.last_timestamp
                """,
                params,
            )


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
    # Query the pre-aggregated summary table instead of scanning raw timeseries rows.
    # lower_bound_timestamp is kept as a parameter for API compatibility but is no
    # longer needed for performance — the summary table has one row per test.
    with connections["ta_timeseries"].cursor() as cursor:
        cursor.execute(
            """
            SELECT
                test_id,
                computed_name,
                failure_message,
                upload_id,
                duration_seconds,
                flags
            FROM ta_testrun_commit_summary
            WHERE repo_id = %s AND commit_sha = %s
              AND outcome IN ('failure', 'flaky_fail')
            ORDER BY computed_name ASC
            """,
            [repo_id, commit_sha],
        )
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
    # Query the pre-aggregated summary table instead of scanning raw timeseries rows.
    with connections["ta_timeseries"].cursor() as cursor:
        cursor.execute(
            """
            SELECT SUM(duration_seconds)
            FROM ta_testrun_commit_summary
            WHERE repo_id = %s AND commit_sha = %s
            """,
            [repo_id, commit_sha],
        )
        result = cursor.fetchone()
        if result is None:
            return None
        return result[0]


def get_pr_comment_agg(
    repo_id: int, commit_sha: str, lower_bound_timestamp: datetime | None = None
) -> PRCommentAgg:
    # Query the pre-aggregated summary table instead of scanning raw timeseries rows
    # with an expensive LAST(outcome, timestamp) GROUP BY test_id aggregate.
    # lower_bound_timestamp is kept for API compatibility but is no longer needed.
    with connections["ta_timeseries"].cursor() as cursor:
        cursor.execute(
            """
            SELECT outcome, count(*)
            FROM ta_testrun_commit_summary
            WHERE repo_id = %s AND commit_sha = %s
            GROUP BY outcome
            """,
            [repo_id, commit_sha],
        )
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
