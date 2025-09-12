from datetime import datetime, timedelta

import pytest
from freezegun import freeze_time

from services.test_analytics.ta_timeseries import (
    get_pr_comment_agg,
    get_pr_comment_duration,
    get_pr_comment_failures,
    get_testruns_for_flake_detection,
    insert_testrun,
    update_testrun_to_flaky,
)
from shared.django_apps.ta_timeseries.models import (
    Testrun,
    calc_test_id,
)
from shared.django_apps.ta_timeseries.tests.factories import (
    TestrunFactory,
)

pytestmark = pytest.mark.django_db(databases=["ta_timeseries"])


def test_insert_testrun():
    insert_testrun(
        timestamp=datetime.now(),
        repo_id=1,
        commit_sha="commit_sha",
        branch="branch",
        upload_id=1,
        flags=["flag1", "flag2"],
        parsing_info={
            "framework": "Pytest",
            "testruns": [
                {
                    "name": "test_name",
                    "classname": "test_classname",
                    "computed_name": "computed_name",
                    "duration": 1.0,
                    "outcome": "pass",
                    "testsuite": "test_suite",
                    "failure_message": None,
                    "filename": "test_filename",
                    "build_url": None,
                }
            ],
        },
    )

    t = Testrun.objects.get(
        test_id=calc_test_id("test_name", "test_classname", "test_suite")
    )
    assert t.outcome == "pass"


def test_pr_comment_agg():
    TestrunFactory.create(commit_sha="agg_commit")
    agg = get_pr_comment_agg(1, "agg_commit")
    assert agg == {"passed": 1, "failed": 0, "skipped": 0}


def test_pr_comment_failures():
    TestrunFactory.create(
        commit_sha="failure_commit",
        outcome="failure",
        failure_message="test failed",
        flags=["flag1", "flag2"],
    )

    failures = get_pr_comment_failures(1, "failure_commit")
    assert len(failures) == 1
    failure = failures[0]
    assert failure["failure_message"] == "test failed"
    assert failure["upload_id"] == 1
    assert failure["flags"] == ["flag1", "flag2"]


def test_get_testruns_for_flake_detection(db):
    test_ids = {calc_test_id("flaky_test", "TestClass", "test_suite")}

    TestrunFactory.create(outcome="failure")
    TestrunFactory.create(outcome="flaky_fail")
    TestrunFactory.create(
        outcome="pass", test_id=calc_test_id("flaky_test", "TestClass", "test_suite")
    )
    TestrunFactory.create(outcome="pass")

    testruns = get_testruns_for_flake_detection(1, test_ids)
    assert len(testruns) == 3
    assert testruns[0].outcome == "failure"
    assert testruns[1].outcome == "flaky_fail"
    assert testruns[2].outcome == "pass"


@freeze_time("2025-01-01")
def test_update_testrun_to_flaky():
    testrun = TestrunFactory.create(
        outcome="failure",
    )
    update_testrun_to_flaky(
        testrun.timestamp,
        testrun.test_id,
    )
    testrun = Testrun.objects.get(test_id=testrun.test_id)
    assert testrun.outcome == "flaky_fail"


def test_get_pr_comment_duration_no_testruns():
    duration = get_pr_comment_duration(1, "nonexistent_commit")
    assert duration is None


def test_get_pr_comment_duration_single_testrun():
    testrun = TestrunFactory.create(duration_seconds=5.5)
    duration = get_pr_comment_duration(1, testrun.commit_sha)
    assert duration == 5.5


def test_get_pr_comment_duration_multiple_testruns_same_test():
    base_time = datetime.now()

    testrun1 = TestrunFactory.create(
        timestamp=base_time, commit_sha="same_commit", duration_seconds=10.0
    )
    testrun2 = TestrunFactory.create(
        timestamp=base_time.replace(second=base_time.second + 1),
        commit_sha="same_commit",
        duration_seconds=15.0,
    )

    duration = get_pr_comment_duration(1, "same_commit")
    assert duration == 25.0


def test_get_pr_comment_duration_multiple_different_tests():
    TestrunFactory.create(
        commit_sha="multi_test_commit",
        duration_seconds=10.0,
    )
    TestrunFactory.create(
        commit_sha="multi_test_commit",
        duration_seconds=20.0,
    )

    duration = get_pr_comment_duration(1, "multi_test_commit")
    assert duration == 30.0


def test_get_pr_comment_duration_with_null_duration():
    TestrunFactory.create(commit_sha="commit_sha", duration_seconds=None)

    duration = get_pr_comment_duration(1, "commit_sha")
    assert duration is None


def test_get_pr_comment_duration_different_repo_commit():
    TestrunFactory.create(commit_sha="commit_a", duration_seconds=10.0)
    TestrunFactory.create(repo_id=2, commit_sha="commit_a", duration_seconds=20.0)
    TestrunFactory.create(commit_sha="commit_b", duration_seconds=30.0)

    assert get_pr_comment_duration(1, "commit_a") == 10.0
    assert get_pr_comment_duration(2, "commit_a") == 20.0
    assert get_pr_comment_duration(1, "commit_b") == 30.0


@pytest.mark.django_db(databases=["ta_timeseries"])
def test_get_pr_comment_duration_with_timestamp_filter():
    base_time = datetime.now()

    TestrunFactory.create(
        timestamp=base_time - timedelta(hours=1),
        commit_sha="duration_commit",
        duration_seconds=10.0,
    )

    TestrunFactory.create(
        timestamp=base_time,
        commit_sha="duration_commit",
        duration_seconds=20.0,
    )

    duration_all = get_pr_comment_duration(1, "duration_commit")
    assert duration_all == 30.0

    duration_filtered = get_pr_comment_duration(1, "duration_commit", base_time)
    assert duration_filtered == 20.0


@pytest.mark.django_db(databases=["ta_timeseries"])
def test_pr_comment_agg_with_timestamp_filter():
    base_time = datetime.now()

    TestrunFactory.create(
        timestamp=base_time - timedelta(hours=1),
        commit_sha="commit_sha",
        outcome="pass",
    )

    TestrunFactory.create(
        timestamp=base_time,
        commit_sha="commit_sha",
        outcome="pass",
    )

    agg_all = get_pr_comment_agg(1, "commit_sha")
    assert agg_all["passed"] == 2

    agg_filtered = get_pr_comment_agg(1, "commit_sha", base_time)
    assert agg_filtered["passed"] == 1


@pytest.mark.django_db(databases=["ta_timeseries"])
def test_pr_comment_failures_with_timestamp_filter():
    base_time = datetime.now()

    TestrunFactory.create(
        timestamp=base_time - timedelta(hours=1),
        commit_sha="commit_sha",
        outcome="failure",
        failure_message="old failure message",
        computed_name="old_computed_name",
    )

    TestrunFactory.create(
        timestamp=base_time,
        commit_sha="commit_sha",
        outcome="failure",
        failure_message="new failure message",
        computed_name="new_computed_name",
    )

    failures_all = get_pr_comment_failures(1, "commit_sha")
    assert len(failures_all) == 2

    failures_filtered = get_pr_comment_failures(1, "commit_sha", base_time)
    assert len(failures_filtered) == 1
    assert failures_filtered[0]["computed_name"] == "new_computed_name"
    assert failures_filtered[0]["failure_message"] == "new failure message"
