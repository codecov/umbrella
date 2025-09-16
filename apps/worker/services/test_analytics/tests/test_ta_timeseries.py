from datetime import datetime

import pytest
from freezegun import freeze_time

from services.test_analytics.ta_timeseries import (
    get_pr_comment_agg,
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
