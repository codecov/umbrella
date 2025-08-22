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


@pytest.mark.django_db(databases=["ta_timeseries"])
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
        name="test_name",
        classname="test_classname",
        testsuite="test_suite",
        failure_message=None,
        filename="test_filename",
    )
    assert t.outcome == "pass"


@pytest.mark.django_db(databases=["ta_timeseries"])
def test_pr_comment_agg():
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

    agg = get_pr_comment_agg(1, "commit_sha")
    assert agg == {
        "passed": 1,
        "failed": 0,
        "skipped": 0,
    }


@pytest.mark.django_db(databases=["ta_timeseries"])
def test_pr_comment_failures():
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
                    "outcome": "failure",
                    "testsuite": "test_suite",
                    "failure_message": "failure_message",
                    "filename": "test_filename",
                    "build_url": None,
                }
            ],
        },
    )

    failures = get_pr_comment_failures(1, "commit_sha")
    assert len(failures) == 1
    failure = failures[0]
    assert failure["test_id"] == calc_test_id(
        "test_name", "test_classname", "test_suite"
    )
    assert failure["computed_name"] == "computed_name"
    assert failure["failure_message"] == "failure_message"
    assert failure["duration_seconds"] == 1.0
    assert failure["upload_id"] == 1
    assert failure["flags"] == ["flag1", "flag2"]


@pytest.mark.django_db(databases=["ta_timeseries"])
def test_get_testruns_for_flake_detection(db):
    test_ids = {calc_test_id("flaky_test_name", "test_classname", "test_suite")}
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
                    "outcome": "failure",
                    "testsuite": "test_suite",
                    "failure_message": "failure_message",
                    "filename": "test_filename",
                    "build_url": None,
                },
                {
                    "name": "flaky_test_name",
                    "classname": "test_classname",
                    "computed_name": "computed_name",
                    "duration": 1.0,
                    "outcome": "failure",
                    "testsuite": "test_suite",
                    "failure_message": "failure_message",
                    "filename": "test_filename",
                    "build_url": None,
                },
                {
                    "name": "flaky_test_name",
                    "classname": "test_classname",
                    "computed_name": "computed_name",
                    "duration": 1.0,
                    "outcome": "pass",
                    "testsuite": "test_suite",
                    "failure_message": "failure_message",
                    "filename": "test_filename",
                    "build_url": None,
                },
                {
                    "name": "test_name",
                    "classname": "test_classname",
                    "computed_name": "computed_name",
                    "duration": 1.0,
                    "outcome": "pass",
                    "testsuite": "test_suite",
                    "failure_message": "failure_message",
                    "filename": "test_filename",
                    "build_url": None,
                },
            ],
        },
        flaky_test_ids=test_ids,
    )

    testruns = get_testruns_for_flake_detection(1, test_ids)
    assert len(testruns) == 3
    assert testruns[0].outcome == "failure"
    assert testruns[0].failure_message == "failure_message"
    assert testruns[0].name == "test_name"
    assert testruns[1].outcome == "flaky_fail"
    assert testruns[1].failure_message == "failure_message"
    assert testruns[1].name == "flaky_test_name"
    assert testruns[2].outcome == "pass"
    assert testruns[2].failure_message == "failure_message"
    assert testruns[2].name == "flaky_test_name"


@pytest.mark.django_db(databases=["ta_timeseries"])
@freeze_time("2025-01-01")
def test_update_testrun_to_flaky():
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
                    "outcome": "failure",
                    "testsuite": "test_suite",
                    "failure_message": "failure_message",
                    "filename": "test_filename",
                    "build_url": None,
                },
            ],
        },
    )
    update_testrun_to_flaky(
        datetime.now(),
        calc_test_id("test_name", "test_classname", "test_suite"),
    )
    testrun = Testrun.objects.get(
        name="test_name",
        classname="test_classname",
        testsuite="test_suite",
    )
    assert testrun.outcome == "flaky_fail"
