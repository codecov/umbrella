from typing import TypedDict

import pytest
from django.utils import timezone

from shared.django_apps.ta_timeseries.models import Testrun
from shared.django_apps.test_analytics.models import Flake, TAUpload
from tasks.detect_flakes import handle_failure, process_single_upload


class TestrunData(TypedDict):
    test_id: str
    outcome: str


class UploadData(TypedDict):
    upload_id: int
    repo_id: int
    testruns: list[TestrunData]


pytestmark = pytest.mark.django_db(databases=["default", "ta_timeseries"])


@pytest.fixture
def setup_test_data(db):
    def _create_test_data(
        repo_id: int,
        upload_id: int,
        testruns: list[TestrunData],
    ):
        timestamp = timezone.now()
        created_testruns = []
        for testrun_data in testruns:
            testrun = Testrun.objects.create(
                timestamp=timestamp,
                test_id=testrun_data["test_id"].encode(),
                outcome=testrun_data["outcome"],
                repo_id=repo_id,
                upload_id=upload_id,
            )
            created_testruns.append(testrun)
        return created_testruns

    return _create_test_data


def test_handle_failure_creates_new_flake():
    curr_flakes = {}
    test_id = b"test1"
    testrun = Testrun(
        timestamp=timezone.now(),
        test_id=test_id,
        outcome="failure",
        repo_id=1,
        upload_id=1,
    )
    repo_id = 1

    changed = handle_failure(curr_flakes, test_id, testrun, repo_id)

    assert changed is True
    assert testrun.outcome == "flaky_fail"
    assert test_id in curr_flakes
    assert curr_flakes[test_id].fail_count == 1
    assert curr_flakes[test_id].count == 1
    assert curr_flakes[test_id].recent_passes_count == 0


def test_handle_failure_updates_existing_flake():
    existing_flake = Flake(
        repoid=1,
        test_id=b"test1",
        count=5,
        fail_count=2,
        recent_passes_count=3,
        start_date=timezone.now(),
    )
    curr_flakes = {b"test1": existing_flake}
    test_id = b"test1"
    testrun = Testrun(
        timestamp=timezone.now(),
        test_id=test_id,
        outcome="error",
        repo_id=1,
        upload_id=1,
    )

    changed = handle_failure(curr_flakes, test_id, testrun, 1)

    assert changed is True
    assert testrun.outcome == "flaky_fail"
    assert existing_flake.fail_count == 3
    assert existing_flake.count == 6
    assert existing_flake.recent_passes_count == 0


def test_handle_failure_already_flaky_no_change():
    existing_flake = Flake(
        repoid=1,
        test_id=b"test1",
        count=5,
        fail_count=2,
        recent_passes_count=0,
        start_date=timezone.now(),
    )
    curr_flakes = {b"test1": existing_flake}
    test_id = b"test1"
    testrun = Testrun(
        timestamp=timezone.now(),
        test_id=test_id,
        outcome="flaky_fail",
        repo_id=1,
        upload_id=1,
    )

    changed = handle_failure(curr_flakes, test_id, testrun, 1)

    assert changed is False
    assert testrun.outcome == "flaky_fail"
    assert existing_flake.fail_count == 3
    assert existing_flake.count == 6


def test_process_single_upload_updates_only_modified(setup_test_data):
    repo_id = 1
    upload_id = 100

    TAUpload.objects.create(id=upload_id, repo_id=repo_id, state="pending")

    testruns = setup_test_data(
        repo_id=repo_id,
        upload_id=upload_id,
        testruns=[
            {"test_id": "test1", "outcome": "failure"},
            {"test_id": "test2", "outcome": "error"},
            {"test_id": "test3", "outcome": "pass"},
            {"test_id": "test4", "outcome": "skip"},
        ],
    )

    curr_flakes = {}
    process_single_upload(upload_id, curr_flakes, repo_id)

    # Verify outcomes were updated in the database
    updated_testruns = {
        bytes(tr.test_id).decode(): tr.outcome
        for tr in Testrun.objects.filter(upload_id=upload_id)
    }

    assert updated_testruns == {
        "test1": "flaky_fail",  # Updated from failure
        "test2": "flaky_fail",  # Updated from error
        "test3": "pass",  # Unchanged
        "test4": "skip",  # Unchanged
    }

    # Verify flakes were created
    assert len(curr_flakes) == 2
    assert b"test1" in curr_flakes
    assert b"test2" in curr_flakes


def test_process_single_upload_no_updates_when_nothing_changed(setup_test_data):
    repo_id = 1
    upload_id = 101

    TAUpload.objects.create(id=upload_id, repo_id=repo_id, state="pending")

    testruns = setup_test_data(
        repo_id=repo_id,
        upload_id=upload_id,
        testruns=[
            {"test_id": "test1", "outcome": "pass"},
            {"test_id": "test2", "outcome": "skip"},
        ],
    )

    curr_flakes = {}
    process_single_upload(upload_id, curr_flakes, repo_id)

    # Verify no outcomes changed
    updated_testruns = {
        bytes(tr.test_id).decode(): tr.outcome
        for tr in Testrun.objects.filter(upload_id=upload_id)
    }

    assert updated_testruns == {
        "test1": "pass",
        "test2": "skip",
    }

    assert len(curr_flakes) == 0


def test_process_single_upload_skips_already_flaky(setup_test_data):
    repo_id = 1
    upload_id = 102

    TAUpload.objects.create(id=upload_id, repo_id=repo_id, state="pending")

    testruns = setup_test_data(
        repo_id=repo_id,
        upload_id=upload_id,
        testruns=[
            {"test_id": "test1", "outcome": "flaky_fail"},
            {"test_id": "test2", "outcome": "failure"},
        ],
    )

    curr_flakes = {}
    process_single_upload(upload_id, curr_flakes, repo_id)

    # Verify outcomes
    updated_testruns = {
        bytes(tr.test_id).decode(): tr.outcome
        for tr in Testrun.objects.filter(upload_id=upload_id)
    }

    assert updated_testruns == {
        "test1": "flaky_fail",  # Already flaky, NOT updated by filter
        "test2": "flaky_fail",  # Updated from failure
    }

    # Both should create flakes in memory
    assert len(curr_flakes) == 2
    # But only test2 was actually updated in DB (test1 was already flaky_fail)
