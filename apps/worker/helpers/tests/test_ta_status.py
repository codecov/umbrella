import pytest
from django.test import override_settings

from database.tests.factories import CommitFactory, RepositoryFactory
from helpers.ta_status import get_test_status
from shared.django_apps.ta_timeseries.tests.factories import TestrunFactory


@pytest.fixture
def setup_testruns(dbsession):
    def _setup(*outcomes):
        repository = RepositoryFactory.create()
        dbsession.add(repository)
        dbsession.flush()

        commit = CommitFactory.create(
            repository=repository, commitid="test_commit_123", branch="main"
        )
        dbsession.add(commit)
        dbsession.flush()

        for outcome in outcomes:
            TestrunFactory.create(
                repo_id=repository.repoid,
                commit_sha=commit.commitid,
                outcome=outcome,
            )

        return repository, commit

    return _setup


@pytest.mark.django_db(databases=["default", "ta_timeseries"])
def test_get_test_status_with_failures_no_passes(dbsession, setup_testruns):
    repository, commit = setup_testruns("failure", "failure")

    any_failures, all_passed = get_test_status(repository.repoid, commit.commitid)

    assert any_failures is True
    assert all_passed is False


@pytest.mark.django_db(databases=["default", "ta_timeseries"])
def test_get_test_status_with_passes_no_failures(dbsession, setup_testruns):
    repository, commit = setup_testruns("pass", "pass")

    any_failures, all_passed = get_test_status(repository.repoid, commit.commitid)

    assert any_failures is False
    assert all_passed is True


@pytest.mark.django_db(databases=["default", "ta_timeseries"])
def test_get_test_status_with_passes_and_failures(dbsession, setup_testruns):
    repository, commit = setup_testruns("pass", "failure")

    any_failures, all_passed = get_test_status(repository.repoid, commit.commitid)

    assert any_failures is True
    assert all_passed is False


@pytest.mark.django_db(databases=["default", "ta_timeseries"])
def test_get_test_status_no_tests(dbsession, setup_testruns):
    repository, commit = setup_testruns()

    any_failures, all_passed = get_test_status(repository.repoid, commit.commitid)

    assert any_failures is False
    assert all_passed is False


@pytest.mark.django_db(databases=["default", "ta_timeseries"])
@override_settings(TA_TIMESERIES_ENABLED=False)
def test_get_test_status_when_ta_timeseries_disabled(dbsession, setup_testruns):
    repository, commit = setup_testruns("failure", "pass")

    any_failures, all_passed = get_test_status(repository.repoid, commit.commitid)

    assert any_failures is False
    assert all_passed is False
