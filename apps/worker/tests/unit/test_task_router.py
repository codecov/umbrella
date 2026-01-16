import pytest

import shared.celery_config as shared_celery_config
from celery_task_router import (
    _get_ownerid_from_comparison_id,
    _get_ownerid_from_ownerid,
    _get_ownerid_from_repoid,
    _get_ownerid_from_task,
    _get_user_plan_from_comparison_id,
    _get_user_plan_from_org_ownerid,
    _get_user_plan_from_ownerid,
    _get_user_plan_from_repoid,
    _get_user_plan_from_task,
    route_task,
)
from database.tests.factories.core import (
    CommitFactory,
    CompareCommitFactory,
    OwnerFactory,
    RepositoryFactory,
)
from shared.plan.constants import DEFAULT_FREE_PLAN, PlanName
from tests.helpers import mock_all_plans_and_tiers


@pytest.fixture
def fake_owners(dbsession):
    owner = OwnerFactory.create(plan=PlanName.CODECOV_PRO_MONTHLY.value)
    owner_enterprise_cloud = OwnerFactory.create(
        plan=PlanName.ENTERPRISE_CLOUD_YEARLY.value
    )
    dbsession.add(owner)
    dbsession.add(owner_enterprise_cloud)
    dbsession.flush()
    return (owner, owner_enterprise_cloud)


@pytest.fixture
def fake_repos(dbsession, fake_owners):
    (owner, owner_enterprise_cloud) = fake_owners
    repo = RepositoryFactory.create(author=owner)
    repo_enterprise_cloud = RepositoryFactory.create(author=owner_enterprise_cloud)
    dbsession.add(repo)
    dbsession.add(repo_enterprise_cloud)
    dbsession.flush()
    return (repo, repo_enterprise_cloud)


@pytest.fixture
def fake_comparison_commit(dbsession, fake_repos):
    (repo, repo_enterprise_cloud) = fake_repos

    commmit = CommitFactory.create(repository=repo)
    commmit_enterprise = CommitFactory.create(repository=repo_enterprise_cloud)
    dbsession.add(commmit)
    dbsession.add(commmit_enterprise)
    dbsession.flush()
    compare_commit = CompareCommitFactory(compare_commit=commmit)
    compare_commit_enterprise = CompareCommitFactory(compare_commit=commmit_enterprise)
    dbsession.add(compare_commit)
    dbsession.add(compare_commit_enterprise)
    dbsession.flush()
    return (compare_commit, compare_commit_enterprise, repo, repo_enterprise_cloud)


def test_get_ownerid_from_ownerid(dbsession, fake_owners):
    (owner, owner_enterprise_cloud) = fake_owners
    assert _get_ownerid_from_ownerid(dbsession, owner.ownerid) == owner.ownerid
    assert (
        _get_ownerid_from_ownerid(dbsession, owner_enterprise_cloud.ownerid)
        == owner_enterprise_cloud.ownerid
    )


def test_get_owner_plan_from_id(dbsession, fake_owners):
    (owner, owner_enterprise_cloud) = fake_owners
    assert (
        _get_user_plan_from_ownerid(dbsession, owner.ownerid)
        == PlanName.CODECOV_PRO_MONTHLY.value
    )
    assert (
        _get_user_plan_from_ownerid(dbsession, owner_enterprise_cloud.ownerid)
        == PlanName.ENTERPRISE_CLOUD_YEARLY.value
    )
    assert _get_user_plan_from_ownerid(dbsession, 10000000) == DEFAULT_FREE_PLAN


def test_get_user_plan_from_org_ownerid(dbsession, fake_owners):
    (owner, owner_enterprise_cloud) = fake_owners
    assert (
        _get_user_plan_from_org_ownerid(dbsession, owner.ownerid)
        == PlanName.CODECOV_PRO_MONTHLY.value
    )
    assert (
        _get_user_plan_from_org_ownerid(dbsession, owner_enterprise_cloud.ownerid)
        == PlanName.ENTERPRISE_CLOUD_YEARLY.value
    )


def test_get_owner_plan_from_repoid(dbsession, fake_repos):
    (repo, repo_enterprise_cloud) = fake_repos
    assert (
        _get_user_plan_from_repoid(dbsession, repo.repoid)
        == PlanName.CODECOV_PRO_MONTHLY.value
    )
    assert (
        _get_user_plan_from_repoid(dbsession, repo_enterprise_cloud.repoid)
        == PlanName.ENTERPRISE_CLOUD_YEARLY.value
    )
    assert _get_user_plan_from_repoid(dbsession, 10000000) == DEFAULT_FREE_PLAN


def test_get_ownerid_from_repoid(dbsession, fake_repos):
    (repo, repo_enterprise_cloud) = fake_repos
    assert _get_ownerid_from_repoid(dbsession, repo.repoid) == repo.ownerid
    assert (
        _get_ownerid_from_repoid(dbsession, repo_enterprise_cloud.repoid)
        == repo_enterprise_cloud.ownerid
    )
    assert _get_ownerid_from_repoid(dbsession, 10000000) is None


def test_get_user_plan_from_comparison_id(dbsession, fake_comparison_commit):
    (compare_commit, compare_commit_enterprise, repo, repo_enterprise_cloud) = (
        fake_comparison_commit
    )
    assert (
        _get_user_plan_from_comparison_id(dbsession, comparison_id=compare_commit.id)
        == PlanName.CODECOV_PRO_MONTHLY.value
    )
    assert (
        _get_user_plan_from_comparison_id(
            dbsession, comparison_id=compare_commit_enterprise.id
        )
        == PlanName.ENTERPRISE_CLOUD_YEARLY.value
    )
    assert _get_user_plan_from_comparison_id(dbsession, 10000000) == DEFAULT_FREE_PLAN


def test_get_ownerid_from_comparison_id(dbsession, fake_comparison_commit):
    (compare_commit, compare_commit_enterprise, repo, repo_enterprise_cloud) = (
        fake_comparison_commit
    )
    assert (
        _get_ownerid_from_comparison_id(dbsession, comparison_id=compare_commit.id)
        == repo.ownerid
    )
    assert (
        _get_ownerid_from_comparison_id(
            dbsession, comparison_id=compare_commit_enterprise.id
        )
        == repo_enterprise_cloud.ownerid
    )
    assert _get_ownerid_from_comparison_id(dbsession, 10000000) is None


def test_get_user_plan_from_task(
    dbsession,
    fake_repos,
    fake_comparison_commit,
):
    (repo, repo_enterprise_cloud) = fake_repos
    compare_commit = fake_comparison_commit[0]
    task_kwargs = {
        "repoid": repo.repoid,
        "commitid": 0,
        "debug": False,
        "rebuild": False,
    }
    assert (
        _get_user_plan_from_task(
            dbsession, shared_celery_config.upload_task_name, task_kwargs
        )
        == PlanName.CODECOV_PRO_MONTHLY.value
    )

    task_kwargs = {
        "repoid": repo_enterprise_cloud.repoid,
        "commitid": 0,
        "debug": False,
        "rebuild": False,
    }
    assert (
        _get_user_plan_from_task(
            dbsession, shared_celery_config.upload_task_name, task_kwargs
        )
        == PlanName.ENTERPRISE_CLOUD_YEARLY.value
    )

    task_kwargs = {"ownerid": repo.ownerid}
    assert (
        _get_user_plan_from_task(
            dbsession, shared_celery_config.delete_owner_task_name, task_kwargs
        )
        == PlanName.CODECOV_PRO_MONTHLY.value
    )

    task_kwargs = {"org_ownerid": repo.ownerid, "user_ownerid": 20}
    assert (
        _get_user_plan_from_task(
            dbsession, shared_celery_config.new_user_activated_task_name, task_kwargs
        )
        == PlanName.CODECOV_PRO_MONTHLY.value
    )

    task_kwargs = {"comparison_id": compare_commit.id}
    assert (
        _get_user_plan_from_task(
            dbsession, shared_celery_config.compute_comparison_task_name, task_kwargs
        )
        == PlanName.CODECOV_PRO_MONTHLY.value
    )

    task_kwargs = {
        "repoid": repo_enterprise_cloud.repoid,
        "commitid": 0,
        "debug": False,
        "rebuild": False,
    }
    assert (
        _get_user_plan_from_task(dbsession, "unknown task", task_kwargs)
        == DEFAULT_FREE_PLAN
    )


def test_get_ownerid_from_task(
    dbsession,
    fake_repos,
    fake_comparison_commit,
):
    (repo, repo_enterprise_cloud) = fake_repos
    compare_commit = fake_comparison_commit[0]
    task_kwargs = {
        "repoid": repo.repoid,
        "commitid": 0,
        "debug": False,
        "rebuild": False,
    }
    assert (
        _get_ownerid_from_task(
            dbsession, shared_celery_config.upload_task_name, task_kwargs
        )
        == repo.ownerid
    )

    task_kwargs = {
        "repoid": repo_enterprise_cloud.repoid,
        "commitid": 0,
        "debug": False,
        "rebuild": False,
    }
    assert (
        _get_ownerid_from_task(
            dbsession, shared_celery_config.upload_task_name, task_kwargs
        )
        == repo_enterprise_cloud.ownerid
    )

    task_kwargs = {"ownerid": repo.ownerid}
    assert (
        _get_ownerid_from_task(
            dbsession, shared_celery_config.delete_owner_task_name, task_kwargs
        )
        == repo.ownerid
    )

    task_kwargs = {"org_ownerid": repo.ownerid, "user_ownerid": 20}
    assert (
        _get_ownerid_from_task(
            dbsession, shared_celery_config.new_user_activated_task_name, task_kwargs
        )
        == repo.ownerid
    )

    task_kwargs = {"comparison_id": compare_commit.id}
    assert (
        _get_ownerid_from_task(
            dbsession, shared_celery_config.compute_comparison_task_name, task_kwargs
        )
        == repo.ownerid
    )

    task_kwargs = {
        "repoid": repo_enterprise_cloud.repoid,
        "commitid": 0,
        "debug": False,
        "rebuild": False,
    }
    assert _get_ownerid_from_task(dbsession, "unknown task", task_kwargs) is None


def test_route_task(mocker, dbsession, fake_repos):
    mock_get_db_session = mocker.patch("celery_task_router.get_db_session")
    mock_route_tasks_shared = mocker.patch(
        "celery_task_router.route_tasks_based_on_user_plan"
    )
    mock_get_db_session.return_value = dbsession
    mock_route_tasks_shared.return_value = {"queue": "correct queue"}
    repo = fake_repos[0]
    task_kwargs = {
        "repoid": repo.repoid,
        "commitid": 0,
        "debug": False,
        "rebuild": False,
    }
    response = route_task(shared_celery_config.upload_task_name, [], task_kwargs, {})
    assert response == {"queue": "correct queue"}
    mock_get_db_session.assert_called()
    mock_route_tasks_shared.assert_called_with(
        shared_celery_config.upload_task_name,
        PlanName.CODECOV_PRO_MONTHLY.value,
        repo.ownerid,
    )


class TestBundleAnalysisTaskRouting:
    """Tests for bundle analysis task routing to enterprise queues."""

    def test_get_user_plan_from_task_bundle_analysis_processor(
        self, dbsession, fake_repos
    ):
        """Test that bundle_analysis_processor_task routes based on repoid."""
        (repo, repo_enterprise_cloud) = fake_repos

        # Regular plan repo
        task_kwargs = {"repoid": repo.repoid, "commitid": "abc123"}
        assert (
            _get_user_plan_from_task(
                dbsession,
                shared_celery_config.bundle_analysis_processor_task_name,
                task_kwargs,
            )
            == PlanName.CODECOV_PRO_MONTHLY.value
        )

        # Enterprise plan repo
        task_kwargs = {"repoid": repo_enterprise_cloud.repoid, "commitid": "abc123"}
        assert (
            _get_user_plan_from_task(
                dbsession,
                shared_celery_config.bundle_analysis_processor_task_name,
                task_kwargs,
            )
            == PlanName.ENTERPRISE_CLOUD_YEARLY.value
        )

    def test_get_user_plan_from_task_bundle_analysis_notify(
        self, dbsession, fake_repos
    ):
        """Test that bundle_analysis_notify_task routes based on repoid."""
        (repo, repo_enterprise_cloud) = fake_repos

        # Regular plan repo
        task_kwargs = {"repoid": repo.repoid, "commitid": "abc123"}
        assert (
            _get_user_plan_from_task(
                dbsession,
                shared_celery_config.bundle_analysis_notify_task_name,
                task_kwargs,
            )
            == PlanName.CODECOV_PRO_MONTHLY.value
        )

        # Enterprise plan repo
        task_kwargs = {"repoid": repo_enterprise_cloud.repoid, "commitid": "abc123"}
        assert (
            _get_user_plan_from_task(
                dbsession,
                shared_celery_config.bundle_analysis_notify_task_name,
                task_kwargs,
            )
            == PlanName.ENTERPRISE_CLOUD_YEARLY.value
        )

    def test_get_user_plan_from_task_bundle_analysis_save_measurements(
        self, dbsession, fake_repos
    ):
        """Test that bundle_analysis_save_measurements_task routes based on repoid."""
        (repo, repo_enterprise_cloud) = fake_repos

        # Regular plan repo
        task_kwargs = {"repoid": repo.repoid, "commitid": "abc123"}
        assert (
            _get_user_plan_from_task(
                dbsession,
                shared_celery_config.bundle_analysis_save_measurements_task_name,
                task_kwargs,
            )
            == PlanName.CODECOV_PRO_MONTHLY.value
        )

        # Enterprise plan repo
        task_kwargs = {"repoid": repo_enterprise_cloud.repoid, "commitid": "abc123"}
        assert (
            _get_user_plan_from_task(
                dbsession,
                shared_celery_config.bundle_analysis_save_measurements_task_name,
                task_kwargs,
            )
            == PlanName.ENTERPRISE_CLOUD_YEARLY.value
        )

    def test_get_ownerid_from_task_bundle_analysis_processor(
        self, dbsession, fake_repos
    ):
        """Test that bundle_analysis_processor_task gets ownerid from repoid."""
        (repo, repo_enterprise_cloud) = fake_repos

        task_kwargs = {"repoid": repo.repoid, "commitid": "abc123"}
        assert (
            _get_ownerid_from_task(
                dbsession,
                shared_celery_config.bundle_analysis_processor_task_name,
                task_kwargs,
            )
            == repo.ownerid
        )

        task_kwargs = {"repoid": repo_enterprise_cloud.repoid, "commitid": "abc123"}
        assert (
            _get_ownerid_from_task(
                dbsession,
                shared_celery_config.bundle_analysis_processor_task_name,
                task_kwargs,
            )
            == repo_enterprise_cloud.ownerid
        )

    def test_get_ownerid_from_task_bundle_analysis_notify(self, dbsession, fake_repos):
        """Test that bundle_analysis_notify_task gets ownerid from repoid."""
        (repo, repo_enterprise_cloud) = fake_repos

        task_kwargs = {"repoid": repo.repoid, "commitid": "abc123"}
        assert (
            _get_ownerid_from_task(
                dbsession,
                shared_celery_config.bundle_analysis_notify_task_name,
                task_kwargs,
            )
            == repo.ownerid
        )

    def test_get_ownerid_from_task_bundle_analysis_save_measurements(
        self, dbsession, fake_repos
    ):
        """Test that bundle_analysis_save_measurements_task gets ownerid from repoid."""
        (repo, repo_enterprise_cloud) = fake_repos

        task_kwargs = {"repoid": repo.repoid, "commitid": "abc123"}
        assert (
            _get_ownerid_from_task(
                dbsession,
                shared_celery_config.bundle_analysis_save_measurements_task_name,
                task_kwargs,
            )
            == repo.ownerid
        )


@pytest.mark.django_db(databases={"default"})
class TestBundleAnalysisRoutingIntegration:
    """End-to-end integration tests for bundle analysis task routing.

    These tests verify the complete routing flow WITHOUT mocking
    route_tasks_based_on_user_plan, ensuring that bundle analysis tasks
    are correctly routed to enterprise queues for enterprise customers.
    """

    @pytest.fixture
    def setup_plans(self):
        """Populate the Plan model with all plan definitions."""
        mock_all_plans_and_tiers()

    def test_bundle_analysis_processor_routes_to_enterprise_queue(
        self, mocker, dbsession, fake_repos, setup_plans
    ):
        """Integration test: enterprise repo bundle_analysis_processor → enterprise queue."""
        mocker.patch("celery_task_router.get_db_session", return_value=dbsession)
        (repo, repo_enterprise_cloud) = fake_repos

        task_kwargs = {"repoid": repo_enterprise_cloud.repoid, "commitid": "abc123"}
        result = route_task(
            shared_celery_config.bundle_analysis_processor_task_name,
            [],
            task_kwargs,
            {},
        )

        assert "enterprise" in result["queue"], (
            f"Expected enterprise queue for enterprise repo, got: {result['queue']}"
        )

    def test_bundle_analysis_notify_routes_to_enterprise_queue(
        self, mocker, dbsession, fake_repos, setup_plans
    ):
        """Integration test: enterprise repo bundle_analysis_notify → enterprise queue."""
        mocker.patch("celery_task_router.get_db_session", return_value=dbsession)
        (repo, repo_enterprise_cloud) = fake_repos

        task_kwargs = {"repoid": repo_enterprise_cloud.repoid, "commitid": "abc123"}
        result = route_task(
            shared_celery_config.bundle_analysis_notify_task_name,
            [],
            task_kwargs,
            {},
        )

        assert "enterprise" in result["queue"], (
            f"Expected enterprise queue for enterprise repo, got: {result['queue']}"
        )

    def test_bundle_analysis_save_measurements_routes_to_enterprise_queue(
        self, mocker, dbsession, fake_repos, setup_plans
    ):
        """Integration test: enterprise repo bundle_analysis_save_measurements → enterprise queue."""
        mocker.patch("celery_task_router.get_db_session", return_value=dbsession)
        (repo, repo_enterprise_cloud) = fake_repos

        task_kwargs = {"repoid": repo_enterprise_cloud.repoid, "commitid": "abc123"}
        result = route_task(
            shared_celery_config.bundle_analysis_save_measurements_task_name,
            [],
            task_kwargs,
            {},
        )

        assert "enterprise" in result["queue"], (
            f"Expected enterprise queue for enterprise repo, got: {result['queue']}"
        )

    def test_bundle_analysis_non_enterprise_routes_to_default_queue(
        self, mocker, dbsession, fake_repos, setup_plans
    ):
        """Integration test: non-enterprise repo → default queue (not enterprise)."""
        mocker.patch("celery_task_router.get_db_session", return_value=dbsession)
        (repo, repo_enterprise_cloud) = fake_repos

        task_kwargs = {"repoid": repo.repoid, "commitid": "abc123"}
        result = route_task(
            shared_celery_config.bundle_analysis_processor_task_name,
            [],
            task_kwargs,
            {},
        )

        assert "enterprise" not in result["queue"], (
            f"Expected non-enterprise queue for non-enterprise repo, got: {result['queue']}"
        )
