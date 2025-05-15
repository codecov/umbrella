import pytest

import shared.celery_config as shared_celery_config
from compare.tests.factories import CommitComparisonFactory
from services.task.task_router import (
    _get_ownerid_from_comparison_id,
    _get_ownerid_from_ownerid,
    _get_ownerid_from_repoid,
    _get_ownerid_from_task,
    _get_user_plan_from_comparison_id,
    _get_user_plan_from_ownerid,
    _get_user_plan_from_repoid,
    _get_user_plan_from_task,
    route_task,
)
from shared.django_apps.core.tests.factories import OwnerFactory, RepositoryFactory
from shared.plan.constants import DEFAULT_FREE_PLAN, PlanName


@pytest.fixture
def fake_owners(db):
    owner = OwnerFactory.create(plan=PlanName.CODECOV_PRO_MONTHLY.value)
    owner_enterprise_cloud = OwnerFactory.create(
        plan=PlanName.ENTERPRISE_CLOUD_YEARLY.value
    )
    owner.save()
    owner_enterprise_cloud.save()
    return (owner, owner_enterprise_cloud)


@pytest.fixture
def fake_repos(db, fake_owners):
    (owner, owner_enterprise_cloud) = fake_owners

    repo = RepositoryFactory(author=owner)
    repo_enterprise = RepositoryFactory(author=owner_enterprise_cloud)
    repo.save()
    repo_enterprise.save()
    return (repo, repo_enterprise)


@pytest.fixture
def fake_compare_commit(db, fake_repos):
    (repo, repo_enterprise) = fake_repos
    compare_commit = CommitComparisonFactory(compare_commit__repository=repo)
    compare_commit_enterprise = CommitComparisonFactory(
        compare_commit__repository=repo_enterprise
    )
    compare_commit.save()
    compare_commit_enterprise.save()
    return (compare_commit, compare_commit_enterprise, repo, repo_enterprise)


def test_get_owner_plan_from_ownerid(fake_owners):
    (owner, owner_enterprise_cloud) = fake_owners
    assert (
        _get_user_plan_from_ownerid(owner.ownerid) == PlanName.CODECOV_PRO_MONTHLY.value
    )
    assert (
        _get_user_plan_from_ownerid(owner_enterprise_cloud.ownerid)
        == PlanName.ENTERPRISE_CLOUD_YEARLY.value
    )
    assert _get_user_plan_from_ownerid(10000000) == DEFAULT_FREE_PLAN


def test_get_owner_from_ownerid(fake_owners):
    (owner, owner_enterprise_cloud) = fake_owners
    assert _get_ownerid_from_ownerid(owner.ownerid) == owner.ownerid
    assert (
        _get_ownerid_from_ownerid(owner_enterprise_cloud.ownerid)
        == owner_enterprise_cloud.ownerid
    )


def test_get_owner_plan_from_repoid(fake_repos):
    (repo, repo_enterprise) = fake_repos
    assert _get_user_plan_from_repoid(repo.repoid) == PlanName.CODECOV_PRO_MONTHLY.value
    assert (
        _get_user_plan_from_repoid(repo_enterprise.repoid)
        == PlanName.ENTERPRISE_CLOUD_YEARLY.value
    )
    assert _get_user_plan_from_repoid(10000000) == DEFAULT_FREE_PLAN


def test_get_owner_from_repoid(fake_repos):
    (repo, repo_enterprise) = fake_repos
    assert _get_ownerid_from_repoid(repo.repoid) == repo.author.ownerid
    assert (
        _get_ownerid_from_repoid(repo_enterprise.repoid)
        == repo_enterprise.author.ownerid
    )
    assert _get_ownerid_from_repoid(10000000) is None


def test_get_user_plan_from_comparison_id(fake_compare_commit):
    (compare_commit, compare_commit_enterprise, repo, repo_enterprise) = (
        fake_compare_commit
    )
    assert (
        _get_user_plan_from_comparison_id(compare_commit.id)
        == PlanName.CODECOV_PRO_MONTHLY.value
    )
    assert (
        _get_user_plan_from_comparison_id(compare_commit_enterprise.id)
        == PlanName.ENTERPRISE_CLOUD_YEARLY.value
    )
    assert _get_user_plan_from_comparison_id(10000000) == DEFAULT_FREE_PLAN


def test_get_owner_from_comparison_id(fake_compare_commit):
    (compare_commit, compare_commit_enterprise, repo, repo_enterprise) = (
        fake_compare_commit
    )
    assert _get_ownerid_from_comparison_id(compare_commit.id) == repo.author.ownerid
    assert (
        _get_ownerid_from_comparison_id(compare_commit_enterprise.id)
        == repo_enterprise.author.ownerid
    )
    assert _get_ownerid_from_comparison_id(10000000) is None


def test_get_user_plan_from_task(
    fake_repos,
    fake_compare_commit,
):
    (repo, repo_enterprise_cloud) = fake_repos
    compare_commit = fake_compare_commit[0]
    task_kwargs = {
        "repoid": repo.repoid,
        "commitid": 0,
        "debug": False,
        "rebuild": False,
    }
    assert (
        _get_user_plan_from_task(shared_celery_config.upload_task_name, task_kwargs)
        == PlanName.CODECOV_PRO_MONTHLY.value
    )

    task_kwargs = {
        "repoid": repo_enterprise_cloud.repoid,
        "commitid": 0,
        "debug": False,
        "rebuild": False,
    }
    assert (
        _get_user_plan_from_task(shared_celery_config.upload_task_name, task_kwargs)
        == PlanName.ENTERPRISE_CLOUD_YEARLY.value
    )

    task_kwargs = {"ownerid": repo.author.ownerid}
    assert (
        _get_user_plan_from_task(
            shared_celery_config.delete_owner_task_name, task_kwargs
        )
        == PlanName.CODECOV_PRO_MONTHLY.value
    )

    task_kwargs = {"comparison_id": compare_commit.id}
    assert (
        _get_user_plan_from_task(
            shared_celery_config.compute_comparison_task_name, task_kwargs
        )
        == PlanName.CODECOV_PRO_MONTHLY.value
    )

    task_kwargs = {
        "repoid": repo_enterprise_cloud.repoid,
        "commitid": 0,
        "debug": False,
        "rebuild": False,
    }
    assert _get_user_plan_from_task("unknown task", task_kwargs) == DEFAULT_FREE_PLAN


def test_get_ownerid_from_task(
    fake_repos,
    fake_compare_commit,
):
    (repo, repo_enterprise_cloud) = fake_repos
    compare_commit = fake_compare_commit[0]
    task_kwargs = {
        "repoid": repo.repoid,
        "commitid": 0,
        "debug": False,
        "rebuild": False,
    }
    assert (
        _get_ownerid_from_task(shared_celery_config.upload_task_name, task_kwargs)
        == repo.author.ownerid
    )

    task_kwargs = {
        "repoid": repo_enterprise_cloud.repoid,
        "commitid": 0,
        "debug": False,
        "rebuild": False,
    }
    assert (
        _get_ownerid_from_task(shared_celery_config.upload_task_name, task_kwargs)
        == repo_enterprise_cloud.author.ownerid
    )

    task_kwargs = {"ownerid": repo.author.ownerid}
    assert (
        _get_ownerid_from_task(shared_celery_config.delete_owner_task_name, task_kwargs)
        == repo.author.ownerid
    )

    task_kwargs = {"comparison_id": compare_commit.id}
    assert (
        _get_ownerid_from_task(
            shared_celery_config.compute_comparison_task_name, task_kwargs
        )
        == repo.author.ownerid
    )

    task_kwargs = {
        "repoid": repo_enterprise_cloud.repoid,
        "commitid": 0,
        "debug": False,
        "rebuild": False,
    }
    assert _get_ownerid_from_task("unknown task", task_kwargs) is None


def test_route_task(mocker, fake_repos):
    mock_route_tasks_shared = mocker.patch(
        "services.task.task_router.route_tasks_based_on_user_plan"
    )
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
    mock_route_tasks_shared.assert_called_with(
        shared_celery_config.upload_task_name,
        PlanName.CODECOV_PRO_MONTHLY.value,
        repo.author.ownerid,
    )
