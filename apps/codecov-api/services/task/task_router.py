import shared.celery_config as shared_celery_config
from codecov_auth.models import Owner
from compare.models import CommitComparison
from core.models import Repository
from shared.celery_router import route_tasks_based_on_user_plan
from shared.plan.constants import DEFAULT_FREE_PLAN


def _get_user_plan_from_ownerid(ownerid, *args, **kwargs) -> str:
    owner = Owner.objects.filter(ownerid=ownerid).first()
    if owner:
        return owner.plan
    return DEFAULT_FREE_PLAN


def _get_user_plan_from_repoid(repoid, *args, **kwargs) -> str:
    repo = Repository.objects.filter(repoid=repoid).first()
    if repo and repo.author:
        return repo.author.plan
    return DEFAULT_FREE_PLAN


def _get_user_plan_from_comparison_id(comparison_id, *args, **kwargs) -> str:
    compare_commit = (
        CommitComparison.objects.filter(id=comparison_id)
        .select_related("compare_commit__repository__author")
        .first()
    )
    if (
        compare_commit
        and compare_commit.compare_commit
        and compare_commit.compare_commit.repository
        and compare_commit.compare_commit.repository.author
    ):
        return compare_commit.compare_commit.repository.author.plan
    return DEFAULT_FREE_PLAN


def _get_user_plan_from_task(task_name: str, task_kwargs: dict) -> str:
    owner_plan_lookup_funcs = {
        # from ownerid
        shared_celery_config.delete_owner_task_name: _get_user_plan_from_ownerid,
        shared_celery_config.sync_repos_task_name: _get_user_plan_from_ownerid,
        shared_celery_config.sync_teams_task_name: _get_user_plan_from_ownerid,
        # from repoid
        shared_celery_config.upload_task_name: _get_user_plan_from_repoid,
        shared_celery_config.notify_task_name: _get_user_plan_from_repoid,
        shared_celery_config.status_set_error_task_name: _get_user_plan_from_repoid,
        shared_celery_config.status_set_pending_task_name: _get_user_plan_from_repoid,
        shared_celery_config.pulls_task_name: _get_user_plan_from_repoid,
        # from comparison_id
        shared_celery_config.compute_comparison_task_name: _get_user_plan_from_comparison_id,
    }
    func_to_use = owner_plan_lookup_funcs.get(
        task_name, lambda *args, **kwargs: DEFAULT_FREE_PLAN
    )
    return func_to_use(**task_kwargs)


def route_task(name, args, kwargs, options={}, task=None, **kw):
    """Function to dynamically route tasks to the proper queue.
    Docs: https://docs.celeryq.dev/en/stable/userguide/routing.html#routers
    """
    user_plan = _get_user_plan_from_task(name, kwargs)
    return route_tasks_based_on_user_plan(name, user_plan)
