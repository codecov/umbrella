import fnmatch
import re
from collections import OrderedDict
from collections.abc import Mapping

from shared.celery_config import BaseCeleryConfig, get_task_group
from shared.config import get_config
from shared.django_apps.codecov_auth.models import Plan

Pattern = re.Pattern


# based on code from https://github.com/celery/celery/blob/main/celery/app/routes.py
class MapRoute:
    def __init__(self, map):
        map = map.items() if isinstance(map, Mapping) else map
        self.map = {}
        self.patterns = OrderedDict()
        for k, v in map:
            if isinstance(k, Pattern):
                self.patterns[k] = v
            elif "*" in k:
                self.patterns[re.compile(fnmatch.translate(k))] = v
            else:
                self.map[k] = v

    def __call__(self, name, *args, **kwargs):
        try:
            return dict(self.map[name])
        except KeyError:
            pass
        except ValueError:
            return {"queue": self.map[name]}
        for regex, route in self.patterns.items():
            if regex.match(name):
                try:
                    return dict(route)
                except ValueError:
                    return {"queue": route}


def _get_default_queue(task_name: str) -> str:
    """Get the default queue for a task based on routing configuration."""
    route = MapRoute(BaseCeleryConfig.task_routes)
    return (route(task_name) or {"queue": BaseCeleryConfig.task_default_queue})["queue"]


def _get_enterprise_config(task_name: str, owner: int) -> tuple[str, dict]:
    """Get enterprise-specific queue name and configuration."""
    owner_specific_config = get_config(
        "setup", "tasks", "enterprise_queues", default={}
    )
    default_enterprise_config = get_config(
        "setup", "tasks", "celery", "enterprise", default={}
    )
    queue_specific_config = get_config(
        "setup",
        "tasks",
        get_task_group(task_name),
        "enterprise",
        default=default_enterprise_config,
    )

    base_queue = f"enterprise_{_get_default_queue(task_name)}"
    if str(owner) in owner_specific_config:
        base_queue = f"{base_queue}_{owner_specific_config[str(owner)]}"

    return base_queue, queue_specific_config


def route_tasks_based_on_user_plan(task_name: str, user_plan: str, owner: int) -> dict:
    """Helper function to dynamically route tasks based on the user plan.

    Args:
        task_name: Name of the task to route
        user_plan: Name of the user's plan
        owner: Owner ID for enterprise routing

    Returns:
        Dict containing queue name and any extra configuration
    """
    plan = Plan.objects.get(name=user_plan)

    if not plan.is_enterprise_plan:
        return {"queue": _get_default_queue(task_name), "extra_config": {}}

    queue, config = _get_enterprise_config(task_name, owner)
    return {"queue": queue, "extra_config": config}
