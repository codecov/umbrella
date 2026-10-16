import fnmatch
import logging
import re
import threading
import time
from collections import OrderedDict
from collections.abc import Mapping

from django.db import OperationalError, close_old_connections

from shared.celery_config import BaseCeleryConfig, get_task_group
from shared.config import get_config
from shared.django_apps.codecov_auth.models import Plan

log = logging.getLogger(__name__)

Pattern = re.Pattern

# Plans change very rarely, so cache whether a plan is an enterprise plan to
# avoid a DB round-trip (and a fresh connection / DNS lookup) on every
# apply_async call.
PLAN_CACHE_TTL_SECONDS = 300
_plan_cache: dict[str, tuple[bool, float]] = {}
_plan_cache_lock = threading.Lock()


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


def clear_plan_cache() -> None:
    """Clear the in-process plan cache (useful for tests or after plan changes)."""
    with _plan_cache_lock:
        _plan_cache.clear()


def _fetch_is_enterprise_plan(user_plan: str) -> bool:
    return Plan.objects.get(name=user_plan).is_enterprise_plan


def _is_enterprise_plan(task_name: str, user_plan: str) -> bool:
    """Return whether `user_plan` is an enterprise plan, using a TTL cache.

    Transient DB connectivity failures (e.g. DNS resolution of the DB host
    failing) are retried once with a fresh connection; if that also fails,
    a stale cached value is used when available.
    """
    now = time.monotonic()
    with _plan_cache_lock:
        cached = _plan_cache.get(user_plan)
    if cached is not None and now - cached[1] < PLAN_CACHE_TTL_SECONDS:
        return cached[0]

    try:
        try:
            value = _fetch_is_enterprise_plan(user_plan)
        except OperationalError:
            log.warning(
                "Plan lookup failed with OperationalError, retrying with a fresh connection",
                extra={"task_name": task_name, "user_plan": user_plan},
                exc_info=True,
            )
            close_old_connections()
            value = _fetch_is_enterprise_plan(user_plan)
    except OperationalError:
        if cached is not None:
            log.warning(
                "Plan lookup failed, using stale cached plan value",
                extra={"task_name": task_name, "user_plan": user_plan},
                exc_info=True,
            )
            return cached[0]
        raise

    with _plan_cache_lock:
        _plan_cache[user_plan] = (value, time.monotonic())
    return value


def route_tasks_based_on_user_plan(task_name: str, user_plan: str, owner: int) -> dict:
    """Helper function to dynamically route tasks based on the user plan.

    Args:
        task_name: Name of the task to route
        user_plan: Name of the user's plan
        owner: Owner ID for enterprise routing

    Returns:
        Dict containing queue name and any extra configuration
    """
    is_enterprise_plan = _is_enterprise_plan(task_name, user_plan)

    if not is_enterprise_plan:
        return {"queue": _get_default_queue(task_name), "extra_config": {}}

    queue, config = _get_enterprise_config(task_name, owner)
    return {"queue": queue, "extra_config": config}
