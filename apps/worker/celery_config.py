# https://docs.celeryq.dev/en/stable/userguide/configuration.html
import gc
import logging
from datetime import timedelta

from celery import signals
from celery.beat import BeatLazyFunc
from celery.schedules import crontab

from celery_task_router import route_task
from helpers.clock import get_utc_now_as_iso_format
from helpers.health_check import get_health_check_interval_seconds
from shared.celery_config import (
    BaseCeleryConfig,
    brolly_stats_rollup_task_name,
    gh_app_webhook_check_task_name,
    health_check_task_name,
    partition_management_task_name,
    process_owners_to_be_deleted_cron_task_name,
)
from shared.config import get_config
from shared.helpers.cache import RedisBackend, cache
from shared.helpers.redis import get_redis_connection

log = logging.getLogger(__name__)


@signals.worker_before_create_process.connect
def prefork_gc_freeze(**kwargs) -> None:
    """Freeze GC to reduce memory usage in worker subprocesses.

    Reference: https://github.com/getsentry/sentry/pull/63001
    """
    gc.freeze()


@signals.setup_logging.connect
def initialize_logging(loglevel=logging.INFO, **kwargs):
    """Initialize Celery logging configuration."""
    celery_logger = logging.getLogger("celery")
    celery_logger.setLevel(loglevel)
    log.info("Initialized celery logging")
    return celery_logger


@signals.worker_process_init.connect
def initialize_cache(**kwargs):
    """Initialize Redis cache backend for worker processes."""
    log.info("Initialized cache")
    redis_cache_backend = RedisBackend(get_redis_connection())
    cache.configure(redis_cache_backend)


# Cron task names
hourly_check_task_name = "app.cron.hourly_check.HourlyCheckTask"
daily_plan_manager_task_name = "app.cron.daily.PlanManagerTask"
regular_cleanup_cron_task_name = "app.cron.cleanup.RegularCleanup"
cache_rollup_cron_task_name = "app.cron.cache_rollup.CacheRollupTask"
trial_expiration_cron_task_name = "app.cron.plan.TrialExpirationCronTask"

# Regular task names
notify_error_task_name = "app.tasks.notify.NotifyErrorTask"
trial_expiration_task_name = "app.tasks.plan.TrialExpirationTask"

# GitHub App backfill task names
backfill_existing_gh_app_installations_name = "app.tasks.backfill_existing_gh_app_installations.BackfillExistingGHAppInstallationsTask"
backfill_existing_individual_gh_app_installation_name = "app.tasks.backfill_existing_individual_gh_app_installation.BackfillExistingIndividualGHAppInstallationTask"
backfill_owners_without_gh_app_installations_name = "app.tasks.backfill_owners_without_gh_app_installations.BackfillOwnersWithoutGHAppInstallationsTask"
backfill_owners_without_gh_app_installation_individual_name = "app.tasks.backfill_owners_without_gh_app_installation_individual.BackfillOwnersWithoutGHAppInstallationIndividualTask"


def _beat_schedule():
    """Build Celery beat schedule configuration for periodic tasks."""
    beat_schedule = {
        "hourly_check": {
            "task": hourly_check_task_name,
            "schedule": crontab(minute="0"),
            "kwargs": {
                "cron_task_generation_time_iso": BeatLazyFunc(get_utc_now_as_iso_format)
            },
        },
        "github_app_webhooks_task": {
            "task": gh_app_webhook_check_task_name,
            "schedule": crontab(minute="0", hour="0,6,12,18"),  # Every 6 hours
            "kwargs": {
                "cron_task_generation_time_iso": BeatLazyFunc(get_utc_now_as_iso_format)
            },
        },
        "trial_expiration_cron": {
            "task": trial_expiration_cron_task_name,
            "schedule": crontab(minute="0", hour="4"),  # 4 AM UTC (12 AM EDT)
            "kwargs": {
                "cron_task_generation_time_iso": BeatLazyFunc(get_utc_now_as_iso_format)
            },
        },
        "regular_cleanup": {
            "task": regular_cleanup_cron_task_name,
            "schedule": crontab(minute="0", hour="4"),  # 4 AM UTC (12 AM EDT)
            "kwargs": {
                "cron_task_generation_time_iso": BeatLazyFunc(get_utc_now_as_iso_format)
            },
        },
        "partition_management": {
            "task": partition_management_task_name,
            "schedule": crontab(minute="0", hour="5"),  # 5 AM UTC
            "kwargs": {
                "cron_task_generation_time_iso": BeatLazyFunc(get_utc_now_as_iso_format)
            },
        },
        "process_owners_to_be_deleted": {
            "task": process_owners_to_be_deleted_cron_task_name,
            "schedule": crontab(minute="*/15"),  # Every 15 minutes
            "kwargs": {
                "cron_task_generation_time_iso": BeatLazyFunc(get_utc_now_as_iso_format)
            },
        },
    }

    if get_config("setup", "health_check", "enabled", default=False):
        beat_schedule["health_check_task"] = {
            "task": health_check_task_name,
            "schedule": timedelta(seconds=get_health_check_interval_seconds()),
            "kwargs": {
                "cron_task_generation_time_iso": BeatLazyFunc(get_utc_now_as_iso_format)
            },
        }

    if get_config("setup", "telemetry", "enabled", default=True):
        beat_schedule["brolly_stats_rollup"] = {
            "task": brolly_stats_rollup_task_name,
            "schedule": crontab(minute="0", hour="2"),
            "kwargs": {
                "cron_task_generation_time_iso": BeatLazyFunc(get_utc_now_as_iso_format)
            },
        }

    if get_config("setup", "cache_rollup", "enabled", default=False):
        beat_schedule["cache_rollup"] = {
            "task": cache_rollup_cron_task_name,
            "schedule": crontab(minute="0", hour="2"),
            "kwargs": {
                "cron_task_generation_time_iso": BeatLazyFunc(get_utc_now_as_iso_format)
            },
        }

    return beat_schedule


class CeleryWorkerConfig(BaseCeleryConfig):
    """Celery worker configuration with beat schedule and task routing."""

    beat_schedule = _beat_schedule()
    task_routes = route_task
