# http://docs.celeryq.org/en/latest/configuration.html#configuration

from shared.config import get_config
from shared.utils.enums import TaskConfigGroup

# Task name follows the following convention:
# task_name |- app.<type>.<config_group>.<identifier>
# <type> can be "tasks" or "cron"
# <config_group> is the task's TaskConfigGroup
# <identifier> is the task name (usually same as task class)

# Miscellaneous tasks
http_request_task_name = f"app.tasks.{TaskConfigGroup.http_request.value}.HTTPRequest"
ai_pr_review_task_name = f"app.tasks.{TaskConfigGroup.ai_pr_review.value}.AiPrReview"
transplant_report_task_name = (
    f"app.tasks.{TaskConfigGroup.reports.value}.transplant_report"
)

# Bundle analysis tasks
bundle_analysis_notify_task_name = (
    f"app.tasks.{TaskConfigGroup.bundle_analysis.value}.BundleAnalysisNotify"
)
bundle_analysis_processor_task_name = (
    f"app.tasks.{TaskConfigGroup.bundle_analysis.value}.BundleAnalysisProcessor"
)
bundle_analysis_save_measurements_task_name = (
    f"app.tasks.{TaskConfigGroup.bundle_analysis.value}.BundleAnalysisSaveMeasurements"
)

# Sync tasks
sync_teams_task_name = f"app.tasks.{TaskConfigGroup.sync_teams.value}.SyncTeams"
sync_repos_task_name = f"app.tasks.{TaskConfigGroup.sync_repos.value}.SyncRepos"
sync_repo_languages_task_name = (
    f"app.tasks.{TaskConfigGroup.sync_repo_languages.value}.SyncLanguages"
)
sync_repo_languages_gql_task_name = (
    f"app.tasks.{TaskConfigGroup.sync_repo_languages_gql.value}.SyncLanguagesGQL"
)

delete_owner_task_name = f"app.tasks.{TaskConfigGroup.delete_owner.value}.DeleteOwner"
process_owners_to_be_deleted_cron_task_name = (
    f"app.tasks.{TaskConfigGroup.delete_owner.value}.ProcessOwnersToBeDeletedCron"
)

# Export owner data tasks
export_owner_task_name = f"app.tasks.{TaskConfigGroup.export_owner.value}.ExportOwner"
export_owner_sql_task_name = (
    f"app.tasks.{TaskConfigGroup.export_owner.value}.ExportOwnerSQL"
)
export_owner_archives_task_name = (
    f"app.tasks.{TaskConfigGroup.export_owner.value}.ExportOwnerArchives"
)
export_owner_finalize_task_name = (
    f"app.tasks.{TaskConfigGroup.export_owner.value}.ExportOwnerFinalize"
)

mark_owner_for_deletion_task_name = (
    f"app.tasks.{TaskConfigGroup.mark_owner_for_deletion.value}.MarkOwnerForDeletion"
)
activate_account_user_task_name = (
    f"app.tasks.{TaskConfigGroup.sync_account.value}.ActivateAccountUser"
)
notify_task_name = f"app.tasks.{TaskConfigGroup.notify.value}.Notify"
pulls_task_name = f"app.tasks.{TaskConfigGroup.pulls.value}.Sync"
status_set_error_task_name = f"app.tasks.{TaskConfigGroup.status.value}.SetError"


# Upload tasks
upload_breadcrumb_task_name = (
    f"app.tasks.{TaskConfigGroup.upload.value}.UploadBreadcrumb"
)
pre_process_upload_task_name = (
    f"app.tasks.{TaskConfigGroup.upload.value}.PreProcessUpload"
)
upload_task_name = f"app.tasks.{TaskConfigGroup.upload.value}.Upload"
upload_processor_task_name = f"app.tasks.{TaskConfigGroup.upload.value}.UploadProcessor"
upload_finisher_task_name = f"app.tasks.{TaskConfigGroup.upload.value}.UploadFinisher"
parallel_verification_task_name = (
    f"app.tasks.{TaskConfigGroup.upload.value}.ParallelVerification"
)

# Test results tasks
test_results_processor_task_name = (
    f"app.tasks.{TaskConfigGroup.test_results.value}.TestResultsProcessor"
)

test_results_finisher_task_name = (
    f"app.tasks.{TaskConfigGroup.test_results.value}.TestResultsFinisherTask"
)

test_analytics_notifier_task_name = (
    f"app.tasks.{TaskConfigGroup.test_results.value}.TestAnalyticsNotifier"
)

sync_test_results_task_name = (
    f"app.tasks.{TaskConfigGroup.test_results.value}.SyncTestResultsTask"
)

ingest_testruns_task_name = (
    f"app.tasks.{TaskConfigGroup.test_results.value}.IngestTestruns"
)

detect_flakes_task_name = f"app.tasks.{TaskConfigGroup.flakes.value}.DetectFlakes"

# Cache rollup tasks
cache_test_rollups_task_name = (
    f"app.tasks.{TaskConfigGroup.cache_rollup.value}.CacheTestRollupsTask"
)

cache_test_rollups_redis_task_name = (
    f"app.tasks.{TaskConfigGroup.cache_rollup.value}.CacheTestRollupsRedisTask"
)

process_flakes_task_name = f"app.tasks.{TaskConfigGroup.flakes.value}.ProcessFlakesTask"

manual_upload_completion_trigger_task_name = (
    f"app.tasks.{TaskConfigGroup.upload.value}.ManualUploadCompletionTrigger"
)
comment_task_name = f"app.tasks.{TaskConfigGroup.comment.value}.Comment"
flush_repo_task_name = f"app.tasks.{TaskConfigGroup.flush_repo.value}.FlushRepo"
ghm_sync_plans_task_name = f"app.tasks.{TaskConfigGroup.sync_plans.value}.SyncPlans"
send_email_task_name = f"app.tasks.{TaskConfigGroup.send_email.value}.SendEmail"
new_user_activated_task_name = (
    f"app.tasks.{TaskConfigGroup.new_user_activated.value}.NewUserActivated"
)

# Compute comparison tasks
compute_comparison_task_name = (
    f"app.tasks.{TaskConfigGroup.compute_comparison.value}.ComputeComparison"
)
compute_component_comparison_task_name = (
    f"app.tasks.{TaskConfigGroup.compute_comparison.value}.ComputeComponentComparison"
)

commit_update_task_name = (
    f"app.tasks.{TaskConfigGroup.commit_update.value}.CommitUpdate"
)

# Profiling tasks
profiling_finding_task_name = (
    f"app.cron.{TaskConfigGroup.profiling.value}.findinguncollected"
)
profiling_summarization_task_name = (
    f"app.tasks.{TaskConfigGroup.profiling.value}.summarization"
)
profiling_collection_task_name = (
    f"app.tasks.{TaskConfigGroup.profiling.value}.collection"
)
profiling_normalization_task_name = (
    f"app.tasks.{TaskConfigGroup.profiling.value}.normalizer"
)

# Timeseries tasks
timeseries_backfill_task_name = f"app.tasks.{TaskConfigGroup.timeseries.value}.backfill"
timeseries_backfill_dataset_task_name = (
    f"app.tasks.{TaskConfigGroup.timeseries.value}.backfill_dataset"
)
timeseries_backfill_commits_task_name = (
    f"app.tasks.{TaskConfigGroup.timeseries.value}.backfill_commits"
)
timeseries_delete_task_name = f"app.tasks.{TaskConfigGroup.timeseries.value}.delete"
timeseries_save_commit_measurements_task_name = (
    f"app.tasks.{TaskConfigGroup.timeseries.value}.save_commit_measurements"
)
timeseries_upsert_component_task_name = (
    f"app.tasks.{TaskConfigGroup.timeseries.value}.UpsertComponentTask"
)

health_check_task_name = f"app.cron.{TaskConfigGroup.healthcheck.value}.HealthCheckTask"

# Daily cron tasks
gh_app_webhook_check_task_name = (
    f"app.cron.{TaskConfigGroup.daily.value}.GitHubAppWebhooksCheckTask"
)
brolly_stats_rollup_task_name = (
    f"app.cron.{TaskConfigGroup.daily.value}.BrollyStatsRollupTask"
)
partition_management_task_name = (
    f"app.cron.{TaskConfigGroup.daily.value}.PartitionManagementTask"
)


def get_task_group(task_name: str) -> str | None:
    task_parts = task_name.split(".")
    if len(task_parts) != 4:
        return None
    return task_parts[2]


# =============================================================================
# Task Resilience Configuration
# =============================================================================
# These settings improve task resilience to pod failures and network issues.
# All values can be overridden via environment variables or YAML config.
#
# References:
# - Celery Reliability: https://docs.celeryq.dev/en/stable/userguide/tasks.html#task-request
# - Redis Broker Options: https://docs.celeryq.dev/en/stable/getting-started/backends-and-brokers/redis.html#configuration
# =============================================================================

# Visibility timeout for Redis broker when acks_late=False (seconds)
# Used when tasks acknowledge immediately upon receipt, allowing faster recovery
# from worker failures. Tasks are removed from queue immediately, so shorter timeout
# is acceptable for faster task recovery.
# Default: 300 seconds (5 minutes)
TASK_VISIBILITY_TIMEOUT_WITHOUT_ACKS_LATE_SECONDS = int(
    get_config(
        "setup", "tasks", "celery", "visibility_timeout_without_acks_late", default=300
    )
)

# Visibility timeout for Redis broker when acks_late=True (seconds)
# Must be longer than the longest-running task that uses acks_late=True.
# Tasks like delete_owner can take up to 48 minutes, so visibility timeout should
# be set accordingly to prevent duplicate task execution.
# Default: 900 seconds (15 minutes)
# Note: Some tasks explicitly set acks_late=True, so this timeout must accommodate them.
TASK_VISIBILITY_TIMEOUT_WITH_ACKS_LATE_SECONDS = int(
    get_config(
        "setup", "tasks", "celery", "visibility_timeout_with_acks_late", default=900
    )
)

# Visibility timeout for Redis broker (seconds)
# How long a task remains invisible after being pulled from queue before becoming
# visible again if not acknowledged. This is the maximum time a task can be "lost"
# before another worker picks it up.
# Automatically selects appropriate timeout based on global task_acks_late setting,
# but defaults to the longer timeout to protect tasks that explicitly set acks_late=True.
# Can be overridden directly via: setup.tasks.celery.visibility_timeout config
# Celery docs: https://docs.celeryq.dev/en/stable/getting-started/backends-and-brokers/redis.html#visibility-timeout
_global_acks_late = bool(get_config("setup", "tasks", "celery", "acks_late"))
_default_visibility_timeout = (
    TASK_VISIBILITY_TIMEOUT_WITH_ACKS_LATE_SECONDS
    if _global_acks_late
    else TASK_VISIBILITY_TIMEOUT_WITHOUT_ACKS_LATE_SECONDS
)
TASK_VISIBILITY_TIMEOUT_SECONDS = int(
    get_config(
        "setup",
        "tasks",
        "celery",
        "visibility_timeout",
        default=max(
            _default_visibility_timeout,
            TASK_VISIBILITY_TIMEOUT_WITH_ACKS_LATE_SECONDS,
        ),
    )
)

# max_retries = maximum total attempts (stop when attempts >= this). Config key name matches Celery.
# Celery docs: https://docs.celeryq.dev/en/stable/userguide/tasks.html#max-retries
TASK_MAX_RETRIES_DEFAULT = int(
    get_config("setup", "tasks", "celery", "max_retries", default=10)
)

# Base delay for exponential backoff retry strategy (seconds)
# Each retry waits: base_delay * (2 ** retry_count)
# Example: 20s, 40s, 80s, 160s, 320s...
# Default: 20 seconds
# Celery docs: https://docs.celeryq.dev/en/stable/userguide/tasks.html#retrying
TASK_RETRY_BACKOFF_BASE_SECONDS = int(
    get_config("setup", "tasks", "celery", "retry_backoff_base", default=20)
)

# Fixed retry delay for specific conditions (seconds)
# Used for predictable retry intervals (e.g., waiting for processing lock)
# Default: 60 seconds
TASK_RETRY_FIXED_DELAY_SECONDS = int(
    get_config("setup", "tasks", "celery", "retry_fixed_delay", default=60)
)

# Upload queue TTL (seconds)
# How long pending uploads remain in Redis before expiring
# Default: 86400 seconds (24 hours)
UPLOAD_QUEUE_TTL_SECONDS = int(
    get_config("setup", "tasks", "upload", "queue_ttl_seconds", default=86400)
)

# Upload processing lock retry delay (seconds)
# When another task is processing uploads, how long to wait before retrying
# Default: 60 seconds
UPLOAD_PROCESSING_RETRY_DELAY_SECONDS = int(
    get_config("setup", "tasks", "upload", "processing_retry_delay", default=60)
)

# Upload processing lock max retries
# How many times to retry when processing lock is held before giving up
# Default: 10 retries (= 10 minutes with 60s delay)
UPLOAD_PROCESSING_MAX_RETRIES = int(
    get_config("setup", "tasks", "upload", "processing_max_retries", default=10)
)

# Preprocess upload max retries
# How many times to retry when preprocess upload lock cannot be acquired
# Default: 10 retries
PREPROCESS_UPLOAD_MAX_RETRIES = int(
    get_config("setup", "tasks", "upload", "preprocess_max_retries", default=10)
)

# Upload processor max retries
# How many times to retry when upload processor encounters retryable errors
# Default: 5 retries
UPLOAD_PROCESSOR_MAX_RETRIES = int(
    get_config("setup", "tasks", "upload", "processor_max_retries", default=5)
)

# Bundle analysis processor max_retries (max total attempts)
# Default: matches TASK_MAX_RETRIES_DEFAULT
BUNDLE_ANALYSIS_PROCESSOR_MAX_RETRIES = int(
    get_config(
        "setup",
        "tasks",
        "bundle_analysis",
        "processor_max_retries",
        default=TASK_MAX_RETRIES_DEFAULT,
    )
)

# Bundle analysis notify max retries
# How many times to retry when bundle analysis notify lock cannot be acquired
# Default: matches TASK_MAX_RETRIES_DEFAULT
BUNDLE_ANALYSIS_NOTIFY_MAX_RETRIES = int(
    get_config(
        "setup",
        "tasks",
        "bundle_analysis",
        "notify_max_retries",
        default=TASK_MAX_RETRIES_DEFAULT,
    )
)

# Default timeout for Redis locks used by LockManager
DEFAULT_LOCK_TIMEOUT_SECONDS = int(
    get_config("setup", "tasks", "lock_timeout", default=300)
)

# Default time to wait when acquiring a Redis lock before giving up
DEFAULT_BLOCKING_TIMEOUT_SECONDS = int(
    get_config("setup", "tasks", "blocking_timeout", default=5)
)


class BaseCeleryConfig:
    broker_url = get_config("services", "celery_broker") or get_config(
        "services", "redis_url"
    )
    result_backend = get_config("services", "celery_broker") or get_config(
        "services", "redis_url"
    )

    # Task visibility timeout for broker transport
    # Uses TASK_VISIBILITY_TIMEOUT_SECONDS constant
    # Can be overridden via: setup.tasks.celery.visibility_timeout config
    broker_transport_options = {
        "visibility_timeout": TASK_VISIBILITY_TIMEOUT_SECONDS,
    }
    result_extended = True
    task_default_queue = get_config(
        "setup", "tasks", "celery", "default_queue", default="celery"
    )
    health_check_default_queue = "healthcheck"

    # Import jobs
    imports = ("tasks",)

    task_serializer = "json"

    accept_content = ["json"]

    worker_max_memory_per_child = int(
        get_config(
            "setup", "tasks", "celery", "worker_max_memory_per_child", default=1500000
        )
    )  # 1.5GB

    # https://docs.celeryq.dev/en/stable/userguide/configuration.html#std-setting-worker_soft_shutdown_timeout
    worker_soft_shutdown_timeout = int(
        get_config(
            "setup", "tasks", "celery", "worker_soft_shutdown_timeout", default=60
        )  # 60 seconds
    )

    # http://docs.celeryproject.org/en/latest/configuration.html?highlight=celery_redirect_stdouts#celeryd-hijack-root-logger
    worker_hijack_root_logger = False

    timezone = "UTC"
    enable_utc = True

    # http://docs.celeryproject.org/en/latest/configuration.html#celery-ignore-result
    task_ignore_result = True

    # http://celery.readthedocs.org/en/latest/userguide/tasks.html#disable-rate-limits-if-they-re-not-used
    worker_disable_rate_limits = True

    # http://celery.readthedocs.org/en/latest/faq.html#should-i-use-retry-or-acks-late
    task_acks_late = bool(get_config("setup", "tasks", "celery", "acks_late"))

    # Reject tasks if the worker is lost (due to pod death, OOM, etc)
    # This immediately returns the task to the queue for another worker to pick up,
    # preventing task loss when pods crash or are terminated.
    # IMPORTANT: Requires task_acks_late=True to work effectively.
    # Default: True (enabled for pod failure resilience)
    # Can be overridden via: setup.tasks.celery.reject_on_worker_lost config
    # Celery docs: https://docs.celeryq.dev/en/stable/userguide/configuration.html#task-reject-on-worker-lost
    task_reject_on_worker_lost = bool(
        get_config("setup", "tasks", "celery", "reject_on_worker_lost", default=False)
    )

    # http://celery.readthedocs.org/en/latest/userguide/optimizing.html#prefetch-limits
    worker_prefetch_multiplier = int(
        get_config("setup", "tasks", "celery", "prefetch", default=1)
    )
    # !!! NEVER 0 !!! 0 == infinite

    # http://celery.readthedocs.org/en/latest/configuration.html#celeryd-task-soft-time-limit
    task_soft_time_limit = int(
        get_config("setup", "tasks", "celery", "soft_timelimit", default=600)
    )

    # http://celery.readthedocs.org/en/latest/configuration.html#std:setting-CELERYD_TASK_TIME_LIMIT
    task_time_limit = int(
        get_config("setup", "tasks", "celery", "hard_timelimit", default=720)
    )

    notify_soft_time_limit = int(
        get_config(
            "setup", "tasks", TaskConfigGroup.notify.value, "timeout", default=120
        )
    )
    timeseries_soft_time_limit = get_config(
        "setup",
        "tasks",
        TaskConfigGroup.timeseries.value,
        "soft_timelimit",
        default=400,
    )
    timeseries_hard_time_limit = get_config(
        "setup",
        "tasks",
        TaskConfigGroup.timeseries.value,
        "hard_timelimit",
        default=480,
    )

    gh_webhook_retry_soft_time_limit = get_config(
        "setup", "tasks", TaskConfigGroup.daily.value, "soft_timelimit", default=600
    )

    gh_webhook_retry_hard_time_limit = get_config(
        "setup", "tasks", TaskConfigGroup.daily.value, "hard_timelimit", default=680
    )

    task_annotations = {
        delete_owner_task_name: {
            "soft_time_limit": 4 * task_soft_time_limit,
            "time_limit": 4 * task_time_limit,
        },
        mark_owner_for_deletion_task_name: {
            "soft_time_limit": 2 * task_soft_time_limit,
            "time_limit": 2 * task_time_limit,
        },
        notify_task_name: {
            "soft_time_limit": notify_soft_time_limit,
            "time_limit": notify_soft_time_limit + 20,
        },
        sync_repos_task_name: {
            "soft_time_limit": 2 * task_soft_time_limit,
            "time_limit": 2 * task_time_limit,
        },
        timeseries_backfill_dataset_task_name: {
            "soft_time_limit": timeseries_soft_time_limit,
            "time_limit": timeseries_hard_time_limit,
        },
        timeseries_backfill_commits_task_name: {
            "soft_time_limit": timeseries_soft_time_limit,
            "time_limit": timeseries_hard_time_limit,
        },
        timeseries_save_commit_measurements_task_name: {
            "soft_time_limit": timeseries_soft_time_limit,
            "time_limit": timeseries_hard_time_limit,
        },
        gh_app_webhook_check_task_name: {
            "soft_time_limit": gh_webhook_retry_soft_time_limit,
            "time_limit": gh_webhook_retry_hard_time_limit,
        },
    }

    # Get the upload queue for backward-compatible fallback
    # This ensures upload_finisher continues to use the upload queue if no dedicated queue is configured
    _upload_queue = get_config(
        "setup",
        "tasks",
        TaskConfigGroup.upload.value,
        "queue",
        default=task_default_queue,
    )

    task_routes = {
        sync_teams_task_name: {
            "queue": get_config(
                "setup",
                "tasks",
                TaskConfigGroup.sync_teams.value,
                "queue",
                default=task_default_queue,
            )
        },
        sync_repos_task_name: {
            "queue": get_config(
                "setup",
                "tasks",
                TaskConfigGroup.sync_repos.value,
                "queue",
                default=task_default_queue,
            )
        },
        sync_repo_languages_task_name: {
            "queue": get_config(
                "setup",
                "tasks",
                TaskConfigGroup.sync_repo_languages.value,
                "queue",
                default=task_default_queue,
            )
        },
        sync_repo_languages_gql_task_name: {
            "queue": get_config(
                "setup",
                "tasks",
                TaskConfigGroup.sync_repo_languages_gql.value,
                "queue",
                default=task_default_queue,
            )
        },
        delete_owner_task_name: {
            "queue": get_config(
                "setup",
                "tasks",
                TaskConfigGroup.delete_owner.value,
                "queue",
                default=task_default_queue,
            )
        },
        mark_owner_for_deletion_task_name: {
            "queue": get_config(
                "setup",
                "tasks",
                TaskConfigGroup.mark_owner_for_deletion.value,
                "queue",
                default=task_default_queue,
            )
        },
        f"app.tasks.{TaskConfigGroup.export_owner.value}.*": {
            "queue": get_config(
                "setup",
                "tasks",
                TaskConfigGroup.export_owner.value,
                "queue",
                default=task_default_queue,
            )
        },
        activate_account_user_task_name: {
            "queue": get_config(
                "setup",
                "tasks",
                TaskConfigGroup.sync_account.value,
                "queue",
                default=task_default_queue,
            )
        },
        notify_task_name: {
            "queue": get_config(
                "setup",
                "tasks",
                TaskConfigGroup.notify.value,
                "queue",
                default=task_default_queue,
            )
        },
        pulls_task_name: {
            "queue": get_config(
                "setup",
                "tasks",
                TaskConfigGroup.pulls.value,
                "queue",
                default=task_default_queue,
            )
        },
        f"app.tasks.{TaskConfigGroup.status.value}.*": {
            "queue": get_config(
                "setup",
                "tasks",
                TaskConfigGroup.status.value,
                "queue",
                default=task_default_queue,
            )
        },
        # UploadFinisher gets its own queue to avoid being blocked by UploadProcessor tasks.
        # Falls back to the upload queue if no dedicated queue is configured (backward-compatible).
        upload_finisher_task_name: {
            "queue": get_config(
                "setup",
                "tasks",
                "upload_finisher",
                "queue",
                default=_upload_queue,
            )
        },
        # All other upload tasks (Upload, UploadProcessor, PreProcessUpload, etc.)
        f"app.tasks.{TaskConfigGroup.upload.value}.*": {
            "queue": get_config(
                "setup",
                "tasks",
                TaskConfigGroup.upload.value,
                "queue",
                default=task_default_queue,
            )
        },
        f"app.tasks.{TaskConfigGroup.test_results.value}.*": {
            "queue": get_config(
                "setup",
                "tasks",
                TaskConfigGroup.test_results.value,
                "queue",
                default=task_default_queue,
            )
        },
        f"app.tasks.{TaskConfigGroup.flakes.value}.*": {
            "queue": get_config(
                "setup",
                "tasks",
                TaskConfigGroup.flakes.value,
                "queue",
                default=task_default_queue,
            )
        },
        f"app.tasks.{TaskConfigGroup.cache_rollup.value}.*": {
            "queue": get_config(
                "setup",
                "tasks",
                TaskConfigGroup.cache_rollup.value,
                "queue",
                default=task_default_queue,
            )
        },
        f"app.tasks.{TaskConfigGroup.archive.value}.*": {
            "queue": get_config(
                "setup",
                "tasks",
                TaskConfigGroup.archive.value,
                "queue",
                default=task_default_queue,
            )
        },
        f"app.tasks.{TaskConfigGroup.bundle_analysis.value}.*": {
            "queue": get_config(
                "setup",
                "tasks",
                TaskConfigGroup.bundle_analysis.value,
                "queue",
                default=task_default_queue,
            )
        },
        comment_task_name: {
            "queue": get_config(
                "setup",
                "tasks",
                TaskConfigGroup.comment.value,
                "queue",
                default=task_default_queue,
            )
        },
        flush_repo_task_name: {
            "queue": get_config(
                "setup",
                "tasks",
                TaskConfigGroup.flush_repo.value,
                "queue",
                default=task_default_queue,
            )
        },
        ghm_sync_plans_task_name: {
            "queue": get_config(
                "setup",
                "tasks",
                TaskConfigGroup.sync_plans.value,
                "queue",
                default=task_default_queue,
            )
        },
        new_user_activated_task_name: {
            "queue": get_config(
                "setup",
                "tasks",
                TaskConfigGroup.new_user_activated.value,
                "queue",
                default=task_default_queue,
            )
        },
        f"app.tasks.{TaskConfigGroup.profiling.value}.*": {
            "queue": get_config(
                "setup",
                "tasks",
                TaskConfigGroup.profiling.value,
                "queue",
                default=task_default_queue,
            )
        },
        f"app.cron.{TaskConfigGroup.profiling.value}.*": {
            "queue": get_config(
                "setup",
                "tasks",
                TaskConfigGroup.profiling.value,
                "queue",
                default=task_default_queue,
            )
        },
        commit_update_task_name: {
            "queue": get_config(
                "setup",
                "tasks",
                TaskConfigGroup.commit_update.value,
                "queue",
                default=task_default_queue,
            )
        },
        f"app.tasks.{TaskConfigGroup.timeseries.value}.*": {
            "queue": get_config(
                "setup",
                "tasks",
                TaskConfigGroup.timeseries.value,
                "queue",
                default=task_default_queue,
            )
        },
        compute_comparison_task_name: {
            "queue": get_config(
                "setup",
                "tasks",
                TaskConfigGroup.compute_comparison.value,
                "queue",
                default=task_default_queue,
            )
        },
        health_check_task_name: {
            "queue": health_check_default_queue,
        },
    }
