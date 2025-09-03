from enum import Enum


class CodecovDatabaseEnum(Enum):
    @classmethod
    def choices(cls):
        return tuple((i.db_id, i.name) for i in cls)

    @classmethod
    def enum_from_int(cls, value):
        for elem in cls:
            if elem.db_id == value:
                return elem
        return None


class TaskConfigGroup(Enum):
    """
    Configuration Group for tasks.
    Marks the config key in the install yaml that affects a given task.
    """

    ai_pr_review = "ai_pr_review"
    archive = "archive"
    bundle_analysis = "bundle_analysis"
    cache_rollup = "cache_rollup"
    comment = "comment"
    commit_update = "commit_update"
    compute_comparison = "compute_comparison"
    daily = "daily"
    delete_owner = "delete_owner"
    flakes = "flakes"
    flush_repo = "flush_repo"
    healthcheck = "healthcheck"
    http_request = "http_request"
    mark_owner_for_deletion = "mark_owner_for_deletion"
    new_user_activated = "new_user_activated"
    notify = "notify"
    profiling = "profiling"
    pulls = "pulls"
    reports = "reports"
    send_email = "send_email"
    status = "status"
    sync_account = "sync_account"
    sync_plans = "sync_plans"
    sync_repos = "sync_repos"
    sync_teams = "sync_teams"
    sync_repo_languages = "sync_repo_languages"
    sync_repo_languages_gql = "sync_repo_languages_gql"
    timeseries = "timeseries"
    upload = "upload"
    test_results = "test_results"
