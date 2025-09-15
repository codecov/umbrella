from django.conf import settings

from services.test_analytics.ta_timeseries import get_pr_comment_agg


def get_test_status(repo_id: int, commit_sha: str) -> tuple[bool, bool]:
    if not settings.TA_TIMESERIES_ENABLED:
        return False, False

    pr_comment_agg = get_pr_comment_agg(repo_id, commit_sha)
    failed = pr_comment_agg.get("failed", 0)
    passed = pr_comment_agg.get("passed", 0)

    any_failures = failed > 0
    all_passed = passed > 0 and failed == 0

    return any_failures, all_passed
