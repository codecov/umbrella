import logging
from datetime import datetime

from django.conf import settings
from django.db import OperationalError

from services.test_analytics.ta_timeseries import get_pr_comment_agg

log = logging.getLogger(__name__)


def get_test_status(
    repo_id: int,
    commit_sha: str,
    lower_bound_timestamp: datetime | None = None,
) -> tuple[bool, bool]:
    if not settings.TA_TIMESERIES_ENABLED:
        return False, False

    try:
        pr_comment_agg = get_pr_comment_agg(repo_id, commit_sha, lower_bound_timestamp)
    except OperationalError:
        log.warning(
            "Failed to get test status from ta_timeseries due to a database error",
            extra={"repo_id": repo_id, "commit_sha": commit_sha},
            exc_info=True,
        )
        return False, False

    failed = pr_comment_agg.get("failed", 0)
    passed = pr_comment_agg.get("passed", 0)

    any_failures = failed > 0
    all_passed = passed > 0 and failed == 0

    return any_failures, all_passed
