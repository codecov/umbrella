import logging
import time
from datetime import datetime

import sentry_sdk
from django.conf import settings
from django.db import DatabaseError, OperationalError, connections

from services.test_analytics.ta_timeseries import get_pr_comment_agg

log = logging.getLogger(__name__)

_MAX_RETRIES = 2
_RETRY_SLEEP_SECONDS = 0.5


def get_test_status(
    repo_id: int,
    commit_sha: str,
    lower_bound_timestamp: datetime | None = None,
) -> tuple[bool, bool]:
    if not settings.TA_TIMESERIES_ENABLED:
        return False, False

    pr_comment_agg = None
    last_exc: DatabaseError | None = None
    for attempt in range(_MAX_RETRIES + 1):
        if attempt > 0:
            # Reset the broken connection before retrying
            connections["ta_timeseries"].close()
            time.sleep(_RETRY_SLEEP_SECONDS)
        try:
            pr_comment_agg = get_pr_comment_agg(
                repo_id, commit_sha, lower_bound_timestamp
            )
            last_exc = None
            break
        except OperationalError as exc:
            last_exc = exc
            log.warning(
                "TA timeseries connection error; will retry if attempts remain",
                extra={
                    "repo_id": repo_id,
                    "commit_sha": commit_sha,
                    "attempt": attempt,
                },
                exc_info=exc,
            )
        except DatabaseError as exc:
            last_exc = exc
            break

    if last_exc is not None:
        log.warning(
            "TA timeseries query failed; failing open for notifications",
            extra={"repo_id": repo_id, "commit_sha": commit_sha},
            exc_info=last_exc,
        )
        sentry_sdk.capture_exception(last_exc)
        return False, False

    failed = pr_comment_agg.get("failed", 0)
    passed = pr_comment_agg.get("passed", 0)

    any_failures = failed > 0
    all_passed = passed > 0 and failed == 0

    return any_failures, all_passed
