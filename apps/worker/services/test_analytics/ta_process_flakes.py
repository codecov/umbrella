import logging
from datetime import timedelta

import sentry_sdk
from django.db.models import Q, QuerySet
from django.utils import timezone
from redis.exceptions import LockError

from services.test_analytics.ta_metrics import process_flakes_summary
from shared.django_apps.reports.models import CommitReport, ReportSession
from shared.django_apps.ta_timeseries.models import Testrun
from shared.django_apps.test_analytics.models import Flake
from shared.helpers.redis import get_redis_connection

log = logging.getLogger(__name__)

LOCK_NAME = "ta_flake_lock:{}"
KEY_NAME = "ta_flake_key:{}"


def get_relevant_uploads(repo_id: int, commit_id: str) -> QuerySet[ReportSession]:
    return ReportSession.objects.filter(
        report__report_type=CommitReport.ReportType.TEST_RESULTS.value,
        report__commit__repository__repoid=repo_id,
        report__commit__commitid=commit_id,
        state__in=["processed"],
    )


def fetch_current_flakes(repo_id: int) -> dict[bytes, Flake]:
    return {
        bytes(flake.test_id): flake for flake in Flake.objects.filter(repoid=repo_id)
    }


def get_testruns(upload: ReportSession) -> QuerySet[Testrun]:
    upload_filter = Q(upload_id=upload.id)

    # we won't process flakes for testruns older than 1 day
    return Testrun.objects.filter(
        Q(timestamp__gte=timezone.now() - timedelta(days=1)) & upload_filter
    ).order_by("timestamp")


def get_testruns_for_uploads(
    upload_ids: list[int],
) -> QuerySet[Testrun]:
    return Testrun.objects.filter(
        timestamp__gte=timezone.now() - timedelta(days=1), upload_id__in=upload_ids
    ).order_by("timestamp")


def handle_pass(curr_flakes: dict[bytes, Flake], test_id: bytes):
    # possible that we expire it and stop caring about it
    if test_id not in curr_flakes:
        return

    curr_flakes[test_id].recent_passes_count += 1
    curr_flakes[test_id].count += 1
    if curr_flakes[test_id].recent_passes_count == 30:
        curr_flakes[test_id].end_date = timezone.now()
        curr_flakes[test_id].save()
        del curr_flakes[test_id]


def handle_failure(
    curr_flakes: dict[bytes, Flake], test_id: bytes, testrun: Testrun, repo_id: int
):
    existing_flake = curr_flakes.get(test_id)

    if existing_flake:
        existing_flake.fail_count += 1
        existing_flake.count += 1
        existing_flake.recent_passes_count = 0
    else:
        new_flake = Flake(
            repoid=repo_id,
            test_id=test_id,
            count=1,
            fail_count=1,
            recent_passes_count=0,
            start_date=timezone.now(),
        )
        curr_flakes[test_id] = new_flake

    if testrun.outcome != "flaky_fail":
        testrun.outcome = "flaky_fail"


@sentry_sdk.trace
def process_single_upload(
    upload: ReportSession,
    curr_flakes: dict[bytes, Flake],
    repo_id: int,
    testruns: list[Testrun],
):
    for testrun in testruns:
        test_id = bytes(testrun.test_id)
        match testrun.outcome:
            case "pass":
                if test_id not in curr_flakes:
                    continue

                handle_pass(curr_flakes, test_id)
            case "failure" | "flaky_fail" | "error":
                handle_failure(curr_flakes, test_id, testrun, repo_id)
            case _:
                continue

    Testrun.objects.bulk_update(testruns, ["outcome"])


@sentry_sdk.trace
def process_flakes_for_commit(repo_id: int, commit_id: str):
    log.info(
        "process_flakes_for_commit: starting processing",
    )
    uploads = list(get_relevant_uploads(repo_id, commit_id))

    log.info(
        "process_flakes_for_commit: fetched uploads",
        extra={"uploads": [upload.id for upload in uploads]},
    )

    curr_flakes = fetch_current_flakes(repo_id)

    log.info(
        "process_flakes_for_commit: fetched current flakes",
        extra={"flakes": [flake.test_id.hex() for flake in curr_flakes.values()]},
    )

    upload_ids = [upload.id for upload in uploads]
    all_testruns = get_testruns_for_uploads(upload_ids)

    testruns_by_upload = {}
    for testrun in all_testruns:
        if testrun.upload_id not in testruns_by_upload:
            testruns_by_upload[testrun.upload_id] = []
        testruns_by_upload[testrun.upload_id].append(testrun)

    for upload in uploads:
        upload_testruns = testruns_by_upload.get(upload.id, [])
        process_single_upload(upload, curr_flakes, repo_id, upload_testruns)
        log.info(
            "process_flakes_for_commit: processed upload",
            extra={"upload": upload.id},
        )

    log.info(
        "process_flakes_for_commit: bulk creating flakes",
        extra={"flakes": [flake.test_id.hex() for flake in curr_flakes.values()]},
    )

    Flake.objects.bulk_create(
        curr_flakes.values(),
        update_conflicts=True,
        unique_fields=["id"],
        update_fields=["end_date", "count", "recent_passes_count", "fail_count"],
    )


@sentry_sdk.trace
def process_flakes_for_repo(repo_id: int):
    redis_client = get_redis_connection()
    lock_name = LOCK_NAME.format(repo_id)
    key_name = KEY_NAME.format(repo_id)
    try:
        with redis_client.lock(lock_name, timeout=300, blocking_timeout=3):
            while commit_ids := redis_client.lpop(key_name, 10):
                for commit_id in commit_ids:
                    with process_flakes_summary.labels("new").time():
                        process_flakes_for_commit(repo_id, commit_id.decode())
            return True
    except LockError:
        log.warning("Failed to acquire lock for repo %s", repo_id)
        return False
