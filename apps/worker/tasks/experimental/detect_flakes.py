import logging
from datetime import timedelta
from uuid import uuid4

import sentry_sdk
from celery.exceptions import SoftTimeLimitExceeded
from django.db import transaction
from django.db.models import Q, QuerySet
from django.utils import timezone

from app import celery_app
from services.test_analytics.ta_metrics import process_flakes_summary
from services.test_analytics.ta_process_flakes import fetch_current_flakes
from shared.celery_config import detect_flakes_task_name
from shared.django_apps.ta_timeseries.models import Testrun
from shared.django_apps.test_analytics.models import Flake, TAUpload
from tasks.base import BaseCodecovTask

log = logging.getLogger(__name__)


def get_testruns(upload_id: int) -> QuerySet[Testrun]:
    upload_filter = Q(upload_id=upload_id)

    # we won't process flakes for testruns older than 1 day
    return Testrun.objects.filter(
        Q(timestamp__gte=timezone.now() - timedelta(days=1)) & upload_filter
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
    upload_id: int, curr_flakes: dict[bytes, Flake], repo_id: int
):
    testruns = get_testruns(upload_id)

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
def process_flakes_for_repo(repo_id: int, upload_ids: list[int]):
    curr_flakes = fetch_current_flakes(repo_id)

    for upload_id in upload_ids:
        with transaction.atomic():
            with process_flakes_summary.labels("new").time():
                # updates testruns and flake objects
                process_single_upload(upload_id, curr_flakes, repo_id)
                new_test_ids = [
                    test_id
                    for test_id, flake in curr_flakes.items()
                    if flake.id is None
                ]

                Flake.objects.bulk_create(
                    curr_flakes.values(),
                    update_conflicts=True,
                    unique_fields=["id"],
                    update_fields=[
                        "end_date",
                        "count",
                        "recent_passes_count",
                        "fail_count",
                    ],
                )

                if new_test_ids:
                    created = Flake.objects.filter(
                        repoid=repo_id, test_id__in=new_test_ids
                    )
                    for flake in created:
                        curr_flakes[bytes(flake.test_id)] = flake

                TAUpload.objects.filter(id=upload_id).update(
                    state="processed", task_token=None
                )


class DetectFlakes(BaseCodecovTask, name=detect_flakes_task_name):
    """
    this task takes a redis lock to avoid stalling tasks due to DB lock contention
    we basically allow one task to do all the work by having it scan for any work it
    can do by getting uploads from TAUpload table

    if we didn't have a redis lock, we'd have a bunch of tasks stalled locking on
    the same row in the flake table, when we can avoid it

    so basically the flow is:
    ingest -> detect flakes

    the ingest task creates the upload objects

    we dedup the detect flakes tasks by having them drop after they're unable to
    take the lock there's an edge case where the previous detect flakes task still
    holds the lock when the next one comes in but is done processing (since checking
    and releasing the lock is not atomic)

    to guard against this, the task that just released the lock must check again

    ---
    Experimental note: this task remains untested and is not production-ready. It
    currently serves as part of the exploratory TA pipeline alongside the
    experimental ingest-testruns and notifier tasks to validate the future
    orchestration flow.
    """

    def run_impl(
        self,
        _db_session,
        *,
        repo_id: int,
        task_token: str | None = None,
        **kwargs,
    ):
        token: str = task_token if task_token is not None else str(uuid4())

        upload_ids: list[int] = []
        try:
            with transaction.atomic():
                uploads = (
                    TAUpload.objects.select_for_update()
                    .filter(repo_id=repo_id, state="pending")
                    .order_by("created_at")
                )

                if not uploads:
                    return {"successful": True, "reason": "no_uploads"}

                head = uploads[0]
                if head.task_token is not None and head.task_token != token:
                    return {"successful": False, "reason": "token_mismatch"}

                for upload in uploads:
                    if upload.task_token and upload.task_token != token:
                        log.error(
                            "Task token mismatch for head of queue",
                            extra={
                                "upload_id": upload.id,
                                "upload_token": upload.task_token,
                                "token": token,
                                "head_token": head.task_token,
                            },
                        )
                        raise RuntimeError("Task token mismatch for head of queue")

                    upload.task_token = token

                    TAUpload.objects.bulk_update(uploads, ["task_token"])
                    upload_ids = [upload.id for upload in uploads]

            if upload_ids:
                process_flakes_for_repo(repo_id, upload_ids)
        except SoftTimeLimitExceeded:
            detect_flakes_task.apply_async(
                kwargs={"repo_id": repo_id, "task_token": token}
            )
            return {"successful": True, "reason": "soft_time_limit_exceeded"}
        except Exception:
            log.exception("Error in detect_flakes")
            raise

        detect_flakes_task.apply_async(kwargs={"repo_id": repo_id, "task_token": token})
        return {"successful": True, "reason": "success"}


RegisteredDetectFlakes = celery_app.register_task(DetectFlakes())
detect_flakes_task = celery_app.tasks[RegisteredDetectFlakes.name]
