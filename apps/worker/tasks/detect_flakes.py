import logging

import sentry_sdk
from celery.exceptions import SoftTimeLimitExceeded
from django.db import transaction
from redis.exceptions import LockError

from app import celery_app
from services.test_analytics.ta_metrics import process_flakes_summary
from services.test_analytics.ta_process_flakes import (
    fetch_current_flakes,
    process_single_upload,
)
from shared.celery_config import detect_flakes_task_name
from shared.django_apps.reports.models import ReportSession
from shared.django_apps.test_analytics.models import Flake, TAUpload
from shared.helpers.redis import get_redis_connection
from tasks.base import BaseCodecovTask

log = logging.getLogger(__name__)


DETECT_FLAKES_LOCK_NAME = "detect_flakes_lock:{}"


@sentry_sdk.trace
def process_flakes_for_repo(repo_id: int):
    redis_client = get_redis_connection()
    lock_name = DETECT_FLAKES_LOCK_NAME.format(repo_id)
    try:
        with redis_client.lock(lock_name):
            curr_flakes = fetch_current_flakes(repo_id)
            while True:
                uploads = list(
                    TAUpload.objects.filter(repo_id=repo_id).order_by("created_at")[:50]
                )
                if not uploads:
                    break
                for upload in uploads:
                    with transaction.atomic():
                        with process_flakes_summary.labels("new").time():
                            # updates testruns and flake objects
                            process_single_upload(
                                ReportSession(id=upload.id), curr_flakes, repo_id
                            )
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
                            upload.delete()
            return True
    except LockError:
        log.warning("Failed to acquire lock for repo %s", repo_id)
        return False


class DetectFlakes(BaseCodecovTask, name=detect_flakes_task_name):
    def run_impl(self, db_session, *, repo_id: int, **kwargs):
        try:
            process_flakes_for_repo(repo_id)
            return {"successful": True}
        except SoftTimeLimitExceeded:
            remaining = TAUpload.objects.filter(repo_id=repo_id).count()
            if remaining > 0:
                detect_flakes_task.apply_async(kwargs={"repo_id": repo_id})
            return {"successful": False}


RegisteredDetectFlakes = celery_app.register_task(DetectFlakes())
detect_flakes_task = celery_app.tasks[RegisteredDetectFlakes.name]
