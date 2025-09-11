import logging

from celery.exceptions import SoftTimeLimitExceeded
from django.db import transaction

from app import celery_app
from shared.celery_config import mark_owner_for_deletion_task_name
from shared.django_apps.codecov_auth.models import Owner, OwnerToBeDeleted, Service
from tasks.base import BaseCodecovTask

log = logging.getLogger(__name__)


def obfuscate_owner_data(owner: Owner) -> None:
    """
    Obfuscate sensitive owner data by replacing with generic values.
    """
    if owner.name:
        owner.name = f"[DELETED_USER_{owner.ownerid}]"

    if owner.email:
        owner.email = f"deleted_{owner.ownerid}@deleted.codecov.io"

    if owner.business_email:
        owner.business_email = f"deleted_{owner.ownerid}@deleted.codecov.io"

    if owner.username:
        owner.username = f"deleted_user_{owner.ownerid}"

    # Obfuscate service by setting to a generic value
    owner.service = Service.TO_BE_DELETED.value

    # Clear sensitive tokens
    owner.oauth_token = None

    # Save the obfuscated data
    owner.save(
        update_fields=[
            "name",
            "email",
            "business_email",
            "username",
            "service",
            "oauth_token",
        ]
    )


class MarkOwnerForDeletionTask(BaseCodecovTask, name=mark_owner_for_deletion_task_name):
    acks_late = True  # retry the task when the worker dies for whatever reason
    max_retries = None  # aka, no limit on retries

    def run_impl(self, _db_session, ownerid: int) -> dict:
        try:
            log.info(f"Starting to mark owner {ownerid} for deletion")

            with transaction.atomic():
                try:
                    owner = Owner.objects.select_for_update().get(ownerid=ownerid)
                except Owner.DoesNotExist:
                    log.error(f"Owner {ownerid} not found")
                    raise ValueError(f"Owner {ownerid} not found")

                # Check if owner is already marked for deletion
                if OwnerToBeDeleted.objects.filter(owner_id=ownerid).exists():
                    log.info(f"Owner {ownerid} is already marked for deletion")
                    return {"status": "already_marked", "ownerid": ownerid}

                obfuscate_owner_data(owner)
                log.info(f"Obfuscated data for owner {ownerid}")

                OwnerToBeDeleted.objects.create(owner_id=ownerid)
                log.info(f"Added owner {ownerid} to OwnerToBeDeleted table")

            return {
                "status": "success",
                "ownerid": ownerid,
                "message": f"Owner {ownerid} marked for deletion and data obfuscated",
            }

        except SoftTimeLimitExceeded:
            raise self.retry()


RegisteredMarkOwnerForDeletionTask = celery_app.register_task(
    MarkOwnerForDeletionTask()
)
mark_owner_for_deletion_task = celery_app.tasks[MarkOwnerForDeletionTask.name]
