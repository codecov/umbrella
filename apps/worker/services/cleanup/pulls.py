import logging
import time

import sentry_sdk
from django.db.models.aggregates import Max
from services.cleanup.models import DELETE_FILES_BATCHSIZE
from services.cleanup.utils import CleanupContext
from shared.django_apps.core.models import Pull, PullStates
from shared.storage.exceptions import FileNotInStorageError

log = logging.getLogger(__name__)


@sentry_sdk.trace
def cleanup_flare(
    context: CleanupContext,
    start_id: int = 1,
    max_id: int = None,
    db_batch_size: int = 500,
    file_batch_size: int = DELETE_FILES_BATCHSIZE,
    max_total_batches: int = 200,  # Cap work per run
):
    """
    Flare is a field on a Pull object.
    Flare is used to draw static graphs (see GraphHandler view in api) and can be large.
    The majority of flare graphs are used in pr comments, so we keep the (maybe large) flare "available"
    in Archive storage while the pull is OPEN.
    If the pull is not OPEN, we dump the flare to save space.
    If we need to generate a flare graph for a non-OPEN pull, we build_report_from_commit
    and generate fresh flare from that report (see GraphHandler view in api).

    Note: through trial and error we learned that sorting by updatestamp on a table this large is not possible,
    so use the sequential pk id to clean up from oldest to newest.

    This task is low and slow - it runs overnight for maximum 10 minutes. It will eventually work through the whole
    table, should be progressing through the backlog each night it runs until it is operating on current Pulls.
    """
    # For any Pull that is not OPEN, clear the flare field(s), targeting older data

    try:
        log.info("Flare cleanup: starting task")

        # id is indexed, so this should be a fast call
        actual_max_id = Pull.objects.aggregate(max_id=Max("id"))["max_id"] or 0
        max_id = max_id or actual_max_id
        max_id = min(max_id, start_id + (max_total_batches * db_batch_size))

        # Clear in db, batch size is 500
        cleaned_db_flares = 0
        for id_start in range(start_id, max_id, db_batch_size):
            id_end = id_start + db_batch_size

            non_open_pulls_with_flare_in_db = (
                Pull.objects.filter(
                    id__gt=id_start,
                    id__lte=id_end,
                    _flare__isnull=False,
                )
                .exclude(state=PullStates.OPEN.value)
                .iterator(chunk_size=100)  # Use iterator to avoid holding DB cursors
            )

            for pull in non_open_pulls_with_flare_in_db:
                pull._flare = None
                pull.save(update_fields=["_flare"])
                cleaned_db_flares += 1

        if cleaned_db_flares:
            log.info(f"Flare cleanup: cleared {cleaned_db_flares} database flares")

        # Clear in Archive, batch size is DELETE_FILES_BATCHSIZE
        # keep small batch_size here since files are deleted 1 by 1
        cleaned_file_flares = 0
        for id_start in range(start_id, max_id, file_batch_size):
            id_end = id_start + file_batch_size

            non_open_pulls_with_flare_in_archive = (
                Pull.objects.filter(
                    id__gt=id_start,
                    id__lte=id_end,
                    _flare_storage_path__isnull=False,
                )
                .exclude(state=PullStates.OPEN.value)
                .values_list("id", "_flare_storage_path")
            )

            batch_of_id_path_pairs = list(non_open_pulls_with_flare_in_archive)
            if not batch_of_id_path_pairs:
                continue

            # Track which pulls had successful deletions
            successful_deletions = []

            for pull_id, path in batch_of_id_path_pairs:
                try:
                    if context.storage.delete_file(context.default_bucket, path):
                        successful_deletions.append(pull_id)
                        cleaned_file_flares += 1
                    time.sleep(0.005)  # Throttle to avoid socket bursts
                except FileNotInStorageError:
                    # If file isn't in storage, still mark as successful
                    successful_deletions.append(pull_id)
                    cleaned_file_flares += 1
                    time.sleep(0.005)  # Throttle to avoid socket bursts
                except Exception as e:
                    log.error(f"Flare cleanup: error deleting file {path}: {e}")
                    sentry_sdk.capture_exception(e)

            # Only update pulls where files were successfully deleted
            if successful_deletions:
                Pull.objects.filter(id__in=successful_deletions).update(
                    _flare_storage_path=None
                )

            context.add_progress(cleaned_files=len(successful_deletions), model=Pull)

        if cleaned_file_flares:
            log.info(f"Flare cleanup: cleaned up {cleaned_file_flares} file flares")

    except Exception as e:
        log.error(f"Flare cleanup: unexpected error: {e}")
        sentry_sdk.capture_exception(e)
