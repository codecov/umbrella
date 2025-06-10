import logging

import sentry_sdk

from services.cleanup.models import DELETE_FILES_BATCHSIZE
from services.cleanup.utils import CleanupContext
from shared.django_apps.core.models import Pull, PullStates
from shared.storage.exceptions import FileNotInStorageError

log = logging.getLogger(__name__)


@sentry_sdk.trace
def cleanup_flare(
    context: CleanupContext,
    start_id: int = 5000,
    max_id: int = 500000,
    batch_size: int = DELETE_FILES_BATCHSIZE,
):
    """
    Flare is a field on a Pull object.
    Flare is used to draw static graphs (see GraphHandler view in api) and can be large.
    The majority of flare graphs are used in pr comments, so we keep the (maybe large) flare "available"
    in Archive storage while the pull is OPEN.
    If the pull is not OPEN, we dump the flare to save space.
    If we need to generate a flare graph for a non-OPEN pull, we build_report_from_commit
    and generate fresh flare from that report (see GraphHandler view in api).
    This will only update the _flare_storage_path field for pulls whose flare files were
    successfully deleted from storage.
    """
    try:
        log.info("Flare cleanup: starting task")
        # For any Pull that is not OPEN, clear the flare field(s), targeting older data

        # this method looks hacky, but sorting by updatestamp on a table this large is not possible,
        # so this is my approach to get the oldest pulls first - using the sequential pk id.
        # going to hard-code limits here for initial runs: Pulls with id 0-5000, batch_size=50 -> ran in under 1 sec
        # start at 5.000, go to 500.000, increase batch_size to 500, so loop through 1000 times

        # Clear in db
        cleaned_db_flares = 0
        # Process in ID ranges instead of sorting by updatestamp
        for id_start in range(start_id, max_id, 500):
            id_end = id_start + batch_size

            non_open_pulls_with_flare_in_db = Pull.objects.filter(
                id__gt=id_start, id__lte=id_end, _flare__isnull=False
            ).exclude(state=PullStates.OPEN.value)

            batch_ids = list(
                non_open_pulls_with_flare_in_db.values_list("id", flat=True)
            )

            # Update directly with ID list
            if batch_ids:
                cleaned_db_flares += Pull.objects.filter(id__in=batch_ids).update(
                    _flare=None
                )

        if cleaned_db_flares:
            log.info(f"Flare cleanup: cleared {cleaned_db_flares} database flares")

        # Clear in Archive
        cleaned_file_flares = 0
        # Process in ID ranges instead of sorting by updatestamp
        # keep small batch_size here since files are deleted 1 by 1
        # this will loop 9.900 times with the current defaults
        for id_start in range(start_id, max_id, batch_size):
            id_end = id_start + batch_size

            non_open_pulls_with_flare_in_archive = Pull.objects.filter(
                id__gt=id_start, id__lte=id_end, _flare_storage_path__isnull=False
            ).exclude(state=PullStates.OPEN.value)

            batch_of_id_path_pairs = list(
                non_open_pulls_with_flare_in_archive.values_list(
                    "id", "_flare_storage_path"
                )
            )

            # Track which pulls had successful deletions
            successful_deletions = []

            # Process all files in this batch
            for pull_id, path in batch_of_id_path_pairs:
                try:
                    if context.storage.delete_file(context.default_bucket, path):
                        successful_deletions.append(pull_id)
                except FileNotInStorageError:
                    # If file isn't in storage, still mark as successful
                    successful_deletions.append(pull_id)
                except Exception as e:
                    log.error(f"Flare cleanup: error deleting file {path}: {e}")
                    sentry_sdk.capture_exception(e)

            # Only update pulls where files were successfully deleted
            if successful_deletions:
                Pull.objects.filter(id__in=successful_deletions).update(
                    _flare_storage_path=None
                )

            cleaned_file_flares += len(successful_deletions)
            context.add_progress(cleaned_files=len(successful_deletions), model=Pull)

        if cleaned_file_flares:
            log.info(f"Flare cleanup: cleaned up {cleaned_file_flares} file flares")

    except Exception as e:
        log.error(f"Flare cleanup: unexpected error: {e}")
        sentry_sdk.capture_exception(e)
