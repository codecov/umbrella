import logging

import sentry_sdk

from services.cleanup.models import DELETE_FILES_BATCHSIZE
from services.cleanup.utils import CleanupContext, CleanupResult
from shared.django_apps.core.models import Pull, PullStates
from shared.storage.exceptions import FileNotInStorageError

log = logging.getLogger(__name__)


@sentry_sdk.trace
def cleanup_flare(
    context: CleanupContext, batch_size: int = DELETE_FILES_BATCHSIZE, limit: int = 1000
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
    # For any Pull that is not OPEN, clear the flare field(s), targeting older data
    non_open_pulls = Pull.objects.exclude(state=PullStates.OPEN.value).order_by(
        "updatestamp"
    )

    # Clear in db
    non_open_pulls_with_flare_in_db = non_open_pulls.filter(
        _flare__isnull=False
    ).exclude(_flare={})

    # Process in batches - this is being overprotective at the moment, the batch size could be much larger
    start = 0
    while start < limit:
        stop = start + batch_size if start + batch_size < limit else limit
        batch = non_open_pulls_with_flare_in_db.values_list("id", flat=True)[start:stop]
        if not batch:
            break
        n_updated = non_open_pulls_with_flare_in_db.filter(id__in=batch).update(
            _flare=None
        )
        context.add_progress(cleaned_models=n_updated, model=Pull)
        start = stop

    log.info(
        f"Flare cleanup: cleared {context.summary.summary.get('Pull', CleanupResult(0)).cleaned_models} database flares"
    )

    # Clear in Archive
    non_open_pulls_with_flare_in_archive = non_open_pulls.filter(
        _flare_storage_path__isnull=False
    )

    # Process archive deletions in batches
    total_files_processed = 0
    start = 0
    while start < limit:
        stop = start + batch_size if start + batch_size < limit else limit
        # Get ids and paths together
        batch_of_id_path_pairs = non_open_pulls_with_flare_in_archive.values_list(
            "id", "_flare_storage_path"
        )[start:stop]
        if not batch_of_id_path_pairs:
            break

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
                sentry_sdk.capture_exception(e)

        # Only update pulls where files were successfully deleted
        if successful_deletions:
            Pull.objects.filter(id__in=successful_deletions).update(
                _flare_storage_path=None
            )

        total_files_processed += len(batch_of_id_path_pairs)
        context.add_progress(cleaned_files=len(successful_deletions), model=Pull)

        start = stop

    log.info(
        f"Flare cleanup: processed {total_files_processed} archive flares, {context.summary.summary.get('Pull', CleanupResult(0)).cleaned_files} successfully deleted"
    )
