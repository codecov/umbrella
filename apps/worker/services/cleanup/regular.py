import logging
import random

import sentry_sdk
from django.db.models.query import QuerySet

from services.cleanup.cleanup import run_cleanup
from services.cleanup.models import DELETE_FILES_BATCHSIZE
from services.cleanup.uploads import cleanup_old_uploads
from services.cleanup.utils import CleanupResult, CleanupSummary, cleanup_context
from shared.django_apps.core.models import Pull, PullStates
from shared.django_apps.reports.models import CommitReport
from shared.django_apps.staticanalysis.models import StaticAnalysisSingleFileSnapshot
from shared.storage.exceptions import FileNotInStorageError

log = logging.getLogger(__name__)


@sentry_sdk.trace
def cleanup_flares(
    context, batch_size=DELETE_FILES_BATCHSIZE, limit=10000
) -> CleanupSummary:
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

    log.info("Starting flare cleanup")

    # Clear in db
    non_open_pulls_with_flare_in_db = non_open_pulls.filter(
        _flare__isnull=False
    ).exclude(_flare={})

    # Process in batches - this is being overprotective at the moment, the batch size could be much larger
    total_db_updated = 0
    start = 0
    while start < limit:
        stop = start + batch_size if start + batch_size < limit else limit
        batch = non_open_pulls_with_flare_in_db.values_list("id", flat=True)[start:stop]
        if not batch:
            break
        n_updated = non_open_pulls_with_flare_in_db.filter(id__in=batch).update(
            _flare=None
        )
        total_db_updated += n_updated
        start = stop

    log.info(f"Flare cleanup cleared {total_db_updated} database flares")

    # Clear in Archive
    non_open_pulls_with_flare_in_archive = non_open_pulls.filter(
        _flare_storage_path__isnull=False
    )

    # Process archive deletions in batches
    total_files_processed = 0
    total_success = 0
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
        total_success += len(successful_deletions)
        start = stop

    log.info(
        f"Flare cleanup: processed {total_files_processed} archive flares, {total_success} successfully deleted"
    )

    totals = CleanupResult(total_db_updated, total_success)
    if total_db_updated > 0 or total_success > 0:
        summary = {Pull: totals}
    else:
        summary = {}
    return CleanupSummary(totals, summary)


def run_regular_cleanup() -> CleanupSummary:
    log.info("Starting regular cleanup job")
    complete_summary = CleanupSummary(CleanupResult(0), summary={})

    cleanups_to_run: list[QuerySet] = [
        StaticAnalysisSingleFileSnapshot.objects.all(),
        CommitReport.objects.filter(code__isnull=False),
    ]

    # as we expect this job to have frequent retries, and cleanup to take a long time,
    # lets shuffle the various cleanups so that each one of those makes a little progress.
    random.shuffle(cleanups_to_run)

    with cleanup_context() as context:
        for query in cleanups_to_run:
            name = query.model.__name__
            log.info(f"Cleaning up `{name}`")
            summary = run_cleanup(query, context=context)
            log.info(f"Cleaned up `{name}`", extra={"summary": summary})
            complete_summary.add(summary)

        # Run flare cleanup as part of regular cleanup
        # reduce limit for initial runs
        flare_summary = cleanup_flares(context, limit=1000)
        complete_summary.add(flare_summary)

        summary = cleanup_old_uploads(context)
        complete_summary.add(summary)

    # TODO:
    # - cleanup `Commit`s that are `deleted`
    # - figure out a way how we can first mark, and then fully delete `Branch`es

    log.info("Regular cleanup finished")
    return complete_summary
