import logging
import random
from collections.abc import Callable
from functools import partial

from services.cleanup.cleanup import cleanup_queryset
from services.cleanup.uploads import cleanup_old_uploads
from services.cleanup.utils import CleanupContext, CleanupSummary, cleanup_context
from shared.django_apps.reports.models import CommitReport
from shared.django_apps.staticanalysis.models import StaticAnalysisSingleFileSnapshot

log = logging.getLogger(__name__)

CleanupJob = Callable[[CleanupContext], None]


def run_regular_cleanup() -> CleanupSummary:
    log.info("Starting regular cleanup job")

    cleanup_jobs: list[tuple[str, CleanupJob]] = [
        (
            "Static Analysis Files",
            partial(cleanup_queryset, StaticAnalysisSingleFileSnapshot.objects.all()),
        ),
        (
            '`CommitReport`s for "local uploads"',
            partial(cleanup_queryset, CommitReport.objects.filter(code__isnull=False)),
        ),
        ("old `Upload`s", cleanup_old_uploads),
        # TODO: re-institute this job with https://linear.app/getsentry/issue/CCMRG-1355
        # ("Stale pull flare", cleanup_flare),
    ]

    # TODO:
    # - cleanup `Commit`s that are `deleted`
    # - figure out a way how we can first mark, and then fully delete `Branch`es

    # as we expect this job to have frequent retries, and cleanup to take a long time,
    # lets shuffle the various cleanups so that each one of those makes a little progress.
    random.shuffle(cleanup_jobs)

    with cleanup_context() as context:
        for description, job in cleanup_jobs:
            log.info(f"Starting cleanup for {description}…")
            job(context)
        summary = context.summary

    log.info("Regular cleanup finished")
    return summary
