"""
This abstracts the "processing state" for a commit.

It takes care that each upload for a specific commit is going through the following
states:

- "processing": when an upload was received and is being parsed/processed.
- "processed": the upload has been processed and an "intermediate report" has been stored,
  the upload is now waiting to be merged into the "master report".
- "merged": the upload was fully merged into the "master report".

The logic in this file also makes sure that processing and merging happens in an "optimal" way
meaning that:

- "postprocessing", which means triggering notifications and other followup work
  only happens once for a commit.
- merging should happen in batches, as that involves loading a bunch of "intermediate report"s
  into memory, which should be bounded.
- (ideally in the future) an upload that has been processed into an "intermediate report"
  should be merged directly into the "master report" without doing a storage roundtrip for that
  "intermediate report".
"""

import logging
from dataclasses import dataclass

from sqlalchemy import case, func
from sqlalchemy.orm import Session

from database.enums import ReportType
from database.models.core import Commit
from database.models.reports import CommitReport, Upload
from shared.metrics import Counter
from shared.reports.enums import UploadState

log = logging.getLogger(__name__)

MERGE_BATCH_SIZE = 10

CLEARED_UPLOADS = Counter(
    "worker_processing_cleared_uploads",
    "Number of uploads cleared from queue because of errors",
)


@dataclass
class UploadNumbers:
    uploaded: int
    """
    The number of uploads that have finished being uploaded.
    """

    processed: int
    """
    The number of uploads that have been processed,
    and are waiting on being merged into the "master report".
    """


def should_perform_merge(uploads: UploadNumbers) -> bool:
    """
    Determines whether a merge should be performed.

    This is the case when no more uploads are expected,
    or we reached the desired batch size for merging.
    """
    return uploads.processed > 0


def should_trigger_postuploaded(uploads: UploadNumbers) -> bool:
    """
    Determines whether post-uploaded steps, such as notifications, etc,
    should be performed.

    This is the case when no more uploads are expected,
    and all the processed uploads have been merged into the "master report".
    """
    return uploads.uploaded == 0 and uploads.processed == 0


class ProcessingState:
    def __init__(self, repoid: int, commitsha: str, db_session: Session) -> None:
        self.repoid = repoid
        self.commitsha = commitsha
        self._db_session = db_session

    def get_upload_numbers(self):
        row = (
            self._db_session.query(
                func.count(
                    case(
                        (
                            Upload.state_id == UploadState.UPLOADED.db_id,
                            Upload.id_,
                        ),
                    )
                ),
                func.count(
                    case(
                        (
                            Upload.state_id == UploadState.PROCESSED.db_id,
                            Upload.id_,
                        ),
                    )
                ),
            )
            .join(CommitReport, Upload.report_id == CommitReport.id_)
            .join(Commit, CommitReport.commit_id == Commit.id_)
            .filter(
                Commit.repoid == self.repoid,
                Commit.commitid == self.commitsha,
                (CommitReport.report_type == None)  # noqa: E711
                | (CommitReport.report_type == ReportType.COVERAGE.value),
            )
            .one()
        )
        return UploadNumbers(uploaded=row[0], processed=row[1])

    def clear_in_progress_uploads(self, upload_ids: list[int]):
        if not upload_ids:
            return
        # Mark still-UPLOADED uploads as ERROR so they stop being counted
        # as "uploaded" in get_upload_numbers(). Only matches UPLOADED --
        # already-PROCESSED uploads (success path) are unaffected.
        #
        # This runs in a finally block, so the transaction may already be
        # in a failed state. Best-effort: log and move on if the DB is
        # unreachable — the upload stays UPLOADED, which is safe.
        try:
            updated = (
                self._db_session.query(Upload)
                .filter(
                    Upload.id_.in_(upload_ids),
                    Upload.state_id == UploadState.UPLOADED.db_id,
                )
                .update(
                    {
                        Upload.state_id: UploadState.ERROR.db_id,
                        Upload.state: "error",
                    },
                    synchronize_session="fetch",
                )
            )
            if updated > 0:
                CLEARED_UPLOADS.inc(updated)
        except Exception:
            log.warning(
                "Failed to clear in-progress uploads (transaction may be aborted)",
                extra={"upload_ids": upload_ids},
                exc_info=True,
            )

    def mark_upload_as_processed(self, upload_id: int):
        upload = self._db_session.query(Upload).get(upload_id)
        if upload:
            upload.state_id = UploadState.PROCESSED.db_id
            # Don't set upload.state here -- the finisher's idempotency check
            # uses state="processed" to detect already-merged uploads.
            # The state string is set by update_uploads() after merging.

    def mark_uploads_as_merged(self, upload_ids: list[int]):
        if not upload_ids:
            return
        self._db_session.query(Upload).filter(
            Upload.id_.in_(upload_ids),
            Upload.state_id == UploadState.PROCESSED.db_id,
        ).update(
            {
                Upload.state_id: UploadState.MERGED.db_id,
                Upload.state: "merged",
            },
            synchronize_session="fetch",
        )
        self._db_session.commit()

    def get_uploads_for_merging(self) -> set[int]:
        rows = (
            self._db_session.query(Upload.id_)
            .join(CommitReport, Upload.report_id == CommitReport.id_)
            .join(Commit, CommitReport.commit_id == Commit.id_)
            .filter(
                Commit.repoid == self.repoid,
                Commit.commitid == self.commitsha,
                (CommitReport.report_type == None)  # noqa: E711
                | (CommitReport.report_type == ReportType.COVERAGE.value),
                Upload.state_id == UploadState.PROCESSED.db_id,
            )
            .limit(MERGE_BATCH_SIZE)
            .all()
        )
        return {row[0] for row in rows}

    def count_remaining_coverage_uploads(self) -> int:
        return (
            self._db_session.query(Upload)
            .join(CommitReport, Upload.report_id == CommitReport.id_)
            .join(Commit, CommitReport.commit_id == Commit.id_)
            .filter(
                Commit.repoid == self.repoid,
                Commit.commitid == self.commitsha,
                (CommitReport.report_type == None)  # noqa: E711
                | (CommitReport.report_type == ReportType.COVERAGE.value),
                Upload.state_id == UploadState.UPLOADED.db_id,
            )
            .count()
        )
