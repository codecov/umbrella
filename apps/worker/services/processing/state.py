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

from dataclasses import dataclass

from sqlalchemy import case, func
from sqlalchemy.orm import Session

from database.enums import ReportType
from database.models.core import Commit
from database.models.reports import CommitReport, Upload
from shared.helpers.redis import get_redis_connection
from shared.metrics import Counter
from shared.reports.enums import UploadState

MERGE_BATCH_SIZE = 10

# TTL for processing state keys in Redis (24 hours, matches intermediate report TTL)
# This prevents state keys from accumulating indefinitely and ensures consistency
# with intermediate report expiration
PROCESSING_STATE_TTL = 24 * 60 * 60

CLEARED_UPLOADS = Counter(
    "worker_processing_cleared_uploads",
    "Number of uploads cleared from queue because of errors",
)


@dataclass
class UploadNumbers:
    processing: int
    """
    The number of uploads currently being processed.
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
    return uploads.processing == 0 or uploads.processed >= MERGE_BATCH_SIZE


def should_trigger_postprocessing(uploads: UploadNumbers) -> bool:
    """
    Determines whether post-processing steps, such as notifications, etc,
    should be performed.

    This is the case when no more uploads are expected,
    and all the processed uploads have been merged into the "master report".
    """
    return uploads.processing == 0 and uploads.processed == 0


class ProcessingState:
    def __init__(
        self, repoid: int, commitsha: str, db_session: Session | None = None
    ) -> None:
        self._redis = get_redis_connection()
        self.repoid = repoid
        self.commitsha = commitsha
        self._db_session = db_session

    def get_upload_numbers(self):
        if self._db_session:
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
            return UploadNumbers(processing=row[0], processed=row[1])

        processing = self._redis.scard(self._redis_key("processing"))
        processed = self._redis.scard(self._redis_key("processed"))
        return UploadNumbers(processing, processed)

    def mark_uploads_as_processing(self, upload_ids: list[int]):
        if not upload_ids:
            return
        key = self._redis_key("processing")
        self._redis.sadd(key, *upload_ids)
        self._redis.expire(key, PROCESSING_STATE_TTL)

    def clear_in_progress_uploads(self, upload_ids: list[int]):
        if not upload_ids:
            return
        if self._db_session:
            # Mark still-UPLOADED uploads as ERROR so they stop being counted
            # as "processing" in get_upload_numbers(). Only matches UPLOADED --
            # already-PROCESSED uploads (success path) are unaffected.
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
            self._redis.srem(self._redis_key("processing"), *upload_ids)
            return
        removed_uploads = self._redis.srem(self._redis_key("processing"), *upload_ids)
        if removed_uploads > 0:
            CLEARED_UPLOADS.inc(removed_uploads)

    def mark_upload_as_processed(self, upload_id: int):
        if self._db_session:
            upload = self._db_session.query(Upload).get(upload_id)
            if upload:
                upload.state_id = UploadState.PROCESSED.db_id
                # Don't set upload.state here -- the finisher's idempotency check
                # uses state="processed" to detect already-merged uploads.
                # The state string is set by update_uploads() after merging.

        processing_key = self._redis_key("processing")
        processed_key = self._redis_key("processed")

        res = self._redis.smove(processing_key, processed_key, upload_id)
        if not res:
            self._redis.sadd(processed_key, upload_id)

        self._redis.expire(processed_key, PROCESSING_STATE_TTL)

    def mark_uploads_as_merged(self, upload_ids: list[int]):
        if not upload_ids:
            return
        if self._db_session:
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
            self._redis.srem(self._redis_key("processed"), *upload_ids)
            return
        self._redis.srem(self._redis_key("processed"), *upload_ids)

    def get_uploads_for_merging(self) -> set[int]:
        if self._db_session:
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

        return {int(id) for id in self._redis.smembers(self._redis_key("processed"))}

    def _redis_key(self, state: str) -> str:
        return f"upload-processing-state/{self.repoid}/{self.commitsha}/{state}"
