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

from shared.helpers.redis import get_redis_connection
from shared.metrics import Counter

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
    def __init__(self, repoid: int, commitsha: str) -> None:
        self._redis = get_redis_connection()
        self.repoid = repoid
        self.commitsha = commitsha

    def get_upload_numbers(self):
        processing = self._redis.scard(self._redis_key("processing"))
        processed = self._redis.scard(self._redis_key("processed"))
        return UploadNumbers(processing, processed)

    def mark_uploads_as_processing(self, upload_ids: list[int]):
        if not upload_ids:
            return
        key = self._redis_key("processing")
        self._redis.sadd(key, *upload_ids)
        # Set TTL to match intermediate report expiration (24 hours)
        # This ensures state keys don't accumulate indefinitely
        self._redis.expire(key, PROCESSING_STATE_TTL)

    def clear_in_progress_uploads(self, upload_ids: list[int]):
        if not upload_ids:
            return
        removed_uploads = self._redis.srem(self._redis_key("processing"), *upload_ids)
        if removed_uploads > 0:
            # the normal flow would move the uploads from the "processing" set
            # to the "processed" set via `mark_upload_as_processed`.
            # this function here is only called in the error case and we don't expect
            # this to be triggered often, if at all.
            CLEARED_UPLOADS.inc(removed_uploads)

    def mark_upload_as_processed(self, upload_id: int):
        processing_key = self._redis_key("processing")
        processed_key = self._redis_key("processed")

        res = self._redis.smove(processing_key, processed_key, upload_id)
        if not res:
            # this can happen when `upload_id` was never in the source set,
            # which probably is the case during initial deployment as
            # the code adding this to the initial set was not deployed yet
            # TODO: make sure to remove this code after a grace period
            self._redis.sadd(processed_key, upload_id)

        # Set TTL on processed key to match intermediate report expiration
        # This ensures uploads marked as processed have a bounded lifetime
        self._redis.expire(processed_key, PROCESSING_STATE_TTL)

    def mark_uploads_as_merged(self, upload_ids: list[int]):
        if not upload_ids:
            return
        self._redis.srem(self._redis_key("processed"), *upload_ids)

    def get_uploads_for_merging(self) -> set[int]:
        return {
            int(id)
            for id in self._redis.srandmember(
                self._redis_key("processed"), MERGE_BATCH_SIZE
            )
        }

    def get_all_processed_uploads(self) -> set[int]:
        """Return ALL upload IDs in the 'processed' set (no batch limit).

        Used by the cooperative finisher to merge every pending upload in one
        lock acquisition instead of processing them in MERGE_BATCH_SIZE chunks.
        """
        return {int(id) for id in self._redis.smembers(self._redis_key("processed"))}

    def _redis_key(self, state: str) -> str:
        return f"upload-processing-state/{self.repoid}/{self.commitsha}/{state}"
