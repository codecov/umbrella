from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from database.tests.factories.core import (
    CommitFactory,
    ReportFactory,
    RepositoryFactory,
    UploadFactory,
)
from services.processing.state import (
    ProcessingState,
    should_perform_merge,
    should_trigger_postprocessing,
)
from shared.reports.enums import UploadState


def test_single_upload():
    state = ProcessingState(1234, uuid4().hex)
    state.mark_uploads_as_processing([1])

    state.mark_upload_as_processed(1)

    # this is the only in-progress upload, nothing more to expect
    assert should_perform_merge(state.get_upload_numbers())

    assert state.get_uploads_for_merging() == {1}
    state.mark_uploads_as_merged([1])

    assert should_trigger_postprocessing(state.get_upload_numbers())


def test_concurrent_uploads():
    state = ProcessingState(1234, uuid4().hex)
    state.mark_uploads_as_processing([1])

    state.mark_upload_as_processed(1)
    # meanwhile, another upload comes in:
    state.mark_uploads_as_processing([2])

    # not merging/postprocessing yet, as that will be debounced with the second upload
    assert not should_perform_merge(state.get_upload_numbers())

    state.mark_upload_as_processed(2)

    assert should_perform_merge(state.get_upload_numbers())

    assert state.get_uploads_for_merging() == {1, 2}
    state.mark_uploads_as_merged([1, 2])

    assert should_trigger_postprocessing(state.get_upload_numbers())


def test_batch_merging_many_uploads():
    state = ProcessingState(1234, uuid4().hex)

    state.mark_uploads_as_processing([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])

    for id in range(1, 12):
        state.mark_upload_as_processed(id)

    # we have only processed 8 out of 9. we want to do a batched merge
    assert should_perform_merge(state.get_upload_numbers())
    merging = state.get_uploads_for_merging()
    assert len(merging) == 10  # = MERGE_BATCH_SIZE
    state.mark_uploads_as_merged(merging)

    # but no notifications yet
    assert not should_trigger_postprocessing(state.get_upload_numbers())

    state.mark_upload_as_processed(12)

    # with the last upload being processed, we do another merge, and then trigger notifications
    assert should_perform_merge(state.get_upload_numbers())
    merging = state.get_uploads_for_merging()
    assert len(merging) == 2
    state.mark_uploads_as_merged(merging)

    assert should_trigger_postprocessing(state.get_upload_numbers())


class TestProcessingStateEmptyListGuards:
    """Tests for empty list guards in ProcessingState methods."""

    @pytest.fixture
    def mock_redis(self):
        """Create a mock Redis connection."""
        with patch("services.processing.state.get_redis_connection") as mock_get_redis:
            mock_redis = MagicMock()
            mock_get_redis.return_value = mock_redis
            yield mock_redis

    def test_mark_uploads_as_processing_empty_list(self, mock_redis):
        """Test that mark_uploads_as_processing handles empty list gracefully."""
        state = ProcessingState(1234, uuid4().hex)
        state._redis = mock_redis

        # Should not call Redis when empty list is passed
        state.mark_uploads_as_processing([])

        mock_redis.sadd.assert_not_called()
        mock_redis.expire.assert_not_called()

    def test_mark_uploads_as_processing_non_empty_list(self, mock_redis):
        """Test that mark_uploads_as_processing works with non-empty list."""
        state = ProcessingState(1234, uuid4().hex)
        state._redis = mock_redis

        state.mark_uploads_as_processing([1, 2, 3])

        mock_redis.sadd.assert_called_once()
        mock_redis.expire.assert_called()

    def test_clear_in_progress_uploads_empty_list(self, mock_redis):
        """Test that clear_in_progress_uploads handles empty list gracefully."""
        state = ProcessingState(1234, uuid4().hex)
        state._redis = mock_redis

        # Should not call Redis when empty list is passed
        state.clear_in_progress_uploads([])

        mock_redis.srem.assert_not_called()

    def test_clear_in_progress_uploads_non_empty_list(self, mock_redis):
        """Test that clear_in_progress_uploads works with non-empty list."""
        state = ProcessingState(1234, uuid4().hex)
        state._redis = mock_redis
        mock_redis.srem.return_value = 3

        state.clear_in_progress_uploads([1, 2, 3])

        mock_redis.srem.assert_called_once()

    def test_mark_uploads_as_merged_empty_list(self, mock_redis):
        """Test that mark_uploads_as_merged handles empty list gracefully."""
        state = ProcessingState(1234, uuid4().hex)
        state._redis = mock_redis

        # Should not call Redis when empty list is passed
        state.mark_uploads_as_merged([])

        mock_redis.srem.assert_not_called()

    def test_mark_uploads_as_merged_non_empty_list(self, mock_redis):
        """Test that mark_uploads_as_merged works with non-empty list."""
        state = ProcessingState(1234, uuid4().hex)
        state._redis = mock_redis

        state.mark_uploads_as_merged([1, 2, 3])

        mock_redis.srem.assert_called_once()


@pytest.mark.parametrize(
    "method_name,upload_ids,should_call_redis",
    [
        ("mark_uploads_as_processing", [], False),
        ("mark_uploads_as_processing", [1], True),
        ("mark_uploads_as_processing", [1, 2, 3], True),
        ("clear_in_progress_uploads", [], False),
        ("clear_in_progress_uploads", [1], True),
        ("clear_in_progress_uploads", [1, 2, 3], True),
        ("mark_uploads_as_merged", [], False),
        ("mark_uploads_as_merged", [1], True),
        ("mark_uploads_as_merged", [1, 2, 3], True),
    ],
)
def test_empty_list_guards_parametrized(method_name, upload_ids, should_call_redis):
    """Parametrized test for empty list guards across all methods."""
    with patch("services.processing.state.get_redis_connection") as mock_get_redis:
        mock_redis = MagicMock()
        mock_get_redis.return_value = mock_redis

        # For clear_in_progress_uploads, srem needs to return a value
        mock_redis.srem.return_value = len(upload_ids) if upload_ids else 0

        state = ProcessingState(1234, uuid4().hex)
        state._redis = mock_redis

        # Call the method
        method = getattr(state, method_name)
        method(upload_ids)

        # Check if Redis was called based on expected behavior
        if should_call_redis:
            assert mock_redis.method_calls, f"{method_name} should call Redis"
        else:
            # For empty lists, only get_redis_connection is called (in __init__),
            # but no actual operations should happen
            assert mock_redis.sadd.call_count == 0, (
                f"{method_name} should not call sadd"
            )
            if method_name != "mark_uploads_as_processing":
                assert mock_redis.srem.call_count == 0, (
                    f"{method_name} should not call srem"
                )


@pytest.mark.django_db(databases={"default"})
class TestProcessingStateDBPath:
    """Tests for the DB-backed path of ProcessingState (when db_session is provided)."""

    @pytest.fixture
    def setup_commit(self, dbsession):
        repository = RepositoryFactory.create()
        dbsession.add(repository)
        dbsession.flush()
        commit = CommitFactory.create(repository=repository)
        dbsession.add(commit)
        dbsession.flush()
        report = ReportFactory.create(commit=commit)
        dbsession.add(report)
        dbsession.flush()
        return repository, commit, report

    def _create_upload(self, dbsession, report, state_id):
        upload = UploadFactory.create(
            report=report,
            state="uploaded",
            state_id=state_id,
        )
        dbsession.add(upload)
        dbsession.flush()
        return upload

    def test_get_upload_numbers_empty(self, dbsession, setup_commit):
        _, commit, _ = setup_commit
        state = ProcessingState(commit.repoid, commit.commitid, db_session=dbsession)
        numbers = state.get_upload_numbers()
        assert numbers.processing == 0
        assert numbers.processed == 0

    def test_get_upload_numbers_with_uploads(self, dbsession, setup_commit):
        _, commit, report = setup_commit
        self._create_upload(dbsession, report, UploadState.UPLOADED.db_id)
        self._create_upload(dbsession, report, UploadState.UPLOADED.db_id)
        self._create_upload(dbsession, report, UploadState.PROCESSED.db_id)

        state = ProcessingState(commit.repoid, commit.commitid, db_session=dbsession)
        numbers = state.get_upload_numbers()
        assert numbers.processing == 2
        assert numbers.processed == 1

    def test_mark_upload_as_processed(self, dbsession, setup_commit):
        _, commit, report = setup_commit
        upload = self._create_upload(dbsession, report, UploadState.UPLOADED.db_id)

        state = ProcessingState(commit.repoid, commit.commitid, db_session=dbsession)
        state.mark_upload_as_processed(upload.id_)
        dbsession.flush()

        dbsession.refresh(upload)
        assert upload.state_id == UploadState.PROCESSED.db_id
        # state string is intentionally NOT set here -- the finisher's
        # idempotency check uses state="processed" to detect already-merged uploads
        assert upload.state == "uploaded"

    def test_mark_uploads_as_merged(self, dbsession, setup_commit):
        _, commit, report = setup_commit
        u1 = self._create_upload(dbsession, report, UploadState.PROCESSED.db_id)
        u2 = self._create_upload(dbsession, report, UploadState.PROCESSED.db_id)

        state = ProcessingState(commit.repoid, commit.commitid, db_session=dbsession)
        state.mark_uploads_as_merged([u1.id_, u2.id_])
        dbsession.flush()

        dbsession.refresh(u1)
        dbsession.refresh(u2)
        assert u1.state_id == UploadState.MERGED.db_id
        assert u1.state == "merged"
        assert u2.state_id == UploadState.MERGED.db_id
        assert u2.state == "merged"

    def test_mark_uploads_as_merged_empty_list(self, dbsession, setup_commit):
        _, commit, _ = setup_commit
        state = ProcessingState(commit.repoid, commit.commitid, db_session=dbsession)
        state.mark_uploads_as_merged([])

    def test_get_uploads_for_merging(self, dbsession, setup_commit):
        _, commit, report = setup_commit
        u1 = self._create_upload(dbsession, report, UploadState.PROCESSED.db_id)
        u2 = self._create_upload(dbsession, report, UploadState.PROCESSED.db_id)
        self._create_upload(dbsession, report, UploadState.UPLOADED.db_id)
        self._create_upload(dbsession, report, UploadState.MERGED.db_id)

        state = ProcessingState(commit.repoid, commit.commitid, db_session=dbsession)
        merging = state.get_uploads_for_merging()
        assert merging == {u1.id_, u2.id_}

    def test_get_uploads_for_merging_respects_batch_size(self, dbsession, setup_commit):
        _, commit, report = setup_commit
        for _ in range(15):
            self._create_upload(dbsession, report, UploadState.PROCESSED.db_id)

        state = ProcessingState(commit.repoid, commit.commitid, db_session=dbsession)
        merging = state.get_uploads_for_merging()
        assert len(merging) == 10

    def test_mark_uploads_as_processing_is_noop(self, dbsession, setup_commit):
        _, commit, report = setup_commit
        upload = self._create_upload(dbsession, report, UploadState.UPLOADED.db_id)

        state = ProcessingState(commit.repoid, commit.commitid, db_session=dbsession)
        state.mark_uploads_as_processing([upload.id_])

        dbsession.refresh(upload)
        assert upload.state_id == UploadState.UPLOADED.db_id

    def test_clear_in_progress_uploads_sets_error_on_uploaded(
        self, dbsession, setup_commit
    ):
        """Uploaded uploads are set to ERROR so they stop counting as 'processing'."""
        _, commit, report = setup_commit
        upload = self._create_upload(dbsession, report, UploadState.UPLOADED.db_id)

        state = ProcessingState(commit.repoid, commit.commitid, db_session=dbsession)
        state.clear_in_progress_uploads([upload.id_])

        dbsession.refresh(upload)
        assert upload.state_id == UploadState.ERROR.db_id
        assert upload.state == "error"

    def test_clear_in_progress_uploads_skips_processed(self, dbsession, setup_commit):
        """Already-processed uploads are not affected (success path in finally block)."""
        _, commit, report = setup_commit
        upload = self._create_upload(dbsession, report, UploadState.PROCESSED.db_id)

        state = ProcessingState(commit.repoid, commit.commitid, db_session=dbsession)
        state.clear_in_progress_uploads([upload.id_])

        dbsession.refresh(upload)
        assert upload.state_id == UploadState.PROCESSED.db_id

    def test_full_lifecycle(self, dbsession, setup_commit):
        """End-to-end: UPLOADED -> PROCESSED -> MERGED with DB state."""
        _, commit, report = setup_commit
        u1 = self._create_upload(dbsession, report, UploadState.UPLOADED.db_id)
        u2 = self._create_upload(dbsession, report, UploadState.UPLOADED.db_id)

        state = ProcessingState(commit.repoid, commit.commitid, db_session=dbsession)

        numbers = state.get_upload_numbers()
        assert numbers.processing == 2
        assert numbers.processed == 0

        state.mark_upload_as_processed(u1.id_)
        dbsession.flush()

        numbers = state.get_upload_numbers()
        assert numbers.processing == 1
        assert numbers.processed == 1
        assert not should_perform_merge(numbers)

        state.mark_upload_as_processed(u2.id_)
        dbsession.flush()

        numbers = state.get_upload_numbers()
        assert numbers.processing == 0
        assert numbers.processed == 2
        assert should_perform_merge(numbers)

        merging = state.get_uploads_for_merging()
        assert merging == {u1.id_, u2.id_}

        state.mark_uploads_as_merged(list(merging))
        dbsession.flush()

        numbers = state.get_upload_numbers()
        assert numbers.processing == 0
        assert numbers.processed == 0
        assert should_trigger_postprocessing(numbers)
