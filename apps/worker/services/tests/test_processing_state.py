from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from services.processing.state import (
    ProcessingState,
    should_perform_merge,
    should_trigger_postprocessing,
)


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


def test_get_all_processed_uploads_returns_full_set():
    """get_all_processed_uploads returns every upload without the MERGE_BATCH_SIZE cap."""
    state = ProcessingState(1234, uuid4().hex)
    ids = list(range(1, 25))
    state.mark_uploads_as_processing(ids)
    for i in ids:
        state.mark_upload_as_processed(i)

    # get_uploads_for_merging is capped at MERGE_BATCH_SIZE (10)
    assert len(state.get_uploads_for_merging()) <= 10

    # get_all_processed_uploads returns everything
    assert state.get_all_processed_uploads() == set(ids)


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
