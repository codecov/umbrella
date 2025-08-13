from unittest.mock import MagicMock, patch

import pytest

from shared.django_apps.core.models import Repository
from shared.django_apps.core.tests.factories import RepositoryFactory
from shared.django_apps.utils.paginator import EstimatedCountPaginator


@pytest.mark.django_db
class TestEstimatedCountPaginator:
    @patch("shared.django_apps.utils.paginator.connections")
    def test_count_with_no_filters_regular_table(
        self, mock_connections: MagicMock
    ) -> None:
        """Test count estimation for unfiltered queryset on regular (non-partitioned) table."""
        queryset = Repository.objects.all()

        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [
            [False],  # is_partitioned check returns False
            [1000],  # reltuples returns 1000
        ]
        mock_connections.__getitem__.return_value.cursor.return_value.__enter__.return_value = mock_cursor

        paginator = EstimatedCountPaginator(queryset, per_page=10)
        count = paginator.count

        assert count == 1000
        assert mock_cursor.execute.call_count == 2

        calls = mock_cursor.execute.call_args_list
        # First call should check if table is partitioned
        assert "pg_inherits" in calls[0][0][0]
        assert calls[0][0][1] == [Repository._meta.db_table]

        # Second call should get reltuples for regular table
        assert "reltuples FROM pg_class" in calls[1][0][0]
        assert calls[1][0][1] == [Repository._meta.db_table]

    @patch("shared.django_apps.utils.paginator.connections")
    def test_count_with_no_filters_partitioned_table(
        self, mock_connections: MagicMock
    ) -> None:
        """Test count estimation for unfiltered queryset on partitioned table."""
        queryset = Repository.objects.all()

        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [
            [True],  # is_partitioned check returns True
            [2500],  # For partitioned table query
        ]
        mock_connections.__getitem__.return_value.cursor.return_value.__enter__.return_value = mock_cursor

        paginator = EstimatedCountPaginator(queryset, per_page=25)
        count = paginator.count

        assert count == 2500
        assert mock_cursor.execute.call_count == 2

        calls = mock_cursor.execute.call_args_list
        # First call should check if table is partitioned
        assert "pg_inherits" in calls[0][0][0]
        assert calls[0][0][1] == [Repository._meta.db_table]

        # Second call should sum estimates across partitions
        assert "SUM(c.reltuples)" in calls[1][0][0]
        assert calls[1][0][1] == [Repository._meta.db_table]

    @patch("shared.django_apps.utils.paginator.connections")
    def test_count_with_null_result(self, mock_connections: MagicMock) -> None:
        """Test count estimation when database returns null result."""
        queryset = Repository.objects.all()

        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [
            [False],  # is_partitioned check returns False
            [None],  # reltuples returns None
        ]
        mock_connections.__getitem__.return_value.cursor.return_value.__enter__.return_value = mock_cursor

        paginator = EstimatedCountPaginator(queryset, per_page=10)
        count = paginator.count

        assert count == 0

    @patch("shared.django_apps.utils.paginator.connections")
    def test_count_with_empty_result(self, mock_connections: MagicMock) -> None:
        """Test count estimation when database returns empty result."""
        queryset = Repository.objects.all()

        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [
            [False],  # is_partitioned check returns False
            None,  # fetchone returns None (no result)
        ]
        mock_connections.__getitem__.return_value.cursor.return_value.__enter__.return_value = mock_cursor

        paginator = EstimatedCountPaginator(queryset, per_page=10)
        count = paginator.count

        assert count == 0

    def test_count_with_filters_falls_back_to_parent(self) -> None:
        """Test that count falls back to parent implementation when queryset has filters."""
        RepositoryFactory.create_batch(5)

        queryset = Repository.objects.filter(name__icontains="test")

        paginator = EstimatedCountPaginator(queryset, per_page=10)

        with patch.object(
            EstimatedCountPaginator.__bases__[0], "count", new_callable=lambda: 3
        ):
            count = paginator.count
            # The count should come from parent class, not our estimation logic
            assert count == 3

    @patch("shared.django_apps.utils.paginator.connections")
    def test_count_with_database_exception_falls_back_to_parent(
        self, mock_connections: MagicMock
    ) -> None:
        """Test that count falls back to parent implementation when database exception occurs."""
        queryset = Repository.objects.all()

        mock_connections.__getitem__.return_value.cursor.side_effect = Exception(
            "Database error"
        )

        paginator = EstimatedCountPaginator(queryset, per_page=10)

        # Should fall back to parent implementation
        with patch.object(
            EstimatedCountPaginator.__bases__[0], "count", new_callable=lambda: 42
        ):
            count = paginator.count
            assert count == 42

    @patch("shared.django_apps.utils.paginator.connections")
    def test_count_with_cursor_exception_falls_back_to_parent(
        self, mock_connections: MagicMock
    ) -> None:
        """Test that count falls back to parent implementation when cursor operation fails."""
        queryset = Repository.objects.all()

        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("SQL error")
        mock_connections.__getitem__.return_value.cursor.return_value.__enter__.return_value = mock_cursor

        paginator = EstimatedCountPaginator(queryset, per_page=10)

        # Should fall back to parent implementation
        with patch.object(
            EstimatedCountPaginator.__bases__[0], "count", new_callable=lambda: 99
        ):
            count = paginator.count
            assert count == 99

    def test_count_property_is_cached(self) -> None:
        """Test that the count property is cached and not recalculated on subsequent accesses."""
        RepositoryFactory.create_batch(3)

        queryset = Repository.objects.all()
        paginator = EstimatedCountPaginator(queryset, per_page=10)

        # First access
        count1 = paginator.count

        # Second access should return same cached value
        count2 = paginator.count

        assert count1 == count2
        # Check that the value is cached - Django's cached_property stores the value
        # in the instance dict with the property name
        assert hasattr(paginator, "__dict__") and "count" in paginator.__dict__

    @patch("shared.django_apps.utils.paginator.connections")
    def test_different_database_aliases(self, mock_connections: MagicMock) -> None:
        """Test that paginator uses correct database alias from queryset."""
        queryset = Repository.objects.using("default").all()

        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [
            [False],  # is_partitioned check returns False
            [500],  # reltuples returns 500
        ]
        mock_connections.__getitem__.return_value.cursor.return_value.__enter__.return_value = mock_cursor

        paginator = EstimatedCountPaginator(queryset, per_page=10)
        count = paginator.count

        # Verify the correct database was accessed
        mock_connections.__getitem__.assert_called_with("default")
        assert count == 500

    @patch("shared.django_apps.utils.paginator.connections")
    def test_float_estimate_converted_to_int(self, mock_connections: MagicMock) -> None:
        """Test that float estimates from database are properly converted to integers."""
        queryset = Repository.objects.all()

        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [
            [False],  # is_partitioned check returns False
            [1000.75],  # reltuples returns a float
        ]
        mock_connections.__getitem__.return_value.cursor.return_value.__enter__.return_value = mock_cursor

        paginator = EstimatedCountPaginator(queryset, per_page=10)
        count = paginator.count

        assert count == 1000
        assert isinstance(count, int)
