from datetime import datetime
from typing import Literal, TypeIs

from django.db import connections

from shared.django_apps.ta_timeseries.models import (
    AggregateDaily,
    BranchAggregateDaily,
    BranchTestAggregateDaily,
    TestAggregateDaily,
)

PRECOMPUTED_BRANCHES: tuple[str, str, str] = ("main", "master", "develop")


def _should_use_precomputed_aggregates(
    branch: str | None,
) -> TypeIs[Literal["main", "master", "develop"] | None]:
    return branch in PRECOMPUTED_BRANCHES or branch is None


def _calculate_slow_test_num(total_tests: int) -> int:
    return min(100, max(total_tests // 20, 1)) if total_tests else 0


def _pct_change(current: int | float | None, past: int | float | None) -> float:
    if past is None or past == 0:
        return 0.0
    if current is None:
        current = 0

    return (current - past) / past


def get_daily_aggregate_querysets(
    repoid: int,
    branch: Literal["main", "master", "develop"] | None,
    start_date: datetime,
    end_date: datetime,
):
    if branch is None:
        test_data = TestAggregateDaily.objects.filter(  # type: ignore[attr-defined]
            repo_id=repoid,
            bucket_daily__gt=start_date,
            bucket_daily__lte=end_date,
        )
        repo_data = AggregateDaily.objects.filter(  # type: ignore[attr-defined]
            repo_id=repoid,
            bucket_daily__gt=start_date,
            bucket_daily__lte=end_date,
        )
    else:
        test_data = BranchTestAggregateDaily.objects.filter(  # type: ignore[attr-defined]
            repo_id=repoid,
            branch=branch,
            bucket_daily__gt=start_date,
            bucket_daily__lte=end_date,
        )
        repo_data = BranchAggregateDaily.objects.filter(  # type: ignore[attr-defined]
            repo_id=repoid,
            branch=branch,
            bucket_daily__gt=start_date,
            bucket_daily__lte=end_date,
        )

    return test_data, repo_data


def refresh_ta_timeseries_aggregates(
    min_timestamp: datetime | None = None,
    max_timestamp: datetime | None = None,
    connection_name: str = "ta_timeseries",
) -> None:
    with connections[connection_name].cursor() as cursor:
        cursor.execute(
            """
            DO $$
            DECLARE
                min_timestamp TIMESTAMP WITH TIME ZONE;
                max_timestamp TIMESTAMP WITH TIME ZONE;
            BEGIN
                min_timestamp := %s;
                max_timestamp := %s;
                
                CALL refresh_continuous_aggregate('ta_timeseries_test_aggregate_hourly', min_timestamp, max_timestamp, true);
                CALL refresh_continuous_aggregate('ta_timeseries_test_aggregate_daily', min_timestamp, max_timestamp, true);
                CALL refresh_continuous_aggregate('ta_timeseries_branch_test_aggregate_hourly', min_timestamp, max_timestamp, true);
                CALL refresh_continuous_aggregate('ta_timeseries_branch_test_aggregate_daily', min_timestamp, max_timestamp, true);

                CALL refresh_continuous_aggregate('ta_timeseries_aggregate_hourly', min_timestamp, max_timestamp, true);
                CALL refresh_continuous_aggregate('ta_timeseries_aggregate_daily', min_timestamp, max_timestamp, true);
                CALL refresh_continuous_aggregate('ta_timeseries_branch_aggregate_hourly', min_timestamp, max_timestamp, true);
                CALL refresh_continuous_aggregate('ta_timeseries_branch_aggregate_daily', min_timestamp, max_timestamp, true);
            END;
            $$;
            """,
            [min_timestamp, max_timestamp],
        )
