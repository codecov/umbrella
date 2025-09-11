from datetime import UTC, datetime, timedelta

from django.db import connections
from django.db.models import Q

from shared.django_apps.ta_timeseries.models import TestAggregateDaily


def get_test_suites(
    repoid: int, term: str | None = None, interval: int = 30
) -> list[str]:
    end_date = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=interval)

    q_filter = (
        Q(repo_id=repoid)
        & Q(bucket_daily__gte=start_date)
        & Q(bucket_daily__lt=end_date)
    )

    if term:
        q_filter &= Q(testsuite__istartswith=term)

    testsuites = (
        TestAggregateDaily.objects.filter(q_filter)
        .values_list("testsuite", flat=True)
        .distinct()[:200]
    )

    return list(testsuites)


def get_flags(repoid: int, term: str | None = None, interval: int = 30) -> list[str]:
    end_date = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=interval)

    with connections["ta_timeseries"].cursor() as cursor:
        if term:
            base_query = """
                SELECT DISTINCT flag
                FROM (
                    SELECT unnest(flags) as flag
                    FROM ta_timeseries_test_aggregate_daily
                    WHERE repo_id = %s
                    AND bucket_daily >= %s
                    AND bucket_daily < %s
                    AND flags IS NOT NULL
                ) unnested_flags
                WHERE flag ILIKE %s
                ORDER BY flag LIMIT 200
            """
            params = [repoid, start_date, end_date, f"{term}%"]
        else:
            base_query = """
                SELECT DISTINCT unnest(flags) as flag
                FROM ta_timeseries_test_aggregate_daily
                WHERE repo_id = %s
                AND bucket_daily >= %s
                AND bucket_daily < %s
                AND flags IS NOT NULL
                ORDER BY flag LIMIT 200
            """
            params = [repoid, start_date, end_date]

        cursor.execute(base_query, params)
        flags = [row[0] for row in cursor.fetchall()]

    return flags
