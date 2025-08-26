from __future__ import annotations

from datetime import UTC, datetime, timedelta

import polars as pl
from django.db import connections
from django.db.models import Q

from rollouts import READ_NEW_TA
from shared.django_apps.core.models import Repository
from shared.django_apps.prevent_timeseries import models as prevent_ts_models
from shared.django_apps.ta_timeseries import models as ta_ts_models
from utils.test_results import get_results, use_new_impl


def get_flags_from_timescale(
    *,
    repoid: int,
    start_date: datetime,
    end_date: datetime,
    term: str | None = None,
    use_prevent: bool = False,
) -> list[str]:
    table = (
        "prevent_timeseries_test_aggregate_daily"
        if use_prevent
        else "ta_timeseries_test_aggregate_daily"
    )

    with connections["ta_timeseries"].cursor() as cursor:
        if term:
            base_query = f"""
                SELECT DISTINCT flag
                FROM (
                    SELECT unnest(flags) as flag
                    FROM {table}
                    WHERE repo_id = %s
                    AND bucket_daily > %s
                    AND bucket_daily <= %s
                    AND flags IS NOT NULL
                ) unnested_flags
                WHERE flag ILIKE %s
                ORDER BY flag LIMIT 200
            """
            params = [repoid, start_date, end_date, f"{term}%"]
        else:
            base_query = f"""
                SELECT DISTINCT unnest(flags) as flag
                FROM {table}
                WHERE repo_id = %s
                AND bucket_daily > %s
                AND bucket_daily <= %s
                AND flags IS NOT NULL
                ORDER BY flag LIMIT 200
            """
            params = [repoid, start_date, end_date]

        cursor.execute(base_query, params)
        flags = [row[0] for row in cursor.fetchall()]

    return flags


def get_test_suites_old(
    repoid: int, term: str | None = None, interval: int = 30
) -> list[str]:
    repo = Repository.objects.get(repoid=repoid)

    table = get_results(repoid, repo.branch, interval)
    if table is None:
        return []

    testsuites = table.select(pl.col("testsuite").explode()).unique()

    if term:
        testsuites = testsuites.filter(pl.col("testsuite").str.starts_with(term))

    return testsuites.to_series().drop_nulls().to_list() or []


def get_test_suites_new(
    repoid: int,
    term: str | None = None,
    interval: int = 30,
    *,
    use_prevent: bool = False,
) -> list[str]:
    end_date = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=interval)

    q_filter = (
        Q(repo_id=repoid)
        & Q(bucket_daily__gt=start_date)
        & Q(bucket_daily__lte=end_date)
    )

    if term:
        q_filter &= Q(testsuite__istartswith=term)

    models = prevent_ts_models if use_prevent else ta_ts_models
    testsuites = (
        models.TestAggregateDaily.objects.filter(q_filter)  # type: ignore[attr-defined]
        .values_list("testsuite", flat=True)
        .distinct()[:200]
    )

    return list(testsuites)


def get_test_suites(
    repoid: int,
    term: str | None = None,
    interval: int = 30,
    *,
    use_prevent: bool = False,
) -> list[str]:
    if READ_NEW_TA.check_value(repoid) or use_new_impl(repoid):
        return get_test_suites_new(repoid, term, interval, use_prevent=use_prevent)
    else:
        return get_test_suites_old(repoid, term, interval)


def get_flags_old(
    repoid: int, term: str | None = None, interval: int = 30
) -> list[str]:
    repo = Repository.objects.get(repoid=repoid)

    table = get_results(repoid, repo.branch, interval)
    if table is None:
        return []

    flags = table.select(pl.col("flags").explode()).unique()

    if term:
        flags = flags.filter(pl.col("flags").str.starts_with(term))

    return flags.to_series().drop_nulls().to_list() or []


def get_flags_new(
    repoid: int,
    term: str | None = None,
    interval: int = 30,
    *,
    use_prevent: bool = False,
) -> list[str]:
    end_date = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=interval)
    return get_flags_from_timescale(
        repoid=repoid,
        start_date=start_date,
        end_date=end_date,
        term=term,
        use_prevent=use_prevent,
    )


def get_flags(
    repoid: int,
    term: str | None = None,
    interval: int = 30,
    *,
    use_prevent: bool = False,
) -> list[str]:
    if READ_NEW_TA.check_value(repoid) or use_new_impl(repoid):
        return get_flags_new(repoid, term, interval, use_prevent=use_prevent)
    else:
        return get_flags_old(repoid, term, interval)
