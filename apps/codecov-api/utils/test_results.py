import tempfile
from datetime import UTC, date, datetime, timedelta
from functools import lru_cache

import polars as pl
from django.conf import settings

from core.models import Commit
from services.task import TaskService
from shared.helpers.redis import get_redis_connection
from shared.metrics import Summary
from shared.storage import get_appropriate_storage_service
from shared.storage.exceptions import FileNotInStorageError

# Date after which commits are considered for new TA tasks implementation
NEW_TA_TASKS_CUTOFF_DATE = datetime(2025, 6, 20, tzinfo=UTC)

get_results_summary = Summary(
    "test_results_get_results", "Time it takes to download results from GCS", ["impl"]
)


def redis_key(
    repoid: int,
    branch: str,
    interval_start: int,
    interval_end: int | None = None,
) -> str:
    key = f"test_results:{repoid}:{branch}:{interval_start}"

    if interval_end is not None:
        key = f"{key}:{interval_end}"

    return key


def storage_key(
    repoid: int, branch: str, interval_start: int, interval_end: int | None = None
) -> str:
    key = f"test_results/rollups/{repoid}/{branch}/{interval_start}"

    if interval_end is not None:
        key = f"{key}_{interval_end}"

    return key


def dedup_table(table: pl.DataFrame) -> pl.DataFrame:
    total_tests = pl.col("total_fail_count") + pl.col("total_pass_count")
    total_time = pl.col("avg_duration") * total_tests

    failure_rate_expr = (pl.col("failure_rate") * total_tests).sum() / total_tests.sum()

    flake_rate_expr = (pl.col("flake_rate") * total_tests).sum() / total_tests.sum()

    avg_duration_expr = total_time.sum() / total_tests.sum()

    total_duration_expr = total_time.sum()

    # dedup
    table = (
        table.group_by("name")
        .agg(
            pl.col("testsuite").alias("testsuite"),
            pl.col("flags").explode().unique().alias("flags"),
            failure_rate_expr.fill_nan(0).alias("failure_rate"),
            flake_rate_expr.fill_nan(0).alias("flake_rate"),
            pl.col("updated_at").max().alias("updated_at"),
            total_duration_expr.alias("total_duration"),
            avg_duration_expr.fill_nan(0).alias("avg_duration"),
            pl.col("total_fail_count").sum().alias("total_fail_count"),
            pl.col("total_flaky_fail_count").sum().alias("total_flaky_fail_count"),
            pl.col("total_pass_count").sum().alias("total_pass_count"),
            pl.col("total_skip_count").sum().alias("total_skip_count"),
            pl.col("commits_where_fail")
            .sum()
            .alias("commits_where_fail"),  # TODO: this is wrong
            pl.col("last_duration").max().alias("last_duration"),
        )
        .sort("name")
    )

    return table


def _has_commits_before_cutoff(repoid: int) -> bool:
    """
    Check if the repository has any commits before the NEW_TA_TASKS_CUTOFF_DATE.
    Returns True if there are commits before the cutoff date, False otherwise.
    """
    return Commit.objects.filter(
        repository_id=repoid, timestamp__lt=NEW_TA_TASKS_CUTOFF_DATE
    ).exists()


@lru_cache
def use_new_impl(repoid: int) -> bool:
    """
    Check if we should use the new implementation
    Use new implementation if the repo has no commits before the cutoff date
    """
    return not _has_commits_before_cutoff(repoid)


def get_results(
    repoid: int,
    branch: str,
    interval_start: int,
    interval_end: int | None = None,
) -> pl.DataFrame | None:
    """
    try redis
    if redis is empty
        try storage
        if storage is empty
            return None
        else
            cache to redis
    deserialize
    """
    if use_new_impl(repoid):
        func = new_get_results
        label = "new"
    else:
        func = old_get_results
        label = "old"

    with get_results_summary.labels(label).time():
        return func(repoid, branch, interval_start, interval_end)


def old_get_results(
    repoid: int,
    branch: str,
    interval_start: int,
    interval_end: int | None = None,
) -> pl.DataFrame | None:
    redis_conn = get_redis_connection()
    key = redis_key(repoid, branch, interval_start, interval_end)
    result: bytes | None = redis_conn.get(key)  # type: ignore

    if result is None:
        # try storage
        storage_service = get_appropriate_storage_service(repoid)
        key = storage_key(repoid, branch, interval_start, interval_end)
        try:
            result = storage_service.read_file(
                bucket_name=settings.GCS_BUCKET_NAME, path=key
            )
            # cache to redis
            TaskService().cache_test_results_redis(repoid, branch)
        except FileNotInStorageError:
            # give up
            return None

    table = pl.read_ipc(result)

    if table.height == 0:
        return None

    table = dedup_table(table)

    return table


def rollup_blob_path(repoid: int, branch: str | None = None) -> str:
    return (
        f"test_analytics/branch_rollups/{repoid}/{branch}.arrow"
        if branch
        else f"test_analytics/repo_rollups/{repoid}.arrow"
    )


def no_version_agg_table(table: pl.LazyFrame) -> pl.LazyFrame:
    total_tests = pl.col("fail_count") + pl.col("pass_count")
    total_time = pl.col("avg_duration") * total_tests

    failure_rate_expr = (pl.col("fail_count")).sum() / total_tests.sum()

    flake_rate_expr = (pl.col("flaky_fail_count")).sum() / total_tests.sum()

    avg_duration_expr = total_time.sum() / total_tests.sum()

    total_duration_expr = total_time.sum()

    table = table.group_by(pl.col("computed_name").alias("name")).agg(
        pl.col("flags")
        .explode()
        .unique()
        .alias("flags"),  # TODO: filter by this before we aggregate
        pl.col("failing_commits").sum().alias("commits_where_fail"),
        pl.col("last_duration").max().alias("last_duration"),
        failure_rate_expr.alias("failure_rate"),
        flake_rate_expr.alias("flake_rate"),
        avg_duration_expr.alias("avg_duration"),
        total_duration_expr.alias("total_duration"),
        pl.col("pass_count").sum().alias("total_pass_count"),
        pl.col("fail_count").sum().alias("total_fail_count"),
        pl.col("flaky_fail_count").sum().alias("total_flaky_fail_count"),
        pl.col("skip_count").sum().alias("total_skip_count"),
        pl.col("updated_at").max().alias("updated_at"),
    )

    return table


def v1_agg_table(table: pl.LazyFrame) -> pl.LazyFrame:
    total_tests = pl.col("fail_count") + pl.col("pass_count")
    total_time = pl.col("avg_duration") * total_tests

    failure_rate_expr = (pl.col("fail_count")).sum() / total_tests.sum()

    flake_rate_expr = (pl.col("flaky_fail_count")).sum() / total_tests.sum()

    avg_duration_expr = total_time.sum() / total_tests.sum()

    total_duration_expr = total_time.sum()

    table = table.group_by(pl.col("computed_name").alias("name")).agg(
        pl.col("testsuite").alias(
            "testsuite"
        ),  # TODO: filter by this before we aggregate
        pl.col("flags")
        .explode()
        .unique()
        .alias("flags"),  # TODO: filter by this before we aggregate
        pl.col("failing_commits").sum().alias("commits_where_fail"),
        pl.col("last_duration").max().alias("last_duration"),
        failure_rate_expr.fill_nan(0).alias("failure_rate"),
        flake_rate_expr.fill_nan(0).alias("flake_rate"),
        avg_duration_expr.fill_nan(0).alias("avg_duration"),
        total_duration_expr.alias("total_duration"),
        pl.col("pass_count").sum().alias("total_pass_count"),
        pl.col("fail_count").sum().alias("total_fail_count"),
        pl.col("flaky_fail_count").sum().alias("total_flaky_fail_count"),
        pl.col("skip_count").sum().alias("total_skip_count"),
        pl.col("updated_at").max().alias("updated_at"),
    )

    return table


def new_get_results(
    repoid: int,
    branch: str | None,
    interval_start: int,
    interval_end: int | None = None,
) -> pl.DataFrame | None:
    storage_service = get_appropriate_storage_service(repoid)
    key = rollup_blob_path(repoid, branch)
    try:
        with tempfile.TemporaryFile() as tmp:
            metadata = {}
            storage_service.read_file(
                bucket_name=settings.GCS_BUCKET_NAME,
                path=key,
                file_obj=tmp,
                metadata_container=metadata,
            )

            table = pl.scan_ipc(tmp)

            # filter start
            start_date = date.today() - timedelta(days=interval_start)
            table = table.filter(pl.col("timestamp_bin") >= start_date)

            # filter end
            if interval_end is not None:
                end_date = date.today() - timedelta(days=interval_end)
                table = table.filter(pl.col("timestamp_bin") <= end_date)

            # aggregate
            match metadata.get("version"):
                case "1":
                    table = v1_agg_table(table)
                case _:  # no version is missding
                    table = no_version_agg_table(table)

            return table.collect()
    except FileNotInStorageError:
        return None
