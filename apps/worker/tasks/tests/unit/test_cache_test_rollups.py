import datetime as dt

import polars as pl
from freezegun import freeze_time

from shared.django_apps.core.tests.factories import RepositoryFactory
from shared.django_apps.reports.models import LastCacheRollupDate
from shared.django_apps.reports.tests.factories import (
    DailyTestRollupFactory,
    LastCacheRollupDateFactory,
    RepositoryFlagFactory,
    TestFactory,
    TestFlagBridgeFactory,
)
from tasks.cache_test_rollups import CacheTestRollupsTask


def read_table(mock_storage, storage_path: str):
    decompressed_table: bytes = mock_storage.read_file("archive", storage_path)
    return pl.read_ipc(decompressed_table)


@freeze_time()
def test_cache_test_rollups(mock_storage, db):
    repo = RepositoryFactory()
    flag = RepositoryFlagFactory(
        repository=repo,
        flag_name="test-rollups",
    )
    flag2 = RepositoryFlagFactory(
        repository=repo,
        flag_name="test-rollups2",
    )
    test = TestFactory(repository=repo, testsuite="testsuite1")
    test2 = TestFactory(repository=repo, testsuite="testsuite2")
    test3 = TestFactory(repository=repo, testsuite="testsuite3")

    _ = TestFlagBridgeFactory(
        test=test,
        flag=flag,
    )
    _ = TestFlagBridgeFactory(
        test=test2,
        flag=flag2,
    )

    _ = DailyTestRollupFactory(
        test=test,
        commits_where_fail=["123", "456"],
        repoid=repo.repoid,
        branch="main",
        pass_count=1,
        date=dt.date.today(),
        latest_run=dt.datetime.now(dt.timezone.utc),
    )
    r = DailyTestRollupFactory(
        test=test2,
        repoid=repo.repoid,
        branch="main",
        pass_count=1,
        fail_count=1,
        date=dt.date.today() - dt.timedelta(days=6),
        commits_where_fail=["123"],
        latest_run=dt.datetime.now(dt.timezone.utc),
    )
    r.created_at = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=1)
    r.save()
    _ = DailyTestRollupFactory(
        test=test2,
        repoid=repo.repoid,
        branch="main",
        pass_count=0,
        fail_count=10,
        date=dt.date.today() - dt.timedelta(days=29),
        commits_where_fail=["123", "789"],
        latest_run=dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=29),
    )
    _ = DailyTestRollupFactory(
        test=test3,
        repoid=repo.repoid,
        branch="main",
        pass_count=0,
        fail_count=10,
        date=dt.date.today() - dt.timedelta(days=50),
        commits_where_fail=["123", "789"],
        latest_run=dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=50),
    )

    task = CacheTestRollupsTask()
    result = task.run_impl(_db_session=None, repo_id=repo.repoid, branch="main")
    assert result == {"success": True}

    storage_key = f"test_results/rollups/{repo.repoid}/main/1"
    table = read_table(mock_storage, storage_key)

    assert table.to_dict(as_series=False) == {
        "avg_duration": [0.0],
        "commits_where_fail": [2],
        "failure_rate": [0.0],
        "flags": [["test-rollups"]],
        "flake_rate": [0.0],
        "last_duration": [0.0],
        "name": [test.name],
        "test_id": [test.id],
        "testsuite": [test.testsuite],
        "total_fail_count": [0],
        "total_flaky_fail_count": [0],
        "total_pass_count": [1],
        "total_skip_count": [0],
        "updated_at": [dt.datetime.now(dt.timezone.utc)],
    }

    storage_key = f"test_results/rollups/{repo.repoid}/main/7"
    table = read_table(mock_storage, storage_key)

    assert table.to_dict(as_series=False) == {
        "avg_duration": [0.0, 0.0],
        "commits_where_fail": [2, 1],
        "failure_rate": [0.0, 0.5],
        "flags": [["test-rollups"], ["test-rollups2"]],
        "flake_rate": [0.0, 0.0],
        "last_duration": [0.0, 0.0],
        "name": [test.name, test2.name],
        "test_id": [test.id, test2.id],
        "testsuite": [test.testsuite, test2.testsuite],
        "total_fail_count": [0, 1],
        "total_flaky_fail_count": [0, 0],
        "total_pass_count": [1, 1],
        "total_skip_count": [0, 0],
        "updated_at": [
            dt.datetime.now(dt.timezone.utc),
            dt.datetime.now(dt.timezone.utc),
        ],
    }

    storage_key = f"test_results/rollups/{repo.repoid}/main/30"
    table = read_table(mock_storage, storage_key)

    assert table.to_dict(as_series=False) == {
        "avg_duration": [0.0, 0.0],
        "commits_where_fail": [2, 2],
        "failure_rate": [0.0, 0.9166666666666666],
        "flags": [["test-rollups"], ["test-rollups2"]],
        "flake_rate": [0.0, 0.0],
        "last_duration": [0.0, 0.0],
        "name": [test.name, test2.name],
        "test_id": [test.id, test2.id],
        "testsuite": [test.testsuite, test2.testsuite],
        "total_fail_count": [0, 11],
        "total_flaky_fail_count": [0, 0],
        "total_pass_count": [1, 1],
        "total_skip_count": [0, 0],
        "updated_at": [
            dt.datetime.now(dt.timezone.utc),
            dt.datetime.now(dt.timezone.utc),
        ],
    }

    storage_key = f"test_results/rollups/{repo.repoid}/main/60_30"
    table = read_table(mock_storage, storage_key)

    assert table.to_dict(as_series=False) == {
        "name": [test3.name],
        "test_id": [test3.id],
        "testsuite": [test3.testsuite],
        "flags": [None],
        "failure_rate": [1.0],
        "flake_rate": [0.0],
        "updated_at": [dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=50)],
        "avg_duration": [0.0],
        "total_fail_count": [10],
        "total_flaky_fail_count": [0],
        "total_pass_count": [0],
        "total_skip_count": [0],
        "commits_where_fail": [2],
        "last_duration": [0.0],
    }


@freeze_time()
def test_cache_test_rollups_no_update_date(mock_storage, db):
    repo = RepositoryFactory()
    rollup_date = LastCacheRollupDateFactory(
        repository=repo,
        last_rollup_date=dt.date.today() - dt.timedelta(days=30),
    )

    task = CacheTestRollupsTask()
    _ = task.run_impl(
        _db_session=None,
        repo_id=rollup_date.repository_id,
        branch=rollup_date.branch,
        update_date=False,
    )

    obj = LastCacheRollupDate.objects.filter(
        repository_id=repo.repoid, branch="main"
    ).first()
    assert obj.last_rollup_date == dt.date.today() - dt.timedelta(days=30)


@freeze_time()
def test_cache_test_rollups_update_date(mock_storage, db):
    repo = RepositoryFactory()

    rollup_date = LastCacheRollupDateFactory(
        repository=repo,
        last_rollup_date=dt.date.today() - dt.timedelta(days=1),
    )

    task = CacheTestRollupsTask()
    _ = task.run_impl(
        _db_session=None,
        repo_id=rollup_date.repository_id,
        branch="main",
        update_date=True,
    )

    obj = LastCacheRollupDate.objects.filter(
        repository_id=repo.repoid, branch="main"
    ).first()
    assert obj.last_rollup_date == dt.date.today()


@freeze_time()
def test_cache_test_rollups_update_date_does_not_exist(mock_storage, db):
    repo = RepositoryFactory()
    task = CacheTestRollupsTask()
    _ = task.run_impl(
        _db_session=None,
        repo_id=repo.repoid,
        branch="main",
        update_date=True,
    )

    obj = LastCacheRollupDate.objects.filter(
        repository_id=repo.repoid, branch="main"
    ).first()
    assert obj.last_rollup_date == dt.date.today()


@freeze_time()
def test_cache_test_rollups_both(mock_storage, db, mocker):
    mock_cache_rollups = mocker.patch("tasks.cache_test_rollups.cache_rollups")
    task = CacheTestRollupsTask()
    mocker.patch.object(task, "run_impl_within_lock")
    repo = RepositoryFactory()
    _ = task.run_impl(
        _db_session=None,
        repo_id=repo.repoid,
        branch="main",
        update_date=True,
        impl_type="both",
    )

    mock_cache_rollups.assert_has_calls(
        [
            mocker.call(repo.repoid, "main"),
            mocker.call(repo.repoid, None),
        ]
    )

    task.run_impl_within_lock.assert_called_once()
