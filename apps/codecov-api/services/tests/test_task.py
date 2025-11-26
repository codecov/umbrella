from datetime import datetime
from unittest.mock import MagicMock

import pytest
from django.conf import settings
from freezegun import freeze_time

from services.task import TaskService, celery_app
from shared import celery_config
from shared.django_apps.core.tests.factories import RepositoryFactory
from timeseries.tests.factories import DatasetFactory


def test_refresh_task(mocker):
    chain_mock = mocker.patch("services.task.task.chain")
    mock_route_task = mocker.patch(
        "services.task.task.route_task", return_value={"queue": "my_queue"}
    )
    TaskService().refresh(5, "codecov")
    chain_mock.assert_called()
    mock_route_task.assert_called()


def test_refresh_task_skips_sync_teams_when_using_integration(mocker):
    """
    Test that sync_teams is skipped when using_integration=True.
    This is necessary because sync_teams requires user OAuth tokens which
    may not be available when using GitHub App installations.
    """
    chain_mock = mocker.patch("services.task.task.chain")
    mock_route_task = mocker.patch(
        "services.task.task.route_task", return_value={"queue": "my_queue"}
    )
    mock_signature = mocker.patch("services.task.task.signature")

    # Call refresh with using_integration=True
    TaskService().refresh(5, "codecov", using_integration=True)

    # Verify chain was called
    chain_mock.assert_called_once()

    # Verify that only sync_repos signature was created (not sync_teams)
    assert mock_signature.call_count == 1
    mock_signature.assert_called_with(
        celery_config.sync_repos_task_name,
        args=None,
        kwargs={
            "ownerid": 5,
            "username": "codecov",
            "using_integration": True,
            "manual_trigger": False,
            "repository_service_ids": None,
        },
        app=celery_app,
        queue="my_queue",
        headers=mocker.ANY,
        immutable=False,
        time_limit=None,
        soft_time_limit=None,
    )


@freeze_time("2023-06-13T10:01:01.000123")
def test_compute_comparison_task(mocker):
    signature_mock = mocker.patch("services.task.task.signature")
    mock_route_task = mocker.patch(
        "services.task.task.route_task", return_value={"queue": "my_queue"}
    )
    TaskService().compute_comparison(5)
    mock_route_task.assert_called_with(
        celery_config.compute_comparison_task_name,
        args=None,
        kwargs={"comparison_id": 5},
    )
    signature_mock.assert_called_with(
        celery_config.compute_comparison_task_name,
        args=None,
        kwargs={"comparison_id": 5},
        app=celery_app,
        queue="my_queue",
        soft_time_limit=None,
        time_limit=None,
        headers={"created_timestamp": "2023-06-13T10:01:01.000123"},
        immutable=False,
    )


def test_compute_comparisons_task(mocker):
    signature_mock = mocker.patch("services.task.task.signature")
    mock_route_task = mocker.patch(
        "services.task.task.route_task", return_value={"queue": "my_queue"}
    )
    apply_async_mock = mocker.patch("celery.group.apply_async")
    TaskService().compute_comparisons([5, 10])
    assert mock_route_task.call_count == 1
    mock_route_task.assert_called_with(
        celery_config.compute_comparison_task_name,
        args=None,
        kwargs={"comparison_id": 5},
    )
    assert signature_mock.call_count == 2
    signature_mock.assert_any_call(
        celery_config.compute_comparison_task_name,
        args=None,
        kwargs={"comparison_id": 10},
        app=celery_app,
        queue="my_queue",
        soft_time_limit=None,
        time_limit=None,
    )
    signature_mock.assert_any_call(
        celery_config.compute_comparison_task_name,
        args=None,
        kwargs={"comparison_id": 5},
        app=celery_app,
        queue="my_queue",
        soft_time_limit=None,
        time_limit=None,
    )
    apply_async_mock.assert_called_once_with()


@pytest.mark.skipif(
    not settings.TIMESERIES_ENABLED, reason="requires timeseries data storage"
)
@pytest.mark.django_db(databases={"default", "timeseries"})
@freeze_time("2023-06-13T10:01:01.000123")
def test_backfill_repo(mocker):
    signature_mock = mocker.patch("services.task.task.signature")
    mock_route_task = mocker.patch(
        "services.task.task.route_task", return_value={"queue": "celery"}
    )
    apply_async_mock = mocker.patch("celery.group.apply_async")

    repo = RepositoryFactory()
    TaskService().backfill_repo(
        repo,
        start_date=datetime(2022, 1, 1),
        end_date=datetime(2022, 1, 25),
        dataset_names=["testing"],
    )

    assert signature_mock.call_count == 3
    assert mock_route_task.call_count == 3
    mock_route_task.assert_any_call(
        celery_config.timeseries_backfill_task_name,
        args=None,
        kwargs={
            "repoid": repo.pk,
            "start_date": "2022-01-15T00:00:00",
            "end_date": "2022-01-25T00:00:00",
            "dataset_names": ["testing"],
        },
    )

    signature_mock.assert_any_call(
        celery_config.timeseries_backfill_task_name,
        args=None,
        kwargs={
            "repoid": repo.pk,
            "start_date": "2022-01-15T00:00:00",
            "end_date": "2022-01-25T00:00:00",
            "dataset_names": ["testing"],
        },
        app=celery_app,
        queue="celery",
        soft_time_limit=None,
        time_limit=None,
        headers={"created_timestamp": "2023-06-13T10:01:01.000123"},
        immutable=False,
    )
    signature_mock.assert_any_call(
        celery_config.timeseries_backfill_task_name,
        args=None,
        kwargs={
            "repoid": repo.pk,
            "start_date": "2022-01-05T00:00:00",
            "end_date": "2022-01-15T00:00:00",
            "dataset_names": ["testing"],
        },
        app=celery_app,
        queue="celery",
        soft_time_limit=None,
        time_limit=None,
        headers={"created_timestamp": "2023-06-13T10:01:01.000123"},
        immutable=False,
    )
    signature_mock.assert_any_call(
        celery_config.timeseries_backfill_task_name,
        args=None,
        kwargs={
            "repoid": repo.pk,
            "start_date": "2022-01-01T00:00:00",
            "end_date": "2022-01-05T00:00:00",
            "dataset_names": ["testing"],
        },
        app=celery_app,
        queue="celery",
        soft_time_limit=None,
        time_limit=None,
        headers={"created_timestamp": "2023-06-13T10:01:01.000123"},
        immutable=False,
    )

    apply_async_mock.assert_called_once_with()


@pytest.mark.skipif(
    not settings.TIMESERIES_ENABLED, reason="requires timeseries data storage"
)
@pytest.mark.django_db(databases={"default", "timeseries"})
@freeze_time("2023-06-13T10:01:01.000123")
def test_backfill_dataset(mocker):
    signature_mock = mocker.patch("services.task.task.signature")
    mocker.patch("services.task.task.route_task", return_value={"queue": "celery"})
    signature = MagicMock()
    signature_mock.return_value = signature

    dataset = DatasetFactory()
    TaskService().backfill_dataset(
        dataset,
        start_date=datetime(2022, 1, 1),
        end_date=datetime(2022, 8, 9),
    )

    signature_mock.assert_called_with(
        "app.tasks.timeseries.backfill_dataset",
        args=None,
        kwargs={
            "dataset_id": dataset.pk,
            "start_date": "2022-01-01T00:00:00",
            "end_date": "2022-08-09T00:00:00",
        },
        app=celery_app,
        queue="celery",
        soft_time_limit=None,
        time_limit=None,
        headers={"created_timestamp": "2023-06-13T10:01:01.000123"},
        immutable=False,
    )
    signature.apply_async.assert_called_once_with()


@freeze_time("2023-06-13T10:01:01.000123")
def test_flush_repo(mocker):
    signature_mock = mocker.patch("services.task.task.signature")
    mock_route_task = mocker.patch(
        "services.task.task.route_task", return_value={"queue": "celery"}
    )
    TaskService().flush_repo(repository_id=12345)
    mock_route_task.assert_called_with(
        "app.tasks.flush_repo.FlushRepo",
        args=None,
        kwargs={"repoid": 12345},
    )
    signature_mock.assert_called_with(
        "app.tasks.flush_repo.FlushRepo",
        args=None,
        kwargs={"repoid": 12345},
        app=celery_app,
        queue="celery",
        soft_time_limit=None,
        time_limit=None,
        headers={"created_timestamp": "2023-06-13T10:01:01.000123"},
        immutable=False,
    )


@freeze_time("2023-06-13T10:01:01.000123")
def test_update_commit_task(mocker):
    signature_mock = mocker.patch("services.task.task.signature")
    mock_route_task = mocker.patch(
        "services.task.task.route_task",
        return_value={
            "queue": "celery",
            "extra_config": {"soft_timelimit": 300, "hard_timelimit": 400},
        },
    )
    TaskService().update_commit(1, 2)
    mock_route_task.assert_called_with(
        celery_config.commit_update_task_name,
        args=None,
        kwargs={"commitid": 1, "repoid": 2},
    )
    signature_mock.assert_called_with(
        celery_config.commit_update_task_name,
        args=None,
        kwargs={"commitid": 1, "repoid": 2},
        app=celery_app,
        queue="celery",
        soft_time_limit=300,
        time_limit=400,
        headers={"created_timestamp": "2023-06-13T10:01:01.000123"},
        immutable=False,
    )


@freeze_time("2023-06-13T10:01:01.000123")
def test_make_http_request_task(mocker):
    signature_mock = mocker.patch("services.task.task.signature")
    mock_route_task = mocker.patch(
        "services.task.task.route_task", return_value={"queue": "celery"}
    )
    TaskService().http_request(
        url="http://example.com",
        method="POST",
        headers={"Content-Type": "text/plain"},
        data="test body",
        timeout=10,
    )
    mock_route_task.assert_called_with(
        "app.tasks.http_request.HTTPRequest",
        args=None,
        kwargs={
            "url": "http://example.com",
            "method": "POST",
            "headers": {"Content-Type": "text/plain"},
            "data": "test body",
            "timeout": 10,
        },
    )
    signature_mock.assert_called_with(
        "app.tasks.http_request.HTTPRequest",
        args=None,
        kwargs={
            "url": "http://example.com",
            "method": "POST",
            "headers": {"Content-Type": "text/plain"},
            "data": "test body",
            "timeout": 10,
        },
        app=celery_app,
        queue="celery",
        soft_time_limit=None,
        time_limit=None,
        headers={"created_timestamp": "2023-06-13T10:01:01.000123"},
        immutable=False,
    )


@freeze_time("2023-06-13T10:01:01.000123")
def test_schedule_task_with_args(mocker):
    """Test that schedule_task supports both args and kwargs parameters."""
    signature_mock = mocker.patch("services.task.task.signature")
    mock_route_task = mocker.patch(
        "services.task.task.route_task", return_value={"queue": "celery"}
    )
    mock_signature = MagicMock()
    signature_mock.return_value = mock_signature

    TaskService().schedule_task(
        "test.task_name",
        args=["arg1", "arg2"],
        kwargs={"key1": "value1"},
        apply_async_kwargs={"countdown": 60},
    )

    mock_route_task.assert_called_with(
        "test.task_name",
        args=["arg1", "arg2"],
        kwargs={"key1": "value1"},
    )
    signature_mock.assert_called_with(
        "test.task_name",
        args=["arg1", "arg2"],
        kwargs={"key1": "value1"},
        app=celery_app,
        queue="celery",
        soft_time_limit=None,
        time_limit=None,
        headers={"created_timestamp": "2023-06-13T10:01:01.000123"},
        immutable=False,
    )
    mock_signature.apply_async.assert_called_once_with(countdown=60)


@freeze_time("2023-06-13T10:01:01.000123")
def test_schedule_task_without_args(mocker):
    """Test that schedule_task works without args (default None)."""
    signature_mock = mocker.patch("services.task.task.signature")
    mock_route_task = mocker.patch(
        "services.task.task.route_task", return_value={"queue": "celery"}
    )
    mock_signature = MagicMock()
    signature_mock.return_value = mock_signature

    TaskService().schedule_task(
        "test.task_name",
        kwargs={"key1": "value1"},
        apply_async_kwargs={},
    )

    mock_route_task.assert_called_with(
        "test.task_name",
        args=None,
        kwargs={"key1": "value1"},
    )
    signature_mock.assert_called_with(
        "test.task_name",
        args=None,
        kwargs={"key1": "value1"},
        app=celery_app,
        queue="celery",
        soft_time_limit=None,
        time_limit=None,
        headers={"created_timestamp": "2023-06-13T10:01:01.000123"},
        immutable=False,
    )
