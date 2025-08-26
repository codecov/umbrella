from datetime import timedelta
from unittest.mock import patch

import pytest
from celery.exceptions import SoftTimeLimitExceeded
from django.utils import timezone

from shared.django_apps.core.tests.factories import RepositoryFactory
from shared.django_apps.prevent_timeseries.models import Testrun
from shared.django_apps.test_analytics.models import Flake, TAUpload
from tasks import detect_flakes as df

pytestmark = pytest.mark.django_db(
    databases=["default", "ta_timeseries"], transaction=True
)


@pytest.fixture
def repository():
    repo = RepositoryFactory()
    return repo.repoid


@pytest.fixture
def upload_factory(repository):
    def _create_upload() -> TAUpload:
        return TAUpload.objects.create(repo_id=repository)

    return _create_upload


@pytest.fixture
def testrun_factory(repository):
    def _create_testrun(
        *,
        upload_id: int,
        test_id: bytes,
        outcome: str,
        timestamp=None,
    ) -> Testrun:
        return Testrun.objects.create(
            timestamp=timestamp or timezone.now(),
            test_id=test_id,
            outcome=outcome,
            repo_id=repository,
            commit_sha="commit",
            branch="main",
            upload_id=upload_id,
        )

    return _create_testrun


@pytest.fixture
def flake_factory(repository):
    def _create_flake(
        *,
        test_id: bytes,
        recent_passes_count: int = 0,
        count: int = 0,
        fail_count: int = 0,
        start_date=None,
        end_date=None,
    ) -> Flake:
        return Flake.objects.create(
            repoid=repository,
            test_id=test_id,
            recent_passes_count=recent_passes_count,
            count=count,
            fail_count=fail_count,
            start_date=start_date or timezone.now(),
            end_date=end_date,
        )

    return _create_flake


def test_detect_flakes(repository, upload_factory, testrun_factory, flake_factory):
    up1 = upload_factory()
    up2 = upload_factory()

    flake_factory(
        test_id=b"t-existing-fail",
        recent_passes_count=0,
        count=5,
        fail_count=2,
        start_date=timezone.now() - timedelta(days=2),
    )

    flake_factory(
        test_id=b"t-expire",
        recent_passes_count=29,
        count=29,
        fail_count=1,
        start_date=timezone.now() - timedelta(days=2),
    )

    testrun_factory(upload_id=up1.id, test_id=b"t-new", outcome="failure")
    testrun_factory(upload_id=up1.id, test_id=b"t-existing-fail", outcome="failure")
    testrun_factory(upload_id=up1.id, test_id=b"t-expire", outcome="pass")
    testrun_factory(upload_id=up2.id, test_id=b"t-new", outcome="failure")

    df.process_flakes_for_repo(repository, [up1.id, up2.id])

    assert TAUpload.objects.filter(repo_id=repository, state="processed").count() == 2

    new_flake = Flake.objects.get(repoid=repository, test_id=b"t-new")

    assert Flake.objects.filter(repoid=repository).count() == 3

    assert Flake.objects.filter(repoid=repository, test_id=b"t-new").count() == 1
    assert new_flake.count == 2
    assert new_flake.fail_count == 2
    assert new_flake.recent_passes_count == 0

    updated_flake = Flake.objects.get(repoid=repository, test_id=b"t-existing-fail")
    assert updated_flake.count == 6
    assert updated_flake.fail_count == 3
    assert updated_flake.recent_passes_count == 0

    expired_flake = Flake.objects.get(repoid=repository, test_id=b"t-expire")
    assert expired_flake.end_date is not None
    assert expired_flake.recent_passes_count == 30


@patch("tasks.detect_flakes.detect_flakes_task")
@patch("tasks.detect_flakes.process_flakes_for_repo")
def test_detect_flakes_soft_time_limit_exceeded(
    mock_process_flakes, mock_task, repository, upload_factory
):
    up = upload_factory()

    mock_process_flakes.side_effect = SoftTimeLimitExceeded()

    task = df.DetectFlakes()
    result = task.run_impl(None, repo_id=repository)

    assert result == {"successful": True, "reason": "soft_time_limit_exceeded"}
    assert mock_task.apply_async.call_count == 1
    call_kwargs = mock_task.apply_async.call_args.kwargs.get("kwargs")
    assert call_kwargs["repo_id"] == repository
    assert "task_token" in call_kwargs and isinstance(call_kwargs["task_token"], str)
    mock_process_flakes.assert_called_once_with(repository, [up.id])

    assert TAUpload.objects.filter(repo_id=repository).count() == 1


@patch("tasks.detect_flakes.detect_flakes_task")
@patch("tasks.detect_flakes.process_flakes_for_repo")
def test_detect_flakes_soft_time_limit_exceeded_no_retry(
    mock_process_flakes, mock_task, repository
):
    mock_process_flakes.side_effect = SoftTimeLimitExceeded()

    task = df.DetectFlakes()
    result = task.run_impl(None, repo_id=repository)

    assert result == {"successful": True, "reason": "no_uploads"}
    mock_task.apply_async.assert_not_called()
    mock_process_flakes.assert_not_called()


def test_detect_flakes_no_uploads(repository):
    task = df.DetectFlakes()
    result = task.run_impl(None, repo_id=repository)

    assert result == {"successful": True, "reason": "no_uploads"}


def test_detect_flakes_task_token_mismatch(repository, upload_factory):
    upload = upload_factory()
    upload.task_token = "different-token"
    upload.state = "pending"
    upload.save()

    task = df.DetectFlakes()

    result = task.run_impl(None, repo_id=repository, task_token="my-token")

    assert result == {"successful": False, "reason": "token_mismatch"}

    upload.refresh_from_db()
    assert upload.state == "pending"
    assert upload.task_token == "different-token"
