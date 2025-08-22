from pathlib import Path

import pytest

from shared.django_apps.core.tests.factories import OwnerFactory, RepositoryFactory
from shared.django_apps.prevent_timeseries.models import Testrun
from shared.django_apps.test_analytics.models import TAUpload
from shared.plan.constants import PlanName
from shared.storage.exceptions import FileNotInStorageError
from shared.upload.types import TAUploadContext
from tasks.ingest_testruns import IngestTestruns
from tests.helpers import mock_all_plans_and_tiers


@pytest.fixture
def sample_test_json_path():
    return Path(__file__).parent.parent / "samples" / "sample_test.json"


@pytest.mark.django_db(databases=["default", "ta_timeseries"])
def test_ingest_testruns_file_not_found(mock_configuration, mock_storage):
    mock_all_plans_and_tiers()

    repository = RepositoryFactory.create()

    upload_context: TAUploadContext = {
        "commit_sha": "cd76b0821854a780b60012aed85af0a8263004ad",
        "branch": "main",
        "merged": False,
        "pull_id": None,
        "flags": ["unit"],
        "storage_path": "path/to/nonexistent.json",
    }

    result = IngestTestruns().run_impl(
        None,
        repoid=repository.repoid,
        upload_context=upload_context,
    )

    assert result == {"processed": 0, "reason": "file_not_in_storage"}
    assert Testrun.objects.count() == 0
    assert TAUpload.objects.count() == 0


@pytest.mark.django_db(databases=["default", "ta_timeseries"])
def test_ingest_testruns_parsing_error(mock_configuration, mock_storage):
    mock_all_plans_and_tiers()

    repository = RepositoryFactory.create()

    storage_path = "path/to/invalid.xml"
    mock_storage.write_file("archive", storage_path, b"invalid xml content")

    upload_context: TAUploadContext = {
        "commit_sha": "cd76b0821854a780b60012aed85af0a8263004ad",
        "branch": "main",
        "merged": False,
        "pull_id": None,
        "flags": ["unit"],
        "storage_path": storage_path,
    }

    result = IngestTestruns().run_impl(
        None,
        repoid=repository.repoid,
        upload_context=upload_context,
    )

    assert result == {"processed": 0, "reason": "unsupported_file_format"}
    assert Testrun.objects.count() == 0
    assert TAUpload.objects.count() == 0


@pytest.mark.django_db(databases=["default", "ta_timeseries"])
def test_ingest_testruns_success_delete_archive(
    mock_configuration, mock_storage, sample_test_json_path
):
    mock_all_plans_and_tiers()

    # Ensure we delete the raw file after processing
    mock_configuration.set_params(
        {"services": {"minio": {"expire_raw_after_n_days": 7}}}
    )

    repository = RepositoryFactory.create()

    storage_path = "path/to/valid.json"
    with open(sample_test_json_path, "rb") as f:
        sample_content = f.read()
    mock_storage.write_file("archive", storage_path, sample_content)

    upload_context: TAUploadContext = {
        "commit_sha": "cd76b0821854a780b60012aed85af0a8263004ad",
        "branch": "main",
        "merged": False,
        "pull_id": None,
        "flags": ["unit"],
        "storage_path": storage_path,
    }

    result = IngestTestruns().run_impl(
        None,
        repoid=repository.repoid,
        upload_context=upload_context,
    )

    assert result["processed"] == 4
    assert result["repoid"] == repository.repoid

    upload_row = TAUpload.objects.filter(repo_id=repository.repoid).first()
    assert upload_row is None

    testruns = Testrun.objects.all()
    assert testruns.count() == 4
    for tr in testruns:
        assert tr.branch == upload_context["branch"]
        assert tr.commit_sha == upload_context["commit_sha"]
        assert tr.flags == upload_context["flags"]

    with pytest.raises(FileNotInStorageError):
        mock_storage.read_file("archive", storage_path)


@pytest.mark.django_db(databases=["default", "ta_timeseries"])
def test_ingest_testruns_success_keep_archive(
    mock_configuration, mock_storage, sample_test_json_path
):
    mock_all_plans_and_tiers()

    # Ensure we keep the raw file after processing
    mock_configuration.set_params(
        {"services": {"minio": {"expire_raw_after_n_days": None}}}
    )

    repository = RepositoryFactory.create(
        author=OwnerFactory.create(plan=PlanName.CODECOV_PRO_MONTHLY.value)
    )
    repository.private = False
    repository.save()

    storage_path = "path/to/valid.json"
    with open(sample_test_json_path, "rb") as f:
        sample_content = f.read()
    mock_storage.write_file("archive", storage_path, sample_content)

    upload_context: TAUploadContext = {
        "commit_sha": "cd76b0821854a780b60012aed85af0a8263004ad",
        "branch": "main",
        "merged": False,
        "pull_id": None,
        "flags": ["unit"],
        "storage_path": storage_path,
    }

    result = IngestTestruns().run_impl(
        None,
        repoid=repository.repoid,
        upload_context=upload_context,
    )

    assert result["processed"] == 4
    assert result["repoid"] == repository.repoid

    upload_row = TAUpload.objects.filter(repo_id=repository.repoid).first()
    assert upload_row is not None

    testruns = Testrun.objects.filter(upload_id=upload_row.id)
    assert testruns.count() == 4
    for tr in testruns:
        assert tr.branch == upload_context["branch"]
        assert tr.commit_sha == upload_context["commit_sha"]
        assert tr.flags == upload_context["flags"]

    # File should have been rewritten into a readable format
    persisted = mock_storage.read_file("archive", storage_path)
    assert persisted is not None
    assert persisted.startswith(b"# path=")
