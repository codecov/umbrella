import base64
import json
import os
import zlib
from pathlib import Path

import pytest

from services.processing.types import UploadArguments
from services.test_analytics.ta_processor import ta_processor_impl
from shared.django_apps.core.tests.factories import CommitFactory, RepositoryFactory
from shared.django_apps.reports.models import UploadError
from shared.django_apps.reports.tests.factories import (
    RepositoryFlagFactory,
    UploadFactory,
    UploadFlagMembershipFactory,
)
from shared.django_apps.ta_timeseries.models import Testrun
from shared.storage.exceptions import FileNotInStorageError


@pytest.fixture
def sample_test_json_path():
    return Path(__file__).parent / "samples" / "sample_test.json"


@pytest.mark.django_db(databases=["default", "ta_timeseries"])
@pytest.mark.parametrize("update_state", [True, False])
def test_ta_processor_impl_no_upload_id(update_state):
    repository = RepositoryFactory.create()
    commit = CommitFactory.create(repository=repository, branch="main")
    commit_yaml = {}

    argument: UploadArguments = {}

    result = ta_processor_impl(
        repository.repoid,
        commit.commitid,
        commit_yaml,
        argument,
        update_state=update_state,
    )

    assert result is False


@pytest.mark.django_db(databases=["default", "ta_timeseries"])
@pytest.mark.parametrize("update_state", [True, False])
def test_ta_processor_impl_already_processed(update_state):
    repository = RepositoryFactory.create()
    commit = CommitFactory.create(repository=repository, branch="main")
    upload = UploadFactory.create(report__commit=commit, state="processed")
    commit_yaml = {}

    argument: UploadArguments = {"upload_id": upload.id}

    result = ta_processor_impl(
        repository.repoid,
        commit.commitid,
        commit_yaml,
        argument,
        update_state=update_state,
    )

    assert result is False


@pytest.mark.django_db(databases=["default", "ta_timeseries"])
def test_ta_processor_impl_no_storage_path(mock_storage):
    repository = RepositoryFactory.create()
    commit = CommitFactory.create(repository=repository, branch="main")
    upload = UploadFactory.create(
        report__commit=commit, state="processing", storage_path=None
    )
    commit_yaml = {}

    argument: UploadArguments = {"upload_id": upload.id}

    result = ta_processor_impl(
        repository.repoid, commit.commitid, commit_yaml, argument, update_state=True
    )

    assert result is False

    upload.refresh_from_db()
    assert upload.state == "processed"

    error = UploadError.objects.get(report_session=upload)
    assert error.error_code == "file_not_in_storage"
    assert error.error_params == {}


@pytest.mark.parametrize("storage_path", [None, "path/to/nonexistent.xml"])
@pytest.mark.django_db(databases=["default", "ta_timeseries"])
def test_ta_processor_impl_file_not_found(mock_storage, storage_path):
    repository = RepositoryFactory.create()
    commit = CommitFactory.create(repository=repository, branch="main")
    upload = UploadFactory.create(
        report__commit=commit,
        state="processing",
        storage_path=None,
    )
    commit_yaml = {}

    argument: UploadArguments = {"upload_id": upload.id}

    result = ta_processor_impl(
        repository.repoid, commit.commitid, commit_yaml, argument, update_state=True
    )

    assert result is False

    upload.refresh_from_db()
    assert upload.state == "processed"

    error = UploadError.objects.get(report_session=upload)
    assert error.error_code == "file_not_in_storage"
    assert error.error_params == {}


@pytest.mark.django_db(databases=["default", "ta_timeseries"])
def test_ta_processor_impl_parsing_error(mock_storage):
    repository = RepositoryFactory.create()
    commit = CommitFactory.create(repository=repository, branch="main")
    upload = UploadFactory.create(
        report__commit=commit, state="processing", storage_path="path/to/invalid.xml"
    )
    commit_yaml = {}

    argument: UploadArguments = {"upload_id": upload.id}

    mock_storage.write_file("archive", "path/to/invalid.xml", b"invalid xml content")

    result = ta_processor_impl(
        repository.repoid, commit.commitid, commit_yaml, argument, update_state=True
    )

    assert result is False

    upload.refresh_from_db()
    assert upload.state == "processed"

    error = UploadError.objects.get(report_session=upload)
    assert error.error_code == "unsupported_file_format"
    assert error.error_params == {
        "error_message": "Error deserializing json\n\nCaused by:\n    expected value at line 1 column 1"
    }


@pytest.mark.django_db(databases=["default", "ta_timeseries"])
def test_ta_processor_impl_warning(mock_storage, snapshot):
    repository = RepositoryFactory.create()
    commit = CommitFactory.create(repository=repository, branch="main")
    upload = UploadFactory.create(
        report__commit=commit, state="processing", storage_path="path/to/invalid.xml"
    )
    commit_yaml = {}

    argument: UploadArguments = {"upload_id": upload.id}

    script_dir = os.path.dirname(__file__)

    with open(
        os.path.join(script_dir, "samples", "sample-warnings-junit.xml"), "rb"
    ) as f:
        sample_junit = f.read()
    sample_content = {
        "test_results_files": [
            {
                "filename": "codecov-demo/temp.junit.xml",
                "format": "base64+compressed",
                "data": base64.b64encode(zlib.compress(sample_junit)).decode("utf-8"),
                "labels": "",
            }
        ],
        "metadata": {},
    }

    mock_storage.write_file(
        "archive", "path/to/invalid.xml", json.dumps(sample_content)
    )

    result = ta_processor_impl(
        repository.repoid, commit.commitid, commit_yaml, argument, update_state=True
    )

    assert result is True

    upload.refresh_from_db()
    assert upload.state == "processed"

    errors = UploadError.objects.filter(report_session=upload)
    assert errors.count() == 2
    for error in errors:
        assert error.error_code == "warning"
        assert snapshot("json") == error.error_params


@pytest.mark.django_db(databases=["default", "ta_timeseries"])
def test_ta_processor_impl_success_delete_archive(mock_storage, sample_test_json_path):
    repository = RepositoryFactory.create()
    commit = CommitFactory.create(repository=repository, branch="main")
    upload = UploadFactory.create(
        report__commit=commit,
        state="processing",
        storage_path="path/to/valid.json",
    )

    flag = RepositoryFlagFactory.create(repository=repository, flag_name="unit")
    UploadFlagMembershipFactory.create(report_session=upload, flag=flag)

    commit_yaml = {"codecov": {"archive": {"uploads": False}}}

    argument: UploadArguments = {"upload_id": upload.id}

    with open(sample_test_json_path, "rb") as f:
        sample_content = f.read()

    mock_storage.write_file("archive", "path/to/valid.json", sample_content)

    result = ta_processor_impl(
        repository.repoid, commit.commitid, commit_yaml, argument, update_state=True
    )

    assert result is True

    testrun_db = Testrun.objects.filter(upload_id=upload.id).first()
    assert testrun_db is not None
    assert testrun_db.branch == commit.branch
    assert testrun_db.upload_id == upload.id
    assert testrun_db.flags == [flag.flag_name]

    with pytest.raises(FileNotInStorageError):
        mock_storage.read_file("archive", "path/to/valid.json")


@pytest.mark.django_db(databases=["default", "ta_timeseries"])
def test_ta_processor_impl_success_keep_archive(mock_storage, sample_test_json_path):
    repository = RepositoryFactory.create()
    commit = CommitFactory.create(repository=repository, branch="main")
    upload = UploadFactory.create(
        report__commit=commit,
        state="processing",
        storage_path="path/to/valid.json",
    )

    flag = RepositoryFlagFactory.create(repository=repository, flag_name="unit")
    UploadFlagMembershipFactory.create(report_session=upload, flag=flag)

    commit_yaml = {"codecov": {"archive": {"uploads": True}}}

    argument: UploadArguments = {"upload_id": upload.id}

    with open(sample_test_json_path, "rb") as f:
        sample_content = f.read()

    mock_storage.write_file("archive", "path/to/valid.json", sample_content)

    result = ta_processor_impl(
        repository.repoid, commit.commitid, commit_yaml, argument, update_state=True
    )

    assert result is True

    testrun_db = Testrun.objects.filter(upload_id=upload.id).first()
    assert testrun_db is not None
    assert testrun_db.branch == commit.branch
    assert testrun_db.upload_id == upload.id
    assert testrun_db.flags == [flag.flag_name]

    assert mock_storage.read_file("archive", "path/to/valid.json") is not None
