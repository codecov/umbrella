from __future__ import annotations

import base64
import json
import os
import zlib
from datetime import datetime
from pathlib import Path

import pytest
import test_results_parser

from services.processing.types import UploadArguments
from services.test_analytics.ta_processor import (
    handle_file_not_found,
    handle_parsing_error,
    insert_testruns_timeseries,
    rewrite_or_delete_upload,
    should_delete_archive_settings,
    ta_processor,
)
from services.yaml import UserYaml
from shared.api_archive.archive import ArchiveService
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
def test_ta_processor_no_upload_id(update_state):
    repository = RepositoryFactory.create()
    commit = CommitFactory.create(repository=repository, branch="main")
    commit_yaml = {}

    argument: UploadArguments = {}

    result = ta_processor(
        repository.repoid,
        commit.commitid,
        commit_yaml,
        argument,
        update_state=update_state,
    )

    assert result is False


@pytest.mark.django_db(databases=["default", "ta_timeseries"])
@pytest.mark.parametrize("update_state", [True, False])
def test_ta_processor_already_processed(update_state):
    repository = RepositoryFactory.create()
    commit = CommitFactory.create(repository=repository, branch="main")
    upload = UploadFactory.create(report__commit=commit, state="processed")
    commit_yaml = {}

    argument: UploadArguments = {"upload_id": upload.id}

    result = ta_processor(
        repository.repoid,
        commit.commitid,
        commit_yaml,
        argument,
        update_state=update_state,
    )

    assert result is False


@pytest.mark.django_db(databases=["default", "ta_timeseries"])
def test_ta_processor_no_storage_path(mock_storage):
    repository = RepositoryFactory.create()
    commit = CommitFactory.create(repository=repository, branch="main")
    upload = UploadFactory.create(
        report__commit=commit, state="processing", storage_path=None
    )
    commit_yaml = {}

    argument: UploadArguments = {"upload_id": upload.id}

    result = ta_processor(
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
def test_ta_processor_file_not_found(mock_storage, storage_path):
    repository = RepositoryFactory.create()
    commit = CommitFactory.create(repository=repository, branch="main")
    upload = UploadFactory.create(
        report__commit=commit,
        state="processing",
        storage_path=None,
    )
    commit_yaml = {}

    argument: UploadArguments = {"upload_id": upload.id}

    result = ta_processor(
        repository.repoid, commit.commitid, commit_yaml, argument, update_state=True
    )

    assert result is False

    upload.refresh_from_db()
    assert upload.state == "processed"

    error = UploadError.objects.get(report_session=upload)
    assert error.error_code == "file_not_in_storage"
    assert error.error_params == {}


@pytest.mark.django_db(databases=["default", "ta_timeseries"])
def test_ta_processor_parsing_error(mock_storage):
    repository = RepositoryFactory.create()
    commit = CommitFactory.create(repository=repository, branch="main")
    upload = UploadFactory.create(
        report__commit=commit, state="processing", storage_path="path/to/invalid.xml"
    )
    commit_yaml = {}

    argument: UploadArguments = {"upload_id": upload.id}

    mock_storage.write_file("archive", "path/to/invalid.xml", b"invalid xml content")

    result = ta_processor(
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
def test_ta_processor_warning(mock_storage, snapshot):
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

    result = ta_processor(
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
def test_ta_processor_success_delete_archive(mock_storage, sample_test_json_path):
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

    result = ta_processor(
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
def test_ta_processor_success_keep_archive(mock_storage, sample_test_json_path):
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

    result = ta_processor(
        repository.repoid, commit.commitid, commit_yaml, argument, update_state=True
    )

    assert result is True

    testrun_db = Testrun.objects.filter(upload_id=upload.id).first()
    assert testrun_db is not None
    assert testrun_db.branch == commit.branch
    assert testrun_db.upload_id == upload.id
    assert testrun_db.flags == [flag.flag_name]

    assert mock_storage.read_file("archive", "path/to/valid.json") is not None


@pytest.mark.django_db
def test_handle_file_not_found():
    upload = UploadFactory()

    handle_file_not_found(upload)

    assert upload.state == "processed"

    error = UploadError.objects.filter(report_session=upload).first()
    assert error is not None
    assert error.error_code == "file_not_in_storage"


@pytest.mark.django_db
def test_parsing_error():
    upload = UploadFactory()

    handle_parsing_error(upload, Exception("test string"))

    assert upload.state == "processed"

    error = UploadError.objects.filter(report_session=upload).first()
    assert error is not None
    assert error.error_code == "unsupported_file_format"
    assert error.error_params["error_message"] == "test string"


@pytest.mark.django_db
def test_warning():
    upload = UploadFactory()

    UploadError.objects.create(
        report_session=upload,
        error_code="warning",
        error_params={"warning_message": "test warning"},
    )

    assert upload.state == "processed"

    error = UploadError.objects.filter(report_session=upload).first()
    assert error is not None
    assert error.error_code == "warning"
    assert error.error_params["warning_message"] == "test warning"


@pytest.mark.parametrize(
    "expire_raw,uploads,result",
    [
        (None, None, False),
        (7, None, True),
        (True, None, True),
        (None, False, True),
    ],
)
def test_should_delete_archive(
    expire_raw, uploads, result, mock_configuration, mock_storage
):
    mock_configuration.set_params(
        {"services": {"minio": {"expire_raw_after_n_days": expire_raw}}}
    )
    fake_yaml = UserYaml.from_dict(
        {"codecov": {"archive": {"uploads": uploads}}} if uploads is not None else {}
    )

    assert should_delete_archive_settings(fake_yaml) == result


@pytest.mark.django_db
def test_rewrite_or_delete_upload_deletes(mock_configuration, mock_storage):
    mock_configuration.set_params(
        {"services": {"minio": {"expire_raw_after_n_days": 1}}}
    )

    upload = UploadFactory(storage_path="url")
    archive_service = ArchiveService(upload.report.commit.repository)

    archive_service.write_file(upload.storage_path, b"test")

    rewrite_or_delete_upload(
        archive_service, UserYaml.from_dict({}), upload, b"rewritten"
    )

    with pytest.raises(FileNotInStorageError):
        archive_service.read_file(upload.storage_path)


@pytest.mark.django_db
def test_rewrite_or_delete_upload_does_not_delete(mock_configuration, mock_storage):
    mock_configuration.set_params(
        {"services": {"minio": {"expire_raw_after_n_days": 1}}}
    )

    upload = UploadFactory(storage_path="http_url")
    archive_service = ArchiveService(upload.report.commit.repository)

    archive_service.write_file(upload.storage_path, b"test")

    rewrite_or_delete_upload(
        archive_service, UserYaml.from_dict({}), upload, b"rewritten"
    )

    assert archive_service.read_file(upload.storage_path) == b"test"


@pytest.mark.django_db
def test_rewrite_or_delete_upload_rewrites(mock_storage):
    upload = UploadFactory(storage_path="url")
    archive_service = ArchiveService(upload.report.commit.repository)

    archive_service.write_file(upload.storage_path, b"test")

    rewrite_or_delete_upload(
        archive_service, UserYaml.from_dict({}), upload, b"rewritten"
    )

    assert archive_service.read_file(upload.storage_path) == b"rewritten"


@pytest.mark.django_db(databases=["default", "ta_timeseries"])
def test_insert_testruns_timeseries(snapshot):
    parsing_infos: list[test_results_parser.ParsingInfo] = [
        {
            "framework": "Pytest",
            "testruns": [
                {
                    "name": "test_1_name",
                    "classname": "test_1_classname",
                    "duration": 1,
                    "outcome": "pass",
                    "testsuite": "test_1_testsuite",
                    "failure_message": None,
                    "filename": "test_1_file",
                    "build_url": "test_1_build_url",
                    "computed_name": "test_1_computed_name",
                }
            ],
        },
        {
            "framework": "Pytest",
            "testruns": [
                {
                    "name": "test_2_name",
                    "classname": "test_2_classname",
                    "duration": 1,
                    "outcome": "failure",
                    "testsuite": "test_2_testsuite",
                    "failure_message": "test_2_failure_message",
                    "filename": "test_2_file",
                    "build_url": "test_2_build_url",
                    "computed_name": "test_2",
                }
            ],
        },
    ]

    upload = UploadFactory()
    upload.report.commit.repository.repoid = 1
    upload.report.commit.commitid = "123"
    upload.report.commit.branch = "main"
    upload.id = 1
    upload.created_at = datetime(2025, 1, 1, 0, 0, 0)

    insert_testruns_timeseries(
        repoid=upload.report.commit.repository.repoid,
        commitid=upload.report.commit.commitid,
        branch=upload.report.commit.branch,
        upload=upload,
        parsing_infos=parsing_infos,
    )

    testruns = Testrun.objects.filter(upload_id=upload.id)
    assert testruns.count() == 2

    testruns_list = sorted(
        testruns.values(
            "timestamp",
            "test_id",
            "name",
            "classname",
            "testsuite",
            "computed_name",
            "outcome",
            "duration_seconds",
            "failure_message",
            "framework",
            "filename",
            "repo_id",
            "commit_sha",
            "branch",
            "flags",
            "upload_id",
        ),
        key=lambda x: x["test_id"].hex(),
    )

    for testrun in testruns_list:
        testrun["timestamp"] = testrun["timestamp"].isoformat()
        testrun["test_id"] = testrun["test_id"].hex()

    assert snapshot("json") == testruns_list
