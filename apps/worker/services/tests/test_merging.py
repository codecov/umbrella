import pytest

from database.tests.factories.core import (
    CommitFactory,
    ReportFactory,
    RepositoryFactory,
    UploadFactory,
)
from services.processing.merging import update_uploads
from services.processing.types import MergeResult, ProcessingResult
from shared.reports.enums import UploadState
from shared.yaml import UserYaml


@pytest.mark.django_db(databases={"default"})
class TestUpdateUploadsState:
    def test_successful_uploads_set_to_merged(self, dbsession):
        repository = RepositoryFactory.create()
        commit = CommitFactory.create(repository=repository)
        report = ReportFactory.create(commit=commit)
        upload = UploadFactory.create(
            report=report,
            state="started",
            state_id=UploadState.UPLOADED.db_id,
        )
        dbsession.add_all([repository, commit, report, upload])
        dbsession.flush()

        processing_results: list[ProcessingResult] = [
            {"upload_id": upload.id_, "successful": True, "arguments": {}},
        ]
        merge_result = MergeResult(
            session_mapping={upload.id_: 0}, deleted_sessions=set()
        )

        update_uploads(dbsession, UserYaml({}), processing_results, [], merge_result)

        dbsession.refresh(upload)
        assert upload.state_id == UploadState.MERGED.db_id
        assert upload.state == "merged"

    def test_failed_uploads_set_to_error(self, dbsession):
        repository = RepositoryFactory.create()
        commit = CommitFactory.create(repository=repository)
        report = ReportFactory.create(commit=commit)
        upload = UploadFactory.create(
            report=report,
            state="started",
            state_id=UploadState.UPLOADED.db_id,
        )
        dbsession.add_all([repository, commit, report, upload])
        dbsession.flush()

        processing_results: list[ProcessingResult] = [
            {
                "upload_id": upload.id_,
                "successful": False,
                "arguments": {},
                "error": {"code": "report_empty", "params": {}},
            },
        ]
        merge_result = MergeResult(session_mapping={}, deleted_sessions=set())

        update_uploads(dbsession, UserYaml({}), processing_results, [], merge_result)

        dbsession.refresh(upload)
        assert upload.state_id == UploadState.ERROR.db_id
        assert upload.state == "error"
