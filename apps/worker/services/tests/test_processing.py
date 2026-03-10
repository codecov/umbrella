from unittest.mock import MagicMock

import pytest

from database.tests.factories.core import (
    CommitFactory,
    RepositoryFactory,
    UploadFactory,
)
from services.processing.processing import process_upload
from services.processing.types import UploadArguments
from shared.reports.enums import UploadState
from shared.yaml import UserYaml


@pytest.mark.django_db(databases={"default"})
class TestProcessUpload:
    def test_successful_processing_marks_upload_as_processed(
        self, dbsession, mocker, mock_storage
    ):
        repository = RepositoryFactory.create()
        commit = CommitFactory.create(repository=repository)
        upload = UploadFactory.create(
            report__commit=commit,
            state="started",
            state_id=UploadState.UPLOADED.db_id,
        )
        dbsession.add_all([repository, commit, upload])
        dbsession.flush()

        arguments: UploadArguments = {
            "commit": commit.commitid,
            "upload_id": upload.id_,
            "version": "v4",
            "reportid": str(upload.report.external_id),
        }

        mock_report_service = mocker.patch(
            "services.processing.processing.ReportService"
        )
        mock_processing_result = MagicMock()
        mock_processing_result.error = None
        mock_processing_result.report = None
        mock_report_service.return_value.build_report_from_raw_content.return_value = (
            mock_processing_result
        )

        mocker.patch("services.processing.processing.save_intermediate_report")
        mocker.patch("services.processing.processing.rewrite_or_delete_upload")

        result = process_upload(
            on_processing_error=lambda error: None,
            db_session=dbsession,
            repo_id=repository.repoid,
            commit_sha=commit.commitid,
            commit_yaml=UserYaml({}),
            arguments=arguments,
        )

        assert result["successful"] is True
        assert result["upload_id"] == upload.id_

        dbsession.refresh(upload)
        assert upload.state_id == UploadState.PROCESSED.db_id
        # state string is not updated by the processor -- the finisher sets it
        # after merging (to avoid triggering the finisher's idempotency check early)
        assert upload.state == "started"
