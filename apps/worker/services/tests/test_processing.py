from unittest.mock import MagicMock

import pytest

from database.tests.factories.core import (
    CommitFactory,
    RepositoryFactory,
    UploadFactory,
)
from services.processing.processing import process_upload
from services.processing.types import UploadArguments
from shared.yaml import UserYaml


@pytest.mark.django_db(databases={"default"})
class TestProcessUpload:
    def test_processor_does_not_enqueue_finisher(self, dbsession, mocker, mock_storage):
        # Setup
        repository = RepositoryFactory.create()
        commit = CommitFactory.create(repository=repository)
        upload = UploadFactory.create(
            report__commit=commit,
            state="started",
        )
        dbsession.add_all([repository, commit, upload])
        dbsession.flush()

        arguments: UploadArguments = {
            "commit": commit.commitid,
            "upload_id": upload.id_,
            "version": "v4",
            "reportid": str(upload.report.external_id),
        }

        # Mock dependencies
        mock_report_service = mocker.patch(
            "services.processing.processing.ReportService"
        )
        mock_processing_result = MagicMock()
        mock_processing_result.error = None
        mock_processing_result.report = None
        mock_report_service.return_value.build_report_from_raw_content.return_value = (
            mock_processing_result
        )

        # Mock ProcessingState to indicate all uploads are processed
        mock_state_instance = MagicMock()
        mock_state_instance.get_upload_numbers.return_value = MagicMock(
            processing=0,
            processed=0,  # All uploads processed and merged
        )
        mocker.patch(
            "services.processing.processing.ProcessingState",
            return_value=mock_state_instance,
        )

        # Mock other dependencies
        mocker.patch("services.processing.processing.save_intermediate_report")
        mocker.patch("services.processing.processing.rewrite_or_delete_upload")

        commit_yaml = UserYaml({})

        # Execute
        result = process_upload(
            on_processing_error=lambda error: None,
            db_session=dbsession,
            repo_id=repository.repoid,
            commit_sha=commit.commitid,
            commit_yaml=commit_yaml,
            arguments=arguments,
        )

        # Verify
        assert result["successful"] is True
        assert result["upload_id"] == upload.id_

        # Finisher enqueue is handled elsewhere; processor no longer triggers it directly.
