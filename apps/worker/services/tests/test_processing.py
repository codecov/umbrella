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
class TestProcessUploadOrphanedTaskRecovery:
    """
    Tests for the orphaned upload recovery mechanism.

    When a processor task retries outside of a chord (e.g., from visibility timeout
    or task_reject_on_worker_lost), it should trigger the finisher if all uploads
    are now processed.
    """

    def test_triggers_finisher_when_last_upload_completes(
        self, dbsession, mocker, mock_storage
    ):
        """
        Test that finisher is triggered when the last upload completes processing.

        This simulates the scenario where:
        1. A processor task died before ACK, missing the chord callback
        2. Task retries standalone (visibility timeout or worker rejection)
        3. Completes successfully and notices all uploads are done
        4. Triggers finisher to complete the upload flow
        """
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

        # Mock should_trigger_postprocessing to return True
        mocker.patch(
            "services.processing.processing.should_trigger_postprocessing",
            return_value=True,
        )

        # Mock celery app to capture finisher task scheduling
        mock_celery_app = mocker.patch("services.processing.processing.celery_app")
        mock_finisher_task = MagicMock()
        mock_celery_app.tasks = {"app.tasks.upload.UploadFinisher": mock_finisher_task}

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

        # Verify finisher was triggered
        mock_finisher_task.apply_async.assert_called_once_with(
            kwargs={
                "repoid": repository.repoid,
                "commitid": commit.commitid,
                "commit_yaml": commit_yaml.to_dict(),
            }
        )

    def test_does_not_trigger_finisher_when_uploads_still_processing(
        self, dbsession, mocker, mock_storage
    ):
        """
        Test that finisher is NOT triggered when other uploads are still processing.

        This verifies we don't prematurely trigger the finisher when only some
        uploads have completed.
        """
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

        # Mock ProcessingState to indicate other uploads still processing
        mock_state_instance = MagicMock()
        mock_state_instance.get_upload_numbers.return_value = MagicMock(
            processing=2,
            processed=1,  # Other uploads still in progress
        )
        mocker.patch(
            "services.processing.processing.ProcessingState",
            return_value=mock_state_instance,
        )

        # Mock should_trigger_postprocessing to return False
        mocker.patch(
            "services.processing.processing.should_trigger_postprocessing",
            return_value=False,
        )

        # Mock celery app to capture finisher task scheduling
        mock_celery_app = mocker.patch("services.processing.processing.celery_app")
        mock_finisher_task = MagicMock()
        mock_celery_app.tasks = {"app.tasks.upload.UploadFinisher": mock_finisher_task}

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

        # Verify finisher was NOT triggered
        mock_finisher_task.apply_async.assert_not_called()
