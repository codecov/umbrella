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


def _setup_process_upload_mocks(mocker, gate_acquired=True):
    """Common mock setup for process_upload tests."""
    mock_report_service = mocker.patch(
        "services.processing.processing.ReportService"
    )
    mock_processing_result = MagicMock()
    mock_processing_result.error = None
    mock_processing_result.report = None
    mock_report_service.return_value.build_report_from_raw_content.return_value = (
        mock_processing_result
    )

    mocker.patch(
        "services.processing.processing.ProcessingState",
        return_value=MagicMock(),
    )

    mocker.patch(
        "services.processing.processing.try_acquire_finisher_gate",
        return_value=gate_acquired,
    )

    mock_celery_app = mocker.patch("services.processing.processing.celery_app")
    mock_finisher_task = MagicMock()
    mock_celery_app.tasks = {"app.tasks.upload.UploadFinisher": mock_finisher_task}

    mocker.patch("services.processing.processing.save_intermediate_report")
    mocker.patch("services.processing.processing.rewrite_or_delete_upload")

    return mock_finisher_task


@pytest.mark.django_db(databases={"default"})
class TestProcessUploadFinisherTriggering:
    """
    Tests for the finisher triggering mechanism at the end of processing.

    After each upload is processed, the processor tries to acquire the finisher
    gate (Redis SET NX). The first processor to win dispatches the finisher;
    all others are no-ops. The finisher itself handles partial-completion via
    its CONTINUATION/SWEEP follow-ups.
    """

    def test_triggers_finisher_when_gate_acquired(
        self, dbsession, mocker, mock_storage
    ):
        """Finisher is dispatched when this processor wins the gate."""
        repository = RepositoryFactory.create()
        commit = CommitFactory.create(repository=repository)
        upload = UploadFactory.create(report__commit=commit, state="started")
        dbsession.add_all([repository, commit, upload])
        dbsession.flush()

        mock_finisher_task = _setup_process_upload_mocks(
            mocker, gate_acquired=True
        )
        commit_yaml = UserYaml({})

        result = process_upload(
            on_processing_error=lambda error: None,
            db_session=dbsession,
            repo_id=repository.repoid,
            commit_sha=commit.commitid,
            commit_yaml=commit_yaml,
            arguments=UploadArguments(
                commit=commit.commitid,
                upload_id=upload.id_,
                version="v4",
                reportid=str(upload.report.external_id),
            ),
        )

        assert result["successful"] is True
        mock_finisher_task.apply_async.assert_called_once_with(
            kwargs={
                "repoid": repository.repoid,
                "commitid": commit.commitid,
                "commit_yaml": commit_yaml.to_dict(),
            }
        )

    def test_does_not_trigger_finisher_when_gate_already_held(
        self, dbsession, mocker, mock_storage
    ):
        """Finisher is NOT dispatched when another processor already holds the gate."""
        repository = RepositoryFactory.create()
        commit = CommitFactory.create(repository=repository)
        upload = UploadFactory.create(report__commit=commit, state="started")
        dbsession.add_all([repository, commit, upload])
        dbsession.flush()

        mock_finisher_task = _setup_process_upload_mocks(
            mocker, gate_acquired=False
        )
        commit_yaml = UserYaml({})

        result = process_upload(
            on_processing_error=lambda error: None,
            db_session=dbsession,
            repo_id=repository.repoid,
            commit_sha=commit.commitid,
            commit_yaml=commit_yaml,
            arguments=UploadArguments(
                commit=commit.commitid,
                upload_id=upload.id_,
                version="v4",
                reportid=str(upload.report.external_id),
            ),
        )

        assert result["successful"] is True
        mock_finisher_task.apply_async.assert_not_called()
