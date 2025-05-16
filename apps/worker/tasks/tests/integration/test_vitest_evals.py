from functools import partial
from uuid import uuid4

import pytest
from sqlalchemy.orm import Session as DbSession

from database.tests.factories import CommitFactory, RepositoryFactory
from shared.api_archive.archive import ArchiveService
from shared.helpers.redis import get_redis_connection
from tasks.tests.utils import run_tasks
from tasks.upload import upload_task
from tests.helpers import mock_all_plans_and_tiers

from .test_upload_e2e import setup_mocks, write_raw_upload


@pytest.mark.integration
@pytest.mark.django_db
def test_vitest_upload(
    dbsession: DbSession,
    mocker,
    mock_repo_provider,
    mock_storage,
    mock_configuration,
):
    mock_all_plans_and_tiers()
    setup_mocks(mocker, dbsession, mock_configuration, mock_repo_provider)

    repository = RepositoryFactory.create()
    dbsession.add(repository)
    dbsession.flush()

    repoid = repository.repoid
    commitid = uuid4().hex
    commit = CommitFactory.create(
        repository=repository, commitid=commitid, pullid=12, _report_json=None
    )
    dbsession.add(commit)
    dbsession.flush()

    archive_service = ArchiveService(repository)
    do_upload = partial(
        write_raw_upload,
        get_redis_connection(),
        archive_service,
        repoid,
        commitid,
    )

    upload_json = do_upload(
        b"""
a.rs
<<<<<< network
# path=coverage.lcov
SF:a.rs
DA:1,1
end_of_record
"""
    )

    # report_service = TestResultsReportService({})
    # commit_report = report_service.initialize_and_save_report(commit)
    # first_upload = report_service.create_report_upload(upload_json, commit_report)
    # dbsession.flush()

    with run_tasks():
        upload_task.apply_async(
            kwargs={
                "repoid": repoid,
                "commitid": commitid,
                "report_type": "test_results",
            }
        )
