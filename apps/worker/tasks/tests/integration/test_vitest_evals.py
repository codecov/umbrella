import base64
import zlib
from functools import partial
from unittest.mock import patch
from uuid import uuid4

import orjson
import pytest
from sqlalchemy.orm import Session as DbSession

from database.tests.factories import CommitFactory, RepositoryFactory
from rollouts import ALLOW_VITEST_EVALS
from shared.api_archive.archive import ArchiveService
from shared.helpers.redis import get_redis_connection
from tasks.tests.utils import run_tasks
from tasks.upload import upload_task
from tests.helpers import mock_all_plans_and_tiers

from .test_upload_e2e import setup_mocks, write_raw_upload


@pytest.mark.integration
@pytest.mark.django_db(databases={"default", "ta_timeseries"})
@patch.object(ALLOW_VITEST_EVALS, "check_value", return_value=True)
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

    vitest_json = {
        "testResults": [
            {
                "assertionResults": [
                    {
                        "ancestorTitles": ["create-project"],
                        "status": "passed",
                        "title": "Create a new SENTRY_DSN for 'sentry-mcp-evals/cloudflare-mcp'",
                        "duration": 7217.116875,
                        "failureMessages": [],
                        "meta": {
                            "eval": {
                                "scores": [
                                    {
                                        "score": 0.6,
                                        "metadata": {
                                            "rationale": "The submitted answer includes all the factual content of the expert answer, specifically the SENTRY_DSN URL, which is the key piece of information. Additionally, the submission provides extra context by mentioning the project name and suggesting how the DSN can be used to initialize Sentry's SDKs. This makes the submission a superset of the expert answer, as it contains all the information from the expert answer plus additional relevant details. There is no conflict between the two answers, and the additional information in the submission is consistent with the expert answer."
                                        },
                                        "name": "Factuality2",
                                    }
                                ],
                                "avgScore": 0.6,
                            }
                        },
                    }
                ],
                "status": "passed",
                "name": "/Users/dcramer/src/sentry-mcp/packages/mcp-server-evals/src/evals/create-dsn.eval.ts",
            }
        ]
    }
    vitest_data = base64.b64encode(zlib.compress(orjson.dumps(vitest_json))).decode()
    upload_contents = {
        "test_results_files": [{"filename": "foo.json", "data": vitest_data}]
    }
    upload_json = do_upload(orjson.dumps(upload_contents), key_suffix="/test_results")

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
