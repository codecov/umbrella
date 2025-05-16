import base64
import zlib

import orjson
import pytest

from services.processing.types import UploadArguments
from services.test_analytics.ta_processor import ta_processor_impl
from shared.django_apps.core.tests.factories import CommitFactory, RepositoryFactory
from shared.django_apps.reports.tests.factories import UploadFactory
from shared.django_apps.ta_timeseries.models import Testrun


@pytest.mark.django_db(databases=["default", "ta_timeseries"])
def test_ta_stores_vitest_evals(mocker, mock_storage):
    mocker.patch("rollouts.ALLOW_VITEST_EVALS.check_value", return_value=True)

    repository = RepositoryFactory.create()
    commit = CommitFactory.create(repository=repository, branch="main")
    upload = UploadFactory.create(
        report__commit=commit, state="processing", storage_path="path/to/valid.json"
    )

    argument: UploadArguments = {"upload_id": upload.id}

    # Example output taken from <https://github.com/getsentry/vitest-evals/issues/13#issuecomment-2835687888>
    meta = {
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
    }
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
                        "meta": meta,
                    }
                ],
                "status": "passed",
                "name": "/Users/dcramer/src/sentry-mcp/packages/mcp-server-evals/src/evals/create-dsn.eval.ts",
            }
        ]
    }
    vitest_data = base64.b64encode(zlib.compress(orjson.dumps(vitest_json))).decode()
    upload_contents = orjson.dumps(
        {"test_results_files": [{"filename": "foo.json", "data": vitest_data}]}
    )
    mock_storage.write_file("archive", "path/to/valid.json", upload_contents)

    result = ta_processor_impl(
        repository.repoid, commit.commitid, {}, argument, update_state=True
    )

    assert result is True

    testrun_db = Testrun.objects.filter(upload_id=upload.id).first()
    assert testrun_db is not None
    assert testrun_db.branch == commit.branch
    assert testrun_db.upload_id == upload.id
    assert testrun_db.properties == meta
