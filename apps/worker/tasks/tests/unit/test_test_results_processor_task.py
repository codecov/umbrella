from pathlib import Path

import pytest

from database.models import CommitReport
from database.tests.factories import CommitFactory, UploadFactory
from tasks.test_results_processor import TestResultsProcessorTask

here = Path(__file__)


@pytest.mark.integration
def test_test_results_processor_ta_processor_fail(
    mocker,
    mock_configuration,
    dbsession,
    codecov_vcr,
    mock_storage,
    mock_redis,
    celery_app,
):
    url = "whatever.txt"
    with open(here.parent.parent / "samples" / "sample_test.json") as f:
        content = f.read()
        mock_storage.write_file("archive", url, content)
    upload = UploadFactory.create(storage_path=url)
    dbsession.add(upload)
    dbsession.flush()
    redis_queue = [{"url": url, "upload_id": upload.id_}]
    mocker.patch.object(TestResultsProcessorTask, "app", celery_app)

    mocker.patch(
        "tasks.test_results_processor.ta_processor",
        side_effect=Exception("test"),
    )

    commit = CommitFactory.create(
        message="hello world",
        commitid="cd76b0821854a780b60012aed85af0a8263004ad",
        repository__author__unencrypted_oauth_token="test7lk5ndmtqzxlx06rip65nac9c7epqopclnoy",
        repository__author__username="joseph-sentry",
        repository__author__service="github",
        repository__name="codecov-demo",
    )
    dbsession.add(commit)
    dbsession.flush()

    current_report_row = CommitReport(commit_id=commit.id_)
    dbsession.add(current_report_row)
    dbsession.flush()

    with pytest.raises(Exception, match="test"):
        TestResultsProcessorTask().run_impl(
            dbsession,
            previous_result=False,
            repoid=upload.report.commit.repoid,
            commitid=commit.commitid,
            commit_yaml={},
            arguments_list=redis_queue,
        )
