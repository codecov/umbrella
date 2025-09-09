import pytest

from database.models import CommitReport, Repository
from database.tests.factories import CommitFactory
from tasks.test_results_finisher import TestResultsFinisherTask


class TestTestResultsFinisherTask:
    @pytest.mark.django_db
    def test_upload_finisher_ta_finish_upload(self, mocker, dbsession):
        ta_finish_upload = mocker.patch(
            "tasks.test_results_finisher.ta_finish_upload",
            return_value={
                "notify_attempted": True,
                "notify_succeeded": True,
                "queue_notify": False,
            },
        )

        commit = CommitFactory.create(
            message="hello world",
            commitid="cd76b0821854a780b60012aed85af0a8263004ad",
            repository__author__unencrypted_oauth_token="test-token",
            repository__author__username="test-user",
            repository__author__service="github",
            repository__name="test-repo",
        )
        dbsession.add(commit)
        dbsession.flush()

        current_report_row = CommitReport(
            commit_id=commit.id_, report_type="test_results"
        )
        dbsession.add(current_report_row)
        dbsession.flush()

        task = TestResultsFinisherTask()
        result = task.run_impl(
            dbsession,
            False,
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml={},
        )

        assert result == {
            "notify_attempted": True,
            "notify_succeeded": True,
            "queue_notify": False,
        }

        assert ta_finish_upload.call_count == 1
        repo = dbsession.query(Repository).filter_by(repoid=commit.repoid).first()
        assert (
            dbsession,
            repo,
            commit,
            mocker.ANY,
        ) == ta_finish_upload.call_args[0]
