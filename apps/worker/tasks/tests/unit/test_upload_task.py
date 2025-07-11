import json
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest import mock
from unittest.mock import AsyncMock, call

import pytest
from celery.exceptions import Retry
from redis.exceptions import LockError

from database.enums import ReportType
from database.models import Upload
from database.models.reports import CommitReport
from database.tests.factories import CommitFactory, OwnerFactory, RepositoryFactory
from database.tests.factories.core import GithubAppInstallationFactory, ReportFactory
from helpers.checkpoint_logger import _kwargs_key
from helpers.checkpoint_logger.flows import TestResultsFlow, UploadFlow
from helpers.exceptions import RepositoryWithoutValidBotError
from helpers.log_context import LogContext, set_log_context
from services.report import NotReadyToBuildReportYetError, ReportService
from shared.helpers.redis import get_redis_connection
from shared.reports.enums import UploadState, UploadType
from shared.torngit import GitlabEnterprise
from shared.torngit.exceptions import TorngitClientError, TorngitRepoNotFoundError
from shared.torngit.gitlab import Gitlab
from shared.utils.sessions import SessionType
from tasks.bundle_analysis_notify import bundle_analysis_notify_task
from tasks.bundle_analysis_processor import bundle_analysis_processor_task
from tasks.test_results_finisher import test_results_finisher_task
from tasks.test_results_processor import test_results_processor_task
from tasks.upload import NEW_TA_TASKS_CUTOFF_DATE, UploadContext, UploadTask
from tasks.upload_finisher import upload_finisher_task
from tasks.upload_processor import upload_processor_task

here = Path(__file__)


def _start_upload_flow(mocker):
    mocker.patch(
        "helpers.checkpoint_logger._get_milli_timestamp",
        side_effect=[1337, 9001, 10000, 15000, 20000, 25000],
    )
    set_log_context(LogContext())
    UploadFlow.log(UploadFlow.UPLOAD_TASK_BEGIN)


class FakeRedis:
    """
    This is a fake, very rudimentary redis implementation to ease the managing
     of mocking `exists`, `lpop` and whatnot in the context of Upload jobs
    """

    def __init__(self, mocker):
        self.lists = {}
        self.keys = {}
        self.lock = mocker.MagicMock()
        self.delete = mocker.MagicMock()
        self.sismember = mocker.MagicMock()
        self.hdel = mocker.MagicMock()

    def exists(self, key):
        if self.lists.get(key):
            return True
        if self.keys.get(key) is not None:
            return True
        return False

    def get(self, key):
        res = None
        if self.keys.get(key) is not None:
            res = self.keys.get(key)
        if self.lists.get(key):
            res = self.lists.get(key)
        if res is None:
            return None
        if not isinstance(res, str | bytes):
            return str(res).encode()
        if not isinstance(res, bytes):
            return res.encode()
        return res

    def lpop(self, key, count=None):
        list = self.lists.get(key)
        if not list:
            return None

        res = None
        if count:
            res = []
            for _ in range(count):
                res.append(list.pop(0))
                if list == []:
                    del self.lists[key]
                    break
        else:
            res = list.pop(0)
            if list == []:
                del self.lists[key]

        return res

    def delete(self, key):
        del self.lists[key]


@pytest.fixture
def mock_redis(mocker):
    m = mocker.patch("shared.helpers.redis._get_redis_instance_from_url")
    redis_server = FakeRedis(mocker)
    m.return_value = redis_server
    yield redis_server


@pytest.fixture()
def mock_repo_provider_service(mocker):
    mocker.patch("tasks.upload.hasattr", return_value=False)

    mock_repo_provider_service = AsyncMock()
    mock_repo_provider_service.get_commit.return_value = {
        "author": {
            "id": "123",
            "username": "456",
            "email": "789",
            "name": "101",
        },
        "message": "hello world",
        "parents": [],
        "timestamp": str(datetime.now()),
    }
    mock_repo_provider_service.get_ancestors_tree.return_value = {"parents": []}
    mock_repo_provider_service.get_pull_request.return_value = {
        "head": {"branch": "main"},
        "base": {},
    }
    mock_repo_provider_service.list_top_level_files.return_value = [
        {"name": "codecov.yml", "path": "codecov.yml"}
    ]
    mock_repo_provider_service.get_source.return_value = {
        "content": """
        codecov:
            max_report_age: 1y ago
        """
    }

    mocker.patch(
        "tasks.base.get_repo_provider_service",
        return_value=mock_repo_provider_service,
    )

    return mock_repo_provider_service


@pytest.fixture(scope="function", autouse=True)
def clear_log_context(mocker):
    set_log_context(LogContext())


@pytest.mark.integration
class TestUploadTaskIntegration:
    @pytest.mark.django_db
    def test_upload_task_call(
        self,
        mocker,
        mock_configuration,
        dbsession,
        codecov_vcr,
        mock_storage,
        celery_app,
        mock_checkpoint_submit,
    ):
        _start_upload_flow(mocker)
        mocked_chord = mocker.patch("tasks.upload.chord")
        url = "v4/raw/2019-05-22/C3C4715CA57C910D11D5EB899FC86A7E/4c4e4654ac25037ae869caeb3619d485970b6304/a84d445c-9c1e-434f-8275-f18f1f320f81.txt"
        mocker.patch.object(UploadTask, "app", celery_app)

        commit = CommitFactory.create(
            message="",
            commitid="abf6d4df662c47e32460020ab14abf9303581429",
            repository__owner__unencrypted_oauth_token="test7lk5ndmtqzxlx06rip65nac9c7epqopclnoy",
            repository__owner__username="ThiagoCodecov",
            repository__owner__service="github",
            repository__yaml={"codecov": {"max_report_age": "1y ago"}},
            repository__name="example-python",
            pullid=1,
            # Setting the time to _before_ patch centric default YAMLs start date of 2024-04-30
            repository__owner__createstamp=datetime(2023, 1, 1, tzinfo=UTC),
        )
        dbsession.add(commit)
        dbsession.flush()
        dbsession.refresh(commit)
        repo_updatestamp = commit.repository.updatestamp
        redis = get_redis_connection()
        redis.lpush(
            f"uploads/{commit.repoid}/{commit.commitid}",
            json.dumps({"url": url, "build": "some_random_build"}),
        )
        kwargs = UploadFlow.save_to_kwargs({})
        result = UploadTask().run_impl(
            dbsession,
            commit.repoid,
            commit.commitid,
            kwargs=kwargs,
        )
        expected_result = {"was_setup": False, "was_updated": True}
        assert expected_result == result
        assert commit.message == "dsidsahdsahdsa"
        assert commit.parent_commit_id is None
        assert commit.report is not None
        sessions = commit.report.uploads
        assert len(sessions) == 1
        first_session = (
            dbsession.query(Upload)
            .filter_by(report_id=commit.report.id, build_code="some_random_build")
            .first()
        )
        processor = upload_processor_task.s(
            repoid=commit.repoid,
            commitid="abf6d4df662c47e32460020ab14abf9303581429",
            commit_yaml={"codecov": {"max_report_age": "1y ago"}},
            arguments={
                "url": url,
                "flags": [],
                "build": "some_random_build",
                "upload_id": first_session.id,
                "upload_pk": first_session.id,
            },
        )
        kwargs = {
            "repoid": commit.repoid,
            "commitid": "abf6d4df662c47e32460020ab14abf9303581429",
            "commit_yaml": {"codecov": {"max_report_age": "1y ago"}},
        }
        kwargs[_kwargs_key(UploadFlow)] = mocker.ANY
        finisher = upload_finisher_task.signature(kwargs=kwargs)
        mocked_chord.assert_called_with([processor], finisher)
        calls = [
            mock.call(
                "time_before_processing",
                UploadFlow.UPLOAD_TASK_BEGIN,
                UploadFlow.PROCESSING_BEGIN,
                data={
                    UploadFlow.UPLOAD_TASK_BEGIN: 1337,
                    UploadFlow.PROCESSING_BEGIN: 9001,
                    UploadFlow.INITIAL_PROCESSING_COMPLETE: 10000,
                },
            ),
            mock.call(
                "initial_processing_duration",
                UploadFlow.PROCESSING_BEGIN,
                UploadFlow.INITIAL_PROCESSING_COMPLETE,
                data={
                    UploadFlow.UPLOAD_TASK_BEGIN: 1337,
                    UploadFlow.PROCESSING_BEGIN: 9001,
                    UploadFlow.INITIAL_PROCESSING_COMPLETE: 10000,
                },
            ),
        ]
        mock_checkpoint_submit.assert_has_calls(calls)

    @pytest.mark.django_db
    def test_upload_task_call_bundle_analysis(
        self,
        mocker,
        mock_configuration,
        dbsession,
        codecov_vcr,
        mock_storage,
        mock_redis,
        celery_app,
    ):
        chain = mocker.patch("tasks.upload.chain")
        storage_path = (
            "v1/repos/testing/ed1bdd67-8fd2-4cdb-ac9e-39b99e4a3892/bundle_report.sqlite"
        )
        redis_queue = [{"url": storage_path, "build_code": "some_random_build"}]
        jsonified_redis_queue = [json.dumps(x) for x in redis_queue]
        mocker.patch.object(UploadTask, "app", celery_app)

        commit = CommitFactory.create(
            message="",
            commitid="abf6d4df662c47e32460020ab14abf9303581429",
            repository__owner__oauth_token="GHTZB+Mi+kbl/ubudnSKTJYb/fgN4hRJVJYSIErtidEsCLDJBb8DZzkbXqLujHAnv28aKShXddE/OffwRuwKug==",
            repository__owner__username="ThiagoCodecov",
            repository__owner__service="github",
            repository__yaml={"codecov": {"max_report_age": "1y ago"}},
            repository__name="example-python",
            pullid=1,
            # Setting the time to _before_ patch centric default YAMLs start date of 2024-04-30
            repository__owner__createstamp=datetime(2023, 1, 1, tzinfo=UTC),
        )
        dbsession.add(commit)
        dbsession.flush()
        dbsession.refresh(commit)

        mock_redis.lists[
            f"uploads/{commit.repoid}/{commit.commitid}/bundle_analysis"
        ] = jsonified_redis_queue

        UploadTask().run_impl(
            dbsession,
            commit.repoid,
            commit.commitid,
            report_type="bundle_analysis",
        )
        commit_report = commit.commit_report(report_type=ReportType.BUNDLE_ANALYSIS)
        assert commit_report
        uploads = commit_report.uploads
        assert len(uploads) == 1
        upload = dbsession.query(Upload).filter_by(report_id=commit_report.id).first()
        processor_sig = bundle_analysis_processor_task.s(
            {},
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml={"codecov": {"max_report_age": "1y ago"}},
            params={
                "url": storage_path,
                "flags": [],
                "build_code": "some_random_build",
                "upload_id": upload.id,
                "upload_pk": upload.id,
            },
        )
        notify_sig = bundle_analysis_notify_task.s(
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml={"codecov": {"max_report_age": "1y ago"}},
        )
        chain.assert_called_with([processor_sig, notify_sig])

    @pytest.mark.django_db
    def test_upload_task_call_bundle_analysis_no_upload(
        self,
        mocker,
        mock_configuration,
        dbsession,
        codecov_vcr,
        mock_storage,
        mock_redis,
        celery_app,
    ):
        chain = mocker.patch("tasks.upload.chain")
        storage_path = "v4/raw/2019-05-22/C3C4715CA57C910D11D5EB899FC86A7E/4c4e4654ac25037ae869caeb3619d485970b6304/a84d445c-9c1e-434f-8275-f18f1f320f81.txt"
        redis_queue = [{"url": storage_path, "build_code": "some_random_build"}]
        jsonified_redis_queue = [json.dumps(x) for x in redis_queue]
        mocker.patch.object(UploadTask, "app", celery_app)

        commit = CommitFactory.create(
            message="",
            commitid="abf6d4df662c47e32460020ab14abf9303581429",
            repository__owner__oauth_token="GHTZB+Mi+kbl/ubudnSKTJYb/fgN4hRJVJYSIErtidEsCLDJBb8DZzkbXqLujHAnv28aKShXddE/OffwRuwKug==",
            repository__owner__username="ThiagoCodecov",
            repository__owner__service="github",
            repository__yaml={"codecov": {"max_report_age": "1y ago"}},
            repository__name="example-python",
            pullid=1,
            # Setting the time to _before_ patch centric default YAMLs start date of 2024-04-30
            repository__owner__createstamp=datetime(2023, 1, 1, tzinfo=UTC),
        )
        dbsession.add(commit)
        dbsession.flush()
        dbsession.refresh(commit)

        repository = commit.repository
        repository.bundle_analysis_enabled = True
        dbsession.add(repository)
        dbsession.flush()
        dbsession.refresh(repository)

        mock_redis.lists[f"uploads/{commit.repoid}/{commit.commitid}/test_results"] = (
            jsonified_redis_queue
        )

        UploadTask().run_impl(
            dbsession,
            commit.repoid,
            commit.commitid,
            report_type="test_results",
        )

        commit_report = commit.commit_report(report_type=ReportType.BUNDLE_ANALYSIS)
        assert commit_report is None

        processor_sig = bundle_analysis_processor_task.s(
            {},
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml={"codecov": {"max_report_age": "1y ago"}},
            params={
                "url": storage_path,
                "flags": [],
                "build_code": "some_random_build",
                "upload_pk": None,
            },
        )
        assert call([processor_sig]) in chain.mock_calls

    @pytest.mark.django_db
    def test_upload_task_call_test_results(
        self,
        mocker,
        mock_configuration,
        dbsession,
        codecov_vcr,
        mock_storage,
        mock_redis,
        celery_app,
    ):
        chain = mocker.patch("tasks.upload.chain")
        storage_path = "v4/raw/2019-05-22/C3C4715CA57C910D11D5EB899FC86A7E/4c4e4654ac25037ae869caeb3619d485970b6304/a84d445c-9c1e-434f-8275-f18f1f320f81.txt"
        redis_queue = [{"url": storage_path, "build_code": "some_random_build"}]
        jsonified_redis_queue = [json.dumps(x) for x in redis_queue]
        mocker.patch.object(UploadTask, "app", celery_app)

        commit = CommitFactory.create(
            message="",
            commitid="abf6d4df662c47e32460020ab14abf9303581429",
            repository__owner__oauth_token="GHTZB+Mi+kbl/ubudnSKTJYb/fgN4hRJVJYSIErtidEsCLDJBb8DZzkbXqLujHAnv28aKShXddE/OffwRuwKug==",
            repository__owner__username="ThiagoCodecov",
            repository__owner__service="github",
            repository__yaml={"codecov": {"max_report_age": "1y ago"}},
            repository__name="example-python",
            pullid=1,
            # Setting the time to _before_ patch centric default YAMLs start date of 2024-04-30
            repository__owner__createstamp=datetime(2023, 1, 1, tzinfo=UTC),
        )
        dbsession.add(commit)
        dbsession.flush()
        dbsession.refresh(commit)

        mock_redis.lists[f"uploads/{commit.repoid}/{commit.commitid}/test_results"] = (
            jsonified_redis_queue
        )

        UploadTask().run_impl(
            dbsession,
            commit.repoid,
            commit.commitid,
            report_type="test_results",
        )
        commit_report = commit.commit_report(report_type=ReportType.TEST_RESULTS)
        assert commit_report
        uploads = commit_report.uploads
        assert len(uploads) == 1
        upload = dbsession.query(Upload).filter_by(report_id=commit_report.id).first()
        processor_sig = test_results_processor_task.s(
            False,
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml={"codecov": {"max_report_age": "1y ago"}},
            arguments_list=[
                {
                    "url": storage_path,
                    "flags": [],
                    "build_code": "some_random_build",
                    "upload_id": upload.id,
                    "upload_pk": upload.id,
                }
            ],
            impl_type="old",
        )
        kwargs = {
            "repoid": commit.repoid,
            "commitid": commit.commitid,
            "commit_yaml": {"codecov": {"max_report_age": "1y ago"}},
            "checkpoints_TestResultsFlow": None,
            "impl_type": "old",
        }

        kwargs[_kwargs_key(TestResultsFlow)] = mocker.ANY
        notify_sig = test_results_finisher_task.signature(kwargs=kwargs)
        chain.assert_called_with(*[processor_sig, notify_sig])

    @pytest.mark.django_db
    def test_upload_task_call_new_ta_tasks(
        self,
        mocker,
        mock_configuration,
        dbsession,
        codecov_vcr,
        mock_storage,
        mock_redis,
        celery_app,
        mock_repo_provider_service,
    ):
        chain = mocker.patch("tasks.upload.chain")
        _ = mocker.patch("tasks.upload.NEW_TA_TASKS.check_value", return_value="both")
        storage_path = "v4/raw/2019-05-22/C3C4715CA57C910D11D5EB899FC86A7E/4c4e4654ac25037ae869caeb3619d485970b6304/a84d445c-9c1e-434f-8275-f18f1f320f81.txt"
        redis_queue = [{"url": storage_path, "build_code": "some_random_build"}]
        jsonified_redis_queue = [json.dumps(x) for x in redis_queue]
        mocker.patch.object(UploadTask, "app", celery_app)

        commit = CommitFactory.create(
            message="",
            commitid="abf6d4df662c47e32460020ab14abf9303581429",
            repository__owner__oauth_token="GHTZB+Mi+kbl/ubudnSKTJYb/fgN4hRJVJYSIErtidEsCLDJBb8DZzkbXqLujHAnv28aKShXddE/OffwRuwKug==",
            repository__owner__username="ThiagoCodecov",
            repository__owner__service="github",
            repository__yaml={"codecov": {"max_report_age": "1y ago"}},
            repository__name="example-python",
            pullid=1,
            # Setting the time to _before_ patch centric default YAMLs start date of 2024-04-30
            repository__owner__createstamp=datetime(2023, 1, 1, tzinfo=UTC),
            branch="main",
        )
        dbsession.add(commit)
        dbsession.flush()
        dbsession.refresh(commit)

        mock_repo_provider_service.get_commit.return_value = {
            "author": {
                "id": "123",
                "username": "456",
                "email": "789",
                "name": "101",
            },
            "message": "hello world",
            "parents": [],
            "timestamp": str(NEW_TA_TASKS_CUTOFF_DATE - timedelta(days=10)),
        }

        mock_redis.lists[f"uploads/{commit.repoid}/{commit.commitid}/test_results"] = (
            jsonified_redis_queue
        )

        UploadTask().run_impl(
            dbsession,
            commit.repoid,
            commit.commitid,
            report_type="test_results",
        )
        commit_report = commit.commit_report(report_type=ReportType.TEST_RESULTS)
        assert commit_report
        uploads = commit_report.uploads
        assert len(uploads) == 1
        upload = dbsession.query(Upload).filter_by(report_id=commit_report.id).first()
        processor_sig = test_results_processor_task.s(
            False,
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml={"codecov": {"max_report_age": "1y ago"}},
            arguments_list=[
                {
                    "url": storage_path,
                    "flags": [],
                    "build_code": "some_random_build",
                    "upload_id": upload.id,
                    "upload_pk": upload.id,
                }
            ],
            impl_type="both",
        )
        kwargs = {
            "repoid": commit.repoid,
            "commitid": commit.commitid,
            "commit_yaml": {"codecov": {"max_report_age": "1y ago"}},
            "checkpoints_TestResultsFlow": None,
            "impl_type": "both",
        }

        kwargs[_kwargs_key(TestResultsFlow)] = mocker.ANY
        notify_sig = test_results_finisher_task.signature(kwargs=kwargs)
        chain.assert_has_calls([call(processor_sig, notify_sig)], any_order=True)

    @pytest.mark.django_db(databases={"default"}, transaction=True)
    def test_upload_task_call_test_results_new_repo(
        self,
        mocker,
        mock_configuration,
        dbsession,
        codecov_vcr,
        mock_storage,
        mock_redis,
        celery_app,
        mock_repo_provider_service,
    ):
        chain = mocker.patch("tasks.upload.chain")
        storage_path = "v4/raw/2019-05-22/C3C4715CA57C910D11D5EB899FC86A7E/4c4e4654ac25037ae869caeb3619d485970b6304/a84d445c-9c1e-434f-8275-f18f1f320f81.txt"
        redis_queue = [{"url": storage_path, "build_code": "some_random_build"}]
        jsonified_redis_queue = [json.dumps(x) for x in redis_queue]

        commit = CommitFactory.create(
            message="",
            commitid="abf6d4df662c47e32460020ab14abf9303581429",
            repository__owner__oauth_token="GHTZB+Mi+kbl/ubudnSKTJYb/fgN4hRJVJYSIErtidEsCLDJBb8DZzkbXqLujHAnv28aKShXddE/OffwRuwKug==",
            repository__owner__username="ThiagoCodecov",
            repository__owner__service="github",
            repository__yaml={"codecov": {"max_report_age": "1y ago"}},
            repository__name="example-python",
            pullid=1,
            # Setting the time to _before_ patch centric default YAMLs start date of 2024-04-30
            repository__owner__createstamp=datetime(2023, 1, 1, tzinfo=UTC),
        )
        dbsession.add(commit)
        dbsession.flush()
        dbsession.refresh(commit)
        commit.timestamp = NEW_TA_TASKS_CUTOFF_DATE.replace(
            day=21
        )  # One day after cutoff
        dbsession.flush()

        mock_redis.lists[f"uploads/{commit.repoid}/{commit.commitid}/test_results"] = (
            jsonified_redis_queue
        )

        mock_repo_provider_service.get_commit.return_value["timestamp"] = (
            NEW_TA_TASKS_CUTOFF_DATE.replace(day=21)
        )  # One day after cutoff

        UploadTask().run_impl(
            dbsession,
            commit.repoid,
            commit.commitid,
            report_type="test_results",
        )

        commit_report = commit.commit_report(report_type=ReportType.TEST_RESULTS)
        assert commit_report
        uploads = commit_report.uploads
        assert len(uploads) == 1
        upload = dbsession.query(Upload).filter_by(report_id=commit_report.id).first()
        processor_sig = test_results_processor_task.s(
            False,
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml={"codecov": {"max_report_age": "1y ago"}},
            arguments_list=[
                {
                    "url": storage_path,
                    "flags": [],
                    "build_code": "some_random_build",
                    "upload_id": upload.id,
                    "upload_pk": upload.id,
                }
            ],
            impl_type="new",
        )
        kwargs = {
            "repoid": commit.repoid,
            "commitid": commit.commitid,
            "commit_yaml": {"codecov": {"max_report_age": "1y ago"}},
            "checkpoints_TestResultsFlow": None,
            "impl_type": "new",
        }

        kwargs[_kwargs_key(TestResultsFlow)] = mocker.ANY
        notify_sig = test_results_finisher_task.signature(kwargs=kwargs)
        chain.assert_called_with(*[processor_sig, notify_sig])

    def test_upload_task_call_no_jobs(
        self,
        mocker,
        mock_configuration,
        dbsession,
        codecov_vcr,
        mock_storage,
        mock_redis,
        celery_app,
    ):
        mocker.patch.object(UploadTask, "app", celery_app)

        commit = CommitFactory.create(
            parent_commit_id=None,
            message="",
            commitid="abf6d4df662c47e32460020ab14abf9303581429",
            repository__owner__unencrypted_oauth_token="test7lk5ndmtqzxlx06rip65nac9c7epqopclnoy",
            repository__owner__username="ThiagoCodecov",
            repository__yaml={"codecov": {"max_report_age": "1y ago"}},
            repository__name="example-python",
            pullid=1,
        )
        dbsession.add(commit)
        dbsession.flush()
        mock_redis.lists[f"uploads/{commit.repoid}/{commit.commitid}"] = []
        result = UploadTask().run_impl(dbsession, commit.repoid, commit.commitid)
        expected_result = {
            "was_setup": False,
            "was_updated": False,
            "tasks_were_scheduled": False,
        }
        assert expected_result == result
        assert commit.message == ""
        assert commit.parent_commit_id is None

    @pytest.mark.django_db
    def test_upload_task_upload_processing_delay_not_enough_delay(
        self,
        mocker,
        mock_configuration,
        dbsession,
        mock_storage,
        celery_app,
    ):
        mock_possibly_update_commit_from_provider_info = mocker.patch(
            "tasks.upload.possibly_update_commit_from_provider_info", return_value=True
        )
        mocker.patch.object(UploadTask, "possibly_setup_webhooks", return_value=True)
        mock_configuration.set_params({"setup": {"upload_processing_delay": 1000}})
        mocker.patch.object(UploadTask, "app", celery_app)

        commit = CommitFactory.create(
            parent_commit_id=None,
            message="",
            commitid="abf6d4df662c47e32460020ab14abf9303581429",
            repository__owner__unencrypted_oauth_token="test7lk5ndmtqzxlx06rip65nac9c7epqopclnoy",
            repository__owner__username="ThiagoCodecov",
            repository__yaml={"codecov": {"max_report_age": "1y ago"}},
            repository__name="example-python",
            pullid=1,
        )
        dbsession.add(commit)
        dbsession.flush()

        redis = get_redis_connection()
        redis.lpush(
            f"uploads/{commit.repoid}/{commit.commitid}",
            '{"build": "part1", "url": "someurl1"}',
        )
        redis.lpush(
            f"uploads/{commit.repoid}/{commit.commitid}",
            '{"build": "part2", "url": "someurl2"}',
        )
        redis.set(
            f"latest_upload/{commit.repoid}/{commit.commitid}",
            (datetime.now() - timedelta(seconds=10)).timestamp(),
        )

        with pytest.raises(Retry):
            UploadTask().run_impl(dbsession, commit.repoid, commit.commitid)

        assert commit.message == ""
        assert commit.parent_commit_id is None
        assert redis.exists(f"uploads/{commit.repoid}/{commit.commitid}")
        assert not mock_possibly_update_commit_from_provider_info.called

    @pytest.mark.django_db
    def test_upload_task_upload_processing_delay_enough_delay(
        self,
        mocker,
        mock_configuration,
        dbsession,
        mock_storage,
        celery_app,
    ):
        mocker.patch(
            "tasks.upload.possibly_update_commit_from_provider_info", return_value=True
        )
        mocker.patch.object(UploadTask, "possibly_setup_webhooks", return_value=True)
        commit = CommitFactory.create(
            parent_commit_id=None,
            message="",
            commitid="abf6d4df662c47e32460020ab14abf9303581429",
            repository__owner__unencrypted_oauth_token="test7lk5ndmtqzxlx06rip65nac9c7epqopclnoy",
            repository__owner__username="ThiagoCodecov",
            repository__yaml={"codecov": {"max_report_age": "1y ago"}},
            repository__name="example-python",
            pullid=1,
        )
        mock_configuration.set_params({"setup": {"upload_processing_delay": 1000}})
        mocker.patch.object(UploadTask, "app", celery_app)
        mocker.patch.object(UploadTask, "possibly_setup_webhooks", return_value=True)
        mocked_chord = mocker.patch("tasks.upload.chord")
        dbsession.add(commit)
        dbsession.flush()

        redis = get_redis_connection()
        redis.set(
            f"latest_upload/{commit.repoid}/{commit.commitid}",
            (datetime.now() - timedelta(seconds=1200)).timestamp(),
        )
        redis.lpush(
            f"uploads/{commit.repoid}/{commit.commitid}",
            '{"build": "part1", "url": "someurl1"}',
        )
        redis.lpush(
            f"uploads/{commit.repoid}/{commit.commitid}",
            '{"build": "part2", "url": "someurl2"}',
        )

        result = UploadTask().run_impl(dbsession, commit.repoid, commit.commitid)

        assert result == {"was_setup": True, "was_updated": True}
        assert commit.message == ""
        assert commit.parent_commit_id is None
        assert not redis.exists(f"uploads/{commit.repoid}/{commit.commitid}")
        mocked_chord.assert_called_with([mocker.ANY, mocker.ANY], mocker.ANY)

    @pytest.mark.django_db
    @pytest.mark.skip(reason="Bitbucket down is breaking this test")
    def test_upload_task_upload_processing_delay_upload_is_none(
        self,
        mocker,
        mock_configuration,
        dbsession,
        mock_storage,
        celery_app,
    ):
        mock_configuration.set_params({"setup": {"upload_processing_delay": 1000}})
        mocker.patch.object(UploadTask, "app", celery_app)
        mocker.patch(
            "tasks.upload.possibly_update_commit_from_provider_info", return_value=True
        )
        mocker.patch.object(UploadTask, "possibly_setup_webhooks", return_value=True)
        mocked_chord = mocker.patch("tasks.upload.chord")
        commit = CommitFactory.create(
            parent_commit_id=None,
            message="",
            commitid="abf6d4df662c47e32460020ab14abf9303581429",
            repository__owner__unencrypted_oauth_token="test7lk5ndmtqzxlx06rip65nac9c7epqopclnoy",
            repository__owner__username="ThiagoCodecov",
            repository__yaml={"codecov": {"max_report_age": "1y ago"}},
            repository__name="example-python",
            pullid=1,
        )
        dbsession.add(commit)
        dbsession.flush()

        redis = get_redis_connection()
        redis.lpush(
            f"uploads/{commit.repoid}/{commit.commitid}",
            '{"build": "part1", "url": "someurl1"}',
        )
        redis.lpush(
            f"uploads/{commit.repoid}/{commit.commitid}",
            '{"build": "part2", "url": "someurl2"}',
        )

        result = UploadTask().run_impl(dbsession, commit.repoid, commit.commitid)

        assert result == {"was_setup": True, "was_updated": True}
        assert commit.message == ""
        assert commit.parent_commit_id is None
        assert not redis.exists(f"uploads/{commit.repoid}/{commit.commitid}")
        mocked_chord.assert_called_with([mocker.ANY, mocker.ANY], mocker.ANY)

    @pytest.mark.django_db
    def test_upload_task_call_multiple_processors(
        self,
        mocker,
        mock_configuration,
        dbsession,
        codecov_vcr,
        mock_storage,
        celery_app,
    ):
        mocked_chord = mocker.patch("tasks.upload.chord")
        mocker.patch.object(UploadTask, "app", celery_app)

        commit = CommitFactory.create(
            message="",
            commitid="abf6d4df662c47e32460020ab14abf9303581429",
            repository__owner__unencrypted_oauth_token="test7lk5ndmtqzxlx06rip65nac9c7epqopclnoy",
            repository__owner__username="ThiagoCodecov",
            repository__owner__service="github",
            repository__yaml={"codecov": {"max_report_age": "1y ago"}},
            repository__name="example-python",
            pullid=1,
            # Setting the time to _before_ patch centric default YAMLs start date of 2024-04-30
            repository__owner__createstamp=datetime(2023, 1, 1, tzinfo=UTC),
        )
        dbsession.add(commit)
        dbsession.flush()

        redis = get_redis_connection()

        redis_queue = [
            {"build": "part1", "url": "someurl1"},
            {"build": "part2", "url": "someurl2"},
            {"build": "part3", "url": "someurl3"},
            {"build": "part4", "url": "someurl4"},
            {"build": "part5", "url": "someurl5"},
            {"build": "part6", "url": "someurl6"},
            {"build": "part7", "url": "someurl7"},
            {"build": "part8", "url": "someurl8"},
        ]
        for arguments in redis_queue:
            redis.lpush(
                f"uploads/{commit.repoid}/{commit.commitid}", json.dumps(arguments)
            )

        result = UploadTask().run_impl(dbsession, commit.repoid, commit.commitid)

        assert result == {"was_setup": False, "was_updated": True}
        assert commit.message == "dsidsahdsahdsa"
        assert commit.parent_commit_id is None
        processors = [
            upload_processor_task.s(
                repoid=commit.repoid,
                commitid="abf6d4df662c47e32460020ab14abf9303581429",
                commit_yaml={"codecov": {"max_report_age": "1y ago"}},
                arguments={
                    **arguments,
                    "flags": [],
                    "upload_id": mocker.ANY,
                    "upload_pk": mocker.ANY,
                },
            )
            for arguments in redis_queue
        ]
        processors.reverse()  # whatever the reason
        kwargs = {
            "repoid": commit.repoid,
            "commitid": "abf6d4df662c47e32460020ab14abf9303581429",
            "commit_yaml": {"codecov": {"max_report_age": "1y ago"}},
        }
        kwargs[_kwargs_key(UploadFlow)] = mocker.ANY
        t_final = upload_finisher_task.signature(kwargs=kwargs)
        mocked_chord.assert_called_with(processors, t_final)

    def test_upload_task_proper_parent(
        self,
        mocker,
        mock_configuration,
        dbsession,
        codecov_vcr,
        mock_storage,
        mock_redis,
        celery_app,
    ):
        mocked_1 = mocker.patch("tasks.upload.chain")
        mocker.patch.object(UploadTask, "app", celery_app)
        mocked_3 = mocker.patch.object(UploadContext, "arguments_list", return_value=[])

        owner = OwnerFactory.create(
            service="github",
            username="ThiagoCodecov",
            unencrypted_oauth_token="test76zow6xgh7modd88noxr245j2z25t4ustoff",
            # Setting the time to _before_ patch centric default YAMLs start date of 2024-04-30
            createstamp=datetime(2023, 1, 1, tzinfo=UTC),
        )
        dbsession.add(owner)

        repo = RepositoryFactory.create(
            owner=owner,
            yaml={"codecov": {"max_report_age": "1y ago"}},
            name="example-python",
        )
        dbsession.add(repo)

        parent_commit = CommitFactory.create(
            message="",
            commitid="c5b67303452bbff57cc1f49984339cde39eb1db5",
            repository=repo,
            pullid=1,
        )

        commit = CommitFactory.create(
            message="",
            commitid="abf6d4df662c47e32460020ab14abf9303581429",
            repository=repo,
            pullid=1,
        )
        dbsession.add(parent_commit)
        dbsession.add(commit)
        dbsession.flush()
        redis_queue = [{"build": "part1"}]
        jsonified_redis_queue = [json.dumps(x) for x in redis_queue]
        mock_redis.lists[f"uploads/{commit.repoid}/{commit.commitid}"] = (
            jsonified_redis_queue
        )

        result = UploadTask().run_impl(dbsession, commit.repoid, commit.commitid)
        expected_result = {"was_setup": False, "was_updated": True}
        assert expected_result == result
        assert commit.message == "dsidsahdsahdsa"
        assert commit.parent_commit_id == "c5b67303452bbff57cc1f49984339cde39eb1db5"
        assert not mocked_1.called
        mock_redis.lock.assert_any_call(
            f"upload_lock_{commit.repoid}_{commit.commitid}",
            blocking_timeout=5,
            timeout=300,
        )

    @pytest.mark.django_db
    def test_upload_task_no_bot(
        self,
        mocker,
        mock_configuration,
        dbsession,
        mock_redis,
        mock_storage,
        celery_app,
    ):
        mocked_schedule_task = mocker.patch.object(UploadTask, "schedule_task")
        mocker.patch.object(UploadTask, "app", celery_app)
        mocked_fetch_yaml = mocker.patch(
            "services.repository.fetch_commit_yaml_and_possibly_store"
        )
        redis_queue = [
            {"build": "part1", "url": "url1"},
            {"build": "part2", "url": "url2"},
        ]
        jsonified_redis_queue = [json.dumps(x) for x in redis_queue]
        mock_get_repo_service = mocker.patch("tasks.base.get_repo_provider_service")
        mock_get_repo_service.side_effect = RepositoryWithoutValidBotError()
        commit = CommitFactory.create(
            message="",
            parent_commit_id=None,
            commitid="abf6d4df662c47e32460020ab14abf9303581429",
            repository__owner__unencrypted_oauth_token="test7lk5ndmtqzxlx06rip65nac9c7epqopclnoy",
            repository__owner__username="ThiagoCodecov",
            repository__yaml={"codecov": {"max_report_age": "764y ago"}},
            repository__name="example-python",
            # Setting the time to _before_ patch centric default YAMLs start date of 2024-04-30
            repository__owner__createstamp=datetime(2023, 1, 1, tzinfo=UTC),
        )
        dbsession.add(commit)
        dbsession.flush()
        mock_redis.lists[f"uploads/{commit.repoid}/{commit.commitid}"] = (
            jsonified_redis_queue
        )
        result = UploadTask().run_impl(dbsession, commit.repoid, commit.commitid)
        expected_result = {"was_setup": False, "was_updated": False}
        assert expected_result == result
        assert commit.message == ""
        assert commit.parent_commit_id is None
        mocked_schedule_task.assert_called_with(
            commit,
            {"codecov": {"max_report_age": "764y ago"}},
            [
                {
                    "build": "part1",
                    "url": "url1",
                    "flags": [],
                    "upload_id": mocker.ANY,
                    "upload_pk": mocker.ANY,
                },
                {
                    "build": "part2",
                    "url": "url2",
                    "flags": [],
                    "upload_id": mocker.ANY,
                    "upload_pk": mocker.ANY,
                },
            ],
            commit.report,
            mocker.ANY,
        )
        assert not mocked_fetch_yaml.called

    @pytest.mark.django_db
    def test_upload_task_bot_no_permissions(
        self,
        mocker,
        mock_configuration,
        dbsession,
        mock_redis,
        mock_storage,
        celery_app,
    ):
        mocked_schedule_task = mocker.patch.object(UploadTask, "schedule_task")
        mocker.patch.object(UploadTask, "app", celery_app)
        mocked_fetch_yaml = mocker.patch(
            "services.repository.fetch_commit_yaml_and_possibly_store"
        )
        redis_queue = [
            {"build": "part1", "url": "url1"},
            {"build": "part2", "url": "url2"},
        ]
        jsonified_redis_queue = [json.dumps(x) for x in redis_queue]
        mock_get_repo_service = mocker.patch("tasks.base.get_repo_provider_service")
        mock_get_repo_service.side_effect = TorngitRepoNotFoundError(
            "fake_response", "message"
        )
        commit = CommitFactory.create(
            message="",
            parent_commit_id=None,
            repository__owner__unencrypted_oauth_token="test7lk5ndmtqzxlx06rip65nac9c7epqopclnoy",
            repository__owner__username="ThiagoCodecov",
            repository__yaml={"codecov": {"max_report_age": "764y ago"}},
            repository__name="example-python",
            # Setting the time to _before_ patch centric default YAMLs start date of 2024-04-30
            repository__owner__createstamp=datetime(2023, 1, 1, tzinfo=UTC),
        )
        dbsession.add(commit)
        dbsession.flush()
        mock_redis.lists[f"uploads/{commit.repoid}/{commit.commitid}"] = (
            jsonified_redis_queue
        )
        result = UploadTask().run_impl(dbsession, commit.repoid, commit.commitid)
        expected_result = {"was_setup": False, "was_updated": False}
        assert expected_result == result
        assert commit.message == ""
        assert commit.parent_commit_id is None
        mocked_schedule_task.assert_called_with(
            commit,
            {"codecov": {"max_report_age": "764y ago"}},
            [
                {
                    "build": "part1",
                    "url": "url1",
                    "flags": [],
                    "upload_id": mocker.ANY,
                    "upload_pk": mocker.ANY,
                },
                {
                    "build": "part2",
                    "url": "url2",
                    "flags": [],
                    "upload_id": mocker.ANY,
                    "upload_pk": mocker.ANY,
                },
            ],
            commit.report,
            mocker.ANY,
        )
        assert not mocked_fetch_yaml.called

    @pytest.mark.django_db
    def test_upload_task_bot_unauthorized(
        self,
        mocker,
        mock_configuration,
        dbsession,
        mock_redis,
        mock_repo_provider,
        mock_storage,
    ):
        mocked_schedule_task = mocker.patch.object(UploadTask, "schedule_task")
        mock_app = mocker.patch.object(UploadTask, "app")
        mock_app.send_task.return_value = True
        redis_queue = [
            {"build": "part1", "url": "url1"},
            {"build": "part2", "url": "url2"},
        ]
        jsonified_redis_queue = [json.dumps(x) for x in redis_queue]
        mock_repo_provider.get_commit.side_effect = TorngitClientError(
            401, "response", "message"
        )
        mock_repo_provider.list_top_level_files.side_effect = TorngitClientError(
            401, "response", "message"
        )
        commit = CommitFactory.create(
            message="",
            parent_commit_id=None,
            repository__owner__unencrypted_oauth_token="test7lk5ndmtqzxlx06rip65nac9c7epqopclnoy",
            repository__owner__username="ThiagoCodecov",
            repository__yaml={"codecov": {"max_report_age": "764y ago"}},
            # Setting the time to _before_ patch centric default YAMLs start date of 2024-04-30
            repository__owner__createstamp=datetime(2023, 1, 1, tzinfo=UTC),
        )
        mock_repo_provider.data = {"repo": {"repoid": commit.repoid}}
        dbsession.add(commit)
        dbsession.flush()
        mock_redis.lists[f"uploads/{commit.repoid}/{commit.commitid}"] = (
            jsonified_redis_queue
        )
        upload_args = UploadContext(
            repoid=commit.repoid,
            commitid=commit.commitid,
            redis_connection=mock_redis,
        )
        result = UploadTask().run_impl_within_lock(dbsession, upload_args, kwargs={})
        assert {"was_setup": False, "was_updated": False} == result
        assert commit.message == ""
        assert commit.parent_commit_id is None
        assert commit.report is not None
        sessions = commit.report.uploads
        assert len(sessions) == 2
        first_session = (
            dbsession.query(Upload)
            .filter_by(report_id=commit.report.id, build_code="part1")
            .first()
        )
        second_session = (
            dbsession.query(Upload)
            .filter_by(report_id=commit.report.id, build_code="part2")
            .first()
        )
        mocked_schedule_task.assert_called_with(
            commit,
            {"codecov": {"max_report_age": "764y ago"}},
            [
                {
                    "build": "part1",
                    "url": "url1",
                    "flags": [],
                    "upload_id": first_session.id,
                    "upload_pk": first_session.id,
                },
                {
                    "build": "part2",
                    "url": "url2",
                    "flags": [],
                    "upload_id": second_session.id,
                    "upload_pk": second_session.id,
                },
            ],
            commit.report,
            mocker.ANY,
        )

    def test_upload_task_upload_already_created(
        self,
        mocker,
        mock_configuration,
        dbsession,
        mock_redis,
        mock_repo_provider,
        mock_storage,
    ):
        mocked_schedule_task = mocker.patch.object(UploadTask, "schedule_task")
        mocker.patch(
            "tasks.upload.possibly_update_commit_from_provider_info", return_value=True
        )
        mock_create_upload = mocker.patch.object(ReportService, "create_report_upload")

        def fail_if_try_to_create_upload(*args, **kwargs):
            raise Exception("tried to create Upload")

        mock_create_upload.side_effect = fail_if_try_to_create_upload
        mocker.patch.object(UploadTask, "possibly_setup_webhooks", return_value=True)
        mock_app = mocker.patch.object(UploadTask, "app")
        mock_app.send_task.return_value = True
        reportid = "5fbeee8b-5a41-4925-b59d-470b9d171235"
        commit = CommitFactory.create(
            message="",
            parent_commit_id=None,
            repository__owner__unencrypted_oauth_token="test7lk5ndmtqzxlx06rip65nac9c7epqopclnoy",
            repository__owner__username="ThiagoCodecov",
            repository__yaml={"codecov": {"max_report_age": "764y ago"}},
            # Setting the time to _before_ patch centric default YAMLs start date of 2024-04-30
            repository__owner__createstamp=datetime(2023, 1, 1, tzinfo=UTC),
        )
        report = CommitReport(commit_id=commit.id_)
        upload = Upload(
            external_id=reportid,
            build_code="part1",
            build_url="build_url",
            env=None,
            report=report,
            job_code="job",
            name="name",
            provider="service",
            state="started",
            storage_path="url",
            order_number=None,
            upload_extras={},
            upload_type=SessionType.uploaded.value,
            state_id=UploadState.UPLOADED.db_id,
            upload_type_id=UploadType.UPLOADED.db_id,
        )
        mock_repo_provider.data = {"repo": {"repoid": commit.repoid}}
        dbsession.add(commit)
        dbsession.add(report)
        dbsession.add(upload)
        dbsession.flush()

        redis_queue = [
            {"build": "part1", "url": "url1", "upload_id": upload.id_},
        ]
        jsonified_redis_queue = [json.dumps(x) for x in redis_queue]
        mock_redis.lists[f"uploads/{commit.repoid}/{commit.commitid}"] = (
            jsonified_redis_queue
        )
        upload_args = UploadContext(
            repoid=commit.repoid,
            commitid=commit.commitid,
            redis_connection=mock_redis,
        )
        result = UploadTask().run_impl_within_lock(dbsession, upload_args, kwargs={})
        assert {"was_setup": True, "was_updated": True} == result
        assert commit.message == ""
        assert commit.parent_commit_id is None
        assert commit.report is not None
        mocked_schedule_task.assert_called_with(
            commit,
            {"codecov": {"max_report_age": "764y ago"}},
            [
                {
                    "build": "part1",
                    "url": "url1",
                    "flags": [],
                    "upload_id": upload.id_,
                    "upload_pk": upload.id_,
                }
            ],
            report,
            mocker.ANY,
        )


class TestUploadTaskUnit:
    def test_list_of_arguments(self, mock_redis):
        upload_args = UploadContext(
            repoid=542,
            commitid="commitid",
            redis_connection=mock_redis,
        )
        first_redis_queue = [
            {"url": "http://example.first.com"},
            {"and_another": "one"},
        ]
        mock_redis.lists["uploads/542/commitid"] = [
            json.dumps(x) for x in first_redis_queue
        ]
        res = list(upload_args.arguments_list())
        assert res == [{"url": "http://example.first.com"}, {"and_another": "one"}]

    @pytest.mark.django_db
    def test_schedule_task_with_one_task(self, dbsession, mocker, mock_repo_provider):
        _start_upload_flow(mocker)
        mocker.patch(
            "tasks.base.get_repo_provider_service",
            return_value=mock_repo_provider,
        )
        mocker.patch(
            "tasks.upload.UploadTask.possibly_setup_webhooks", return_value=True
        )
        mocked_chord = mocker.patch("tasks.upload.chord")
        commit = CommitFactory.create()
        commit_yaml = {"codecov": {"max_report_age": "100y ago"}}
        dbsession.add(commit)
        dbsession.flush()
        upload_args = UploadContext(
            repoid=commit.repoid,
            commitid=commit.commitid,
            redis_connection=mock_redis,
        )
        result = UploadTask().schedule_task(
            commit,
            commit_yaml,
            [{"upload_id": 1, "upload_pk": 1}],
            ReportFactory.create(),
            upload_args,
        )
        assert result == mocked_chord.return_value.apply_async.return_value
        processor = upload_processor_task.s(
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml=commit_yaml,
            arguments={"upload_id": 1, "upload_pk": 1},
        )
        finisher = upload_finisher_task.signature(
            kwargs={
                "repoid": commit.repoid,
                "commitid": commit.commitid,
                "commit_yaml": commit_yaml,
                _kwargs_key(UploadFlow): mocker.ANY,
            }
        )
        mocked_chord.assert_called_with([processor], finisher)

    def test_run_impl_unobtainable_lock_no_pending_jobs(
        self, dbsession, mocker, mock_redis
    ):
        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()
        mock_redis.lock.side_effect = LockError()
        result = UploadTask().run_impl(dbsession, commit.repoid, commit.commitid)
        assert result == {
            "tasks_were_scheduled": False,
            "was_setup": False,
            "was_updated": False,
        }

    def test_run_impl_unobtainable_lock_too_many_retries(
        self, dbsession, mocker, mock_redis
    ):
        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()
        mock_redis.lock.side_effect = LockError()
        mock_redis.keys[f"uploads/{commit.repoid}/{commit.commitid}"] = ["something"]
        task = UploadTask()
        task.request.retries = 3
        result = task.run_impl(dbsession, commit.repoid, commit.commitid)
        assert result == {
            "tasks_were_scheduled": False,
            "was_setup": False,
            "was_updated": False,
            "reason": "too_many_retries",
        }

    def test_run_impl_currently_processing(self, dbsession, mocker, mock_redis):
        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()
        mocked_is_currently_processing = mocker.patch.object(
            UploadContext, "is_currently_processing", return_value=True
        )
        mocked_run_impl_within_lock = mocker.patch.object(
            UploadTask, "run_impl_within_lock", return_value=True
        )
        mock_redis.keys[f"uploads/{commit.repoid}/{commit.commitid}"] = ["something"]
        task = UploadTask()
        task.request.retries = 0
        with pytest.raises(Retry):
            task.run_impl(dbsession, commit.repoid, commit.commitid)
        mocked_is_currently_processing.assert_called_with()
        assert not mocked_run_impl_within_lock.called

    def test_run_impl_currently_processing_second_retry(
        self, dbsession, mocker, mock_redis
    ):
        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()
        mocked_is_currently_processing = mocker.patch.object(
            UploadContext, "is_currently_processing", return_value=True
        )
        mocked_run_impl_within_lock = mocker.patch.object(
            UploadTask, "run_impl_within_lock", return_value={"some": "value"}
        )
        mock_redis.keys[f"uploads/{commit.repoid}/{commit.commitid}"] = ["something"]
        task = UploadTask()
        task.request.retries = 1
        result = task.run_impl(dbsession, commit.repoid, commit.commitid)
        mocked_is_currently_processing.assert_called_with()
        assert mocked_run_impl_within_lock.called
        assert result == {"some": "value"}

    def test_is_currently_processing(self, mock_redis):
        repoid = 1
        commitid = "adsdadsadfdsjnskgiejrw"
        lock_name = f"upload_processing_lock_{repoid}_{commitid}"
        mock_redis.keys[lock_name] = "val"
        upload_args = UploadContext(
            repoid=repoid,
            commitid=commitid,
            redis_connection=mock_redis,
        )
        assert upload_args.is_currently_processing()
        upload_args = UploadContext(
            repoid=repoid,
            commitid="pol",
            redis_connection=mock_redis,
        )
        assert not upload_args.is_currently_processing()

    def test_run_impl_unobtainable_lock_retry(self, dbsession, mocker, mock_redis):
        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()
        mock_redis.lock.side_effect = LockError()
        mock_redis.keys[f"uploads/{commit.repoid}/{commit.commitid}"] = ["something"]
        task = UploadTask()
        task.request.retries = 0
        with pytest.raises(Retry):
            task.run_impl(dbsession, commit.repoid, commit.commitid)

    def test_possibly_setup_webhooks_public_repo(
        self, mocker, mock_configuration, mock_repo_provider
    ):
        mock_configuration.set_params({"github": {"bot": {"key": "somekey"}}})
        commit = CommitFactory.create(
            repository__private=False,
            repository__owner__unencrypted_oauth_token="aaaaabbbbhhhh",
        )
        task = UploadTask()
        mock_repo_provider.data = mocker.MagicMock()
        mock_repo_provider.service = "github"
        res = task.possibly_setup_webhooks(commit, mock_repo_provider)
        assert res is True
        mock_repo_provider.post_webhook.assert_called_with(
            "Codecov Webhook. None",
            "None/webhooks/github",
            ["pull_request", "delete", "push", "public", "status", "repository"],
            "ab164bf3f7d947f2a0681b215404873e",
            token=None,
        )

    def test_possibly_setup_webhooks_owner_has_ghapp(
        self, mocker, dbsession, mock_configuration, mock_repo_provider
    ):
        mock_configuration.set_params({"github": {"bot": {"key": "somekey"}}})
        commit = CommitFactory.create(
            repository__using_integration=False, repository__hookid=None
        )
        ghapp = GithubAppInstallationFactory(
            installation_id=1234,
            owner=commit.repository.owner,
        )
        dbsession.add(ghapp)
        dbsession.flush()
        task = UploadTask()
        mock_repo_provider.data = mocker.MagicMock()
        mock_repo_provider.service = "github"
        res = task.possibly_setup_webhooks(commit, mock_repo_provider)
        assert res is False
        mock_repo_provider.post_webhook.assert_not_called()

    def test_possibly_setup_webhooks_gitlab(
        self, dbsession, mocker, mock_configuration
    ):
        mock_configuration.set_params({"gitlab": {"bot": {"key": "somekey"}}})
        repository = RepositoryFactory.create()
        dbsession.add(repository)
        commit = CommitFactory.create(repository=repository)
        dbsession.add(commit)

        gitlab_provider = mocker.MagicMock(
            Gitlab, get_commit_diff=mock.AsyncMock(return_value={})
        )
        mock_repo_provider = mocker.patch(
            "services.repository._get_repo_provider_service_instance"
        )
        mock_repo_provider.return_value = gitlab_provider
        gitlab_provider.data = mocker.MagicMock()
        gitlab_provider.service = "gitlab"

        task = UploadTask()
        res = task.possibly_setup_webhooks(commit, gitlab_provider)
        assert res is True

        assert repository.webhook_secret is not None
        gitlab_provider.post_webhook.assert_called_with(
            "Codecov Webhook. None",
            "None/webhooks/gitlab",
            {
                "push_events": True,
                "issues_events": False,
                "merge_requests_events": True,
                "tag_push_events": False,
                "note_events": False,
                "job_events": False,
                "build_events": True,
                "pipeline_events": True,
                "wiki_events": False,
            },
            commit.repository.webhook_secret,
            token=None,
        )

    def test_needs_webhook_secret_backfill(self, dbsession, mocker, mock_configuration):
        mock_configuration.set_params({"gitlab": {"bot": {"key": "somekey"}}})
        repository = RepositoryFactory.create(
            repoid="5678", hookid="1234", webhook_secret=None
        )
        dbsession.add(repository)
        commit = CommitFactory.create(repository=repository)
        dbsession.add(commit)
        gitlab_provider = mocker.MagicMock(
            Gitlab, get_commit_diff=mock.AsyncMock(return_value={})
        )
        mock_repo_provider = mocker.patch(
            "services.repository._get_repo_provider_service_instance"
        )
        mock_repo_provider.return_value = gitlab_provider
        gitlab_provider.data = mocker.MagicMock()
        gitlab_provider.service = "gitlab"
        task = UploadTask()
        res = task.possibly_setup_webhooks(commit, gitlab_provider)
        assert res is False

        assert repository.webhook_secret is not None
        gitlab_provider.edit_webhook.assert_called_with(
            hookid=repository.hookid,
            name="Codecov Webhook. None",
            url="None/webhooks/gitlab",
            events={
                "push_events": True,
                "issues_events": False,
                "merge_requests_events": True,
                "tag_push_events": False,
                "note_events": False,
                "job_events": False,
                "build_events": True,
                "pipeline_events": True,
                "wiki_events": False,
            },
            secret=commit.repository.webhook_secret,
        )

    def test_needs_webhook_secret_backfill_gle(
        self, dbsession, mocker, mock_configuration
    ):
        mock_configuration.set_params(
            {"gitlab_enterprise": {"bot": {"key": "somekey"}}}
        )
        repository = RepositoryFactory.create(
            repoid="5678", hookid="1234", webhook_secret=None
        )
        dbsession.add(repository)
        commit = CommitFactory.create(repository=repository)
        dbsession.add(commit)
        gitlab_e_provider = mocker.MagicMock(
            GitlabEnterprise, get_commit_diff=mock.AsyncMock(return_value={})
        )
        mock_repo_provider = mocker.patch(
            "services.repository._get_repo_provider_service_instance"
        )
        mock_repo_provider.return_value = gitlab_e_provider
        gitlab_e_provider.data = mocker.MagicMock()
        gitlab_e_provider.service = "gitlab"
        task = UploadTask()
        res = task.possibly_setup_webhooks(commit, gitlab_e_provider)
        assert res is False

        assert repository.webhook_secret is not None
        gitlab_e_provider.edit_webhook.assert_called_with(
            hookid=repository.hookid,
            name="Codecov Webhook. None",
            url="None/webhooks/gitlab",
            events={
                "push_events": True,
                "issues_events": False,
                "merge_requests_events": True,
                "tag_push_events": False,
                "note_events": False,
                "job_events": False,
                "build_events": True,
                "pipeline_events": True,
                "wiki_events": False,
            },
            secret=commit.repository.webhook_secret,
        )

    def test_doesnt_need_webhook_secret_backfill(
        self, dbsession, mocker, mock_configuration
    ):
        mock_configuration.set_params({"gitlab": {"bot": {"key": "somekey"}}})
        secret = str(uuid.uuid4())
        repository = RepositoryFactory.create(
            repoid="5678", hookid="1234", webhook_secret=secret
        )
        dbsession.add(repository)
        commit = CommitFactory.create(repository=repository)
        dbsession.add(commit)
        gitlab_provider = mocker.MagicMock(
            Gitlab, get_commit_diff=mock.AsyncMock(return_value={})
        )
        mock_repo_provider = mocker.patch(
            "services.repository._get_repo_provider_service_instance"
        )
        mock_repo_provider.return_value = gitlab_provider
        gitlab_provider.data = mocker.MagicMock()
        gitlab_provider.service = "gitlab"
        task = UploadTask()
        res = task.possibly_setup_webhooks(commit, gitlab_provider)
        assert res is False

        assert repository.webhook_secret is secret
        gitlab_provider.edit_webhook.assert_not_called()

    def test_doesnt_need_webhook_secret_backfill_no_hookid(
        self, dbsession, mocker, mock_configuration
    ):
        mock_configuration.set_params({"gitlab": {"bot": {"key": "somekey"}}})
        repository = RepositoryFactory.create(
            repoid="5678", hookid=None, webhook_secret=None, using_integration=True
        )
        dbsession.add(repository)
        commit = CommitFactory.create(repository=repository)
        dbsession.add(commit)
        gitlab_provider = mocker.MagicMock(
            Gitlab, get_commit_diff=mock.AsyncMock(return_value={})
        )
        mock_repo_provider = mocker.patch(
            "services.repository._get_repo_provider_service_instance"
        )
        mock_repo_provider.return_value = gitlab_provider
        gitlab_provider.data = mocker.MagicMock()
        gitlab_provider.service = "gitlab"
        task = UploadTask()
        res = task.possibly_setup_webhooks(commit, gitlab_provider)
        assert res is False

        assert repository.webhook_secret is None
        gitlab_provider.edit_webhook.assert_not_called()

    def test_needs_webhook_secret_backfill_error(
        self, dbsession, mocker, mock_configuration
    ):
        mock_configuration.set_params(
            {"gitlab_enterprise": {"bot": {"key": "somekey"}}}
        )
        repository = RepositoryFactory.create(
            repoid="5678", hookid="1234", webhook_secret=None
        )
        dbsession.add(repository)
        commit = CommitFactory.create(repository=repository)
        dbsession.add(commit)
        gitlab_e_provider = mocker.MagicMock(
            GitlabEnterprise, get_commit_diff=mock.AsyncMock(return_value={})
        )
        mock_repo_provider = mocker.patch(
            "services.repository._get_repo_provider_service_instance"
        )
        mock_repo_provider.return_value = gitlab_e_provider
        gitlab_e_provider.data = mocker.MagicMock()
        gitlab_e_provider.service = "gitlab"
        gitlab_e_provider.edit_webhook.side_effect = TorngitClientError
        task = UploadTask()

        res = task.possibly_setup_webhooks(commit, gitlab_e_provider)
        assert res is False
        assert repository.webhook_secret is None
        gitlab_e_provider.edit_webhook.assert_called_once()

    def test_upload_not_ready_to_build_report(
        self, dbsession, mocker, mock_configuration, mock_repo_provider, mock_redis
    ):
        mock_configuration.set_params({"github": {"bot": {"key": "somekey"}}})
        commit = CommitFactory.create()
        dbsession.add(commit)
        dbsession.flush()
        mocker.patch.object(UploadContext, "has_pending_jobs", return_value=True)
        task = UploadTask()
        mock_repo_provider.data = mocker.MagicMock()
        mock_repo_provider.service = "github"
        mocker.patch.object(
            ReportService,
            "initialize_and_save_report",
            side_effect=NotReadyToBuildReportYetError(),
        )
        upload_args = UploadContext(
            repoid=commit.repoid,
            commitid=commit.commitid,
            redis_connection=mock_redis,
        )
        with pytest.raises(Retry):
            task.run_impl_within_lock(dbsession, upload_args, kwargs={})
