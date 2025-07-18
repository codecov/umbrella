from pathlib import Path
from unittest import mock

import fakeredis
import pytest
import vcr
from django.conf import settings

from shared.reports.resources import Report, ReportFile
from shared.reports.types import ReportLine
from shared.storage.memory import MemoryStorageService
from shared.testutils import django_setup_test_db
from shared.utils.sessions import Session

# we need to enable this in the test environment since we're often creating
# timeseries data and then asserting something about the aggregates all in
# a single transaction.  calling `refresh_continuous_aggregate` doesn't work
# either since it cannot be called in a transaction.
settings.TIMESERIES_REAL_TIME_AGGREGATES = True


def pytest_configure(config):
    """
    pytest_configure is the canonical way to configure test server for entire testing suite
    """
    pass


@pytest.fixture
def codecov_vcr(request):
    current_path = Path(request.node.fspath)
    current_path_name = current_path.name.replace(".py", "")
    cassette_path = current_path.parent / "cassetes" / current_path_name
    if request.node.cls:
        cls_name = request.node.cls.__name__
        cassette_path = cassette_path / cls_name
    current_name = request.node.name
    cassette_file_path = str(cassette_path / f"{current_name}.yaml")
    with vcr.use_cassette(
        cassette_file_path,
        filter_headers=["authorization"],
        match_on=["method", "scheme", "host", "port", "path"],
    ) as cassette_maker:
        yield cassette_maker


@pytest.fixture
def mock_redis(mocker):
    m = mocker.patch("shared.helpers.redis._get_redis_instance_from_url")
    redis_server = fakeredis.FakeStrictRedis()
    m.return_value = redis_server
    yield redis_server


@pytest.fixture
def mock_storage(mocker):
    m = mocker.patch("shared.storage.get_appropriate_storage_service")
    storage_server = MemoryStorageService({})
    m.return_value = storage_server
    return storage_server


@pytest.fixture(scope="class")
def mock_storage_cls(request):
    with mock.patch("shared.storage.get_appropriate_storage_service") as m:
        storage_server = MemoryStorageService({})
        m.return_value = storage_server
        request.cls.storage = storage_server
        yield


@pytest.fixture(scope="class")
def sample_report(request):
    report = Report()
    first_file = ReportFile("foo/file1.py")
    first_file.append(1, ReportLine.create(1, sessions=[[0, 1]], complexity=(10, 2)))
    first_file.append(2, ReportLine.create(0, sessions=[[0, 1]]))
    first_file.append(3, ReportLine.create(1, sessions=[[0, 1]]))
    first_file.append(5, ReportLine.create(1, sessions=[[0, 1]]))
    first_file.append(6, ReportLine.create(0, sessions=[[0, 1]]))
    first_file.append(8, ReportLine.create(1, sessions=[[0, 1]]))
    first_file.append(9, ReportLine.create(1, sessions=[[0, 1]]))
    first_file.append(10, ReportLine.create(0, sessions=[[0, 1]]))
    second_file = ReportFile("bar/file2.py")
    second_file.append(12, ReportLine.create(1, sessions=[[0, 1]]))
    second_file.append(51, ReportLine.create("1/2", type="b", sessions=[[0, 1]]))
    third_file = ReportFile("file3.py")
    third_file.append(1, ReportLine.create(1, sessions=[[0, 1]]))
    report.append(first_file)
    report.append(second_file)
    report.append(third_file)
    report.add_session(Session(flags=["flag1", "flag2"]))

    request.cls.sample_report = report


@pytest.fixture(scope="session")
def django_db_setup(
    request: pytest.FixtureRequest,
    django_test_environment,
    django_db_blocker,
    django_db_use_migrations,
    django_db_keepdb,
    django_db_createdb,
    django_db_modify_db_settings,
):
    yield from django_setup_test_db(
        request,
        django_test_environment,
        django_db_blocker,
        django_db_use_migrations,
        django_db_keepdb,
        django_db_createdb,
        django_db_modify_db_settings,
    )
