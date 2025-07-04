from pathlib import Path
from unittest import mock

import fakeredis
import pytest
import vcr
from django.conf import settings
from django.db import connections
from django.test.utils import setup_databases, teardown_databases
from pytest_django.fixtures import (
    _disable_migrations,
    _get_databases_for_setup,
)

from shared.reports.resources import Report, ReportFile
from shared.reports.types import ReportLine
from shared.storage.memory import MemoryStorageService
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
    django_test_environment: None,
    django_db_blocker,
    django_db_use_migrations: bool,
    django_db_keepdb: bool,
    django_db_createdb: bool,
    django_db_modify_db_settings: None,
):
    """Top level fixture to ensure test databases are available"""

    setup_databases_args = {}

    if not django_db_use_migrations:
        _disable_migrations()

    if django_db_keepdb and not django_db_createdb:
        setup_databases_args["keepdb"] = True

    aliases, serialized_aliases = _get_databases_for_setup(request.session.items)

    with django_db_blocker.unblock():
        for connection in connections:
            if "timeseries" in connection:
                with connections[connection].cursor() as cursor:
                    cursor.execute(
                        "SELECT _timescaledb_internal.stop_background_workers();"
                    )

        db_cfg = setup_databases(
            verbosity=request.config.option.verbose,
            interactive=False,
            aliases=aliases,
            serialized_aliases=serialized_aliases,
            **setup_databases_args,
        )

        for connection in connections:
            if "timeseries" in connection:
                with connections[connection].cursor() as cursor:
                    cursor.execute(
                        "SELECT _timescaledb_internal.start_background_workers();"
                    )

    yield

    if not django_db_keepdb:
        with django_db_blocker.unblock():
            try:
                teardown_databases(db_cfg, verbosity=request.config.option.verbose)
            except Exception as exc:  # noqa: BLE001
                request.node.warn(
                    pytest.PytestWarning(
                        f"Error when trying to teardown test databases: {exc!r}"
                    )
                )
