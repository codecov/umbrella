import dataclasses
import json
from collections.abc import Callable
from decimal import Decimal

import database.events  # noqa: F401
from database.models.timeseries import TimeseriesBaseModel
from shared.config import get_config
from shared.timeseries.helpers import is_timeseries_enabled
from shared.utils.ReportEncoder import ReportEncoder
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, scoped_session, sessionmaker

from .base import Base


def create_all(engine):
    Base.metadata.create_all(engine)


class DatabaseEncoder(ReportEncoder):
    def default(self, obj):
        if dataclasses.is_dataclass(obj):
            return dataclasses.astuple(obj)
        if isinstance(obj, Decimal):
            return str(obj)
        return super().default(obj)


def json_dumps(d):
    return json.dumps(d, cls=DatabaseEncoder)


class SessionFactory:
    def __init__(self, database_url, timeseries_database_url=None):
        self.database_url = _fix_engine(database_url)
        self.timeseries_database_url = _fix_engine(timeseries_database_url)
        self.main_engine = None
        self.timeseries_engine = None

    def create_session(self):
        self.main_engine = create_engine(
            self.database_url,
            json_serializer=json_dumps,
            pool_pre_ping=True,
        )

        if is_timeseries_enabled():
            self.timeseries_engine = create_engine(
                self.timeseries_database_url,
                json_serializer=json_dumps,
                pool_pre_ping=True,
            )

            main_engine = self.main_engine
            timeseries_engine = self.timeseries_engine

            class RoutingSession(Session):
                def get_bind(self, mapper=None, clause=None, **kwargs):
                    if mapper is not None and issubclass(
                        mapper.class_, TimeseriesBaseModel
                    ):
                        return timeseries_engine
                    if (
                        clause is not None
                        and hasattr(clause, "table")
                        and clause.table.name.startswith("timeseries_")
                    ):
                        return timeseries_engine
                    return main_engine

            session_factory = sessionmaker(class_=RoutingSession)
        else:
            session_factory = sessionmaker(bind=self.main_engine)

        return scoped_session(session_factory)


def _fix_engine(database_url: str) -> str:
    return database_url.replace("postgres://", "postgresql://")


session_factory = SessionFactory(
    database_url=get_config(
        "services",
        "database_url",
        default="postgresql://postgres:@postgres:5432/postgres",
    ),
    timeseries_database_url=get_config(
        "services",
        "timeseries_database_url",
        default="postgresql://postgres:@timescale:5432/postgres",
    ),
)

session = session_factory.create_session()

get_db_session = session


class TaskSessionManager:
    """Manages per-task database sessions with support for test overrides."""

    def __init__(self):
        self._session_maker = None
        self._test_session_factory: Callable[[], Session] | None = None

    def set_test_session_factory(self, factory: Callable[[], Session] | None):
        """Override create_task_session() to return a shared test session."""
        self._test_session_factory = factory

    def _get_session_maker(self):
        if self._session_maker is None:
            if session_factory.main_engine is None:
                _ = session_factory.create_session()

            main_engine = session_factory.main_engine
            timeseries_engine = session_factory.timeseries_engine

            if main_engine is None:
                raise RuntimeError(
                    "Cannot create task session: database engine not initialized"
                )

            if is_timeseries_enabled() and timeseries_engine is not None:
                main_engine_ref = main_engine
                timeseries_engine_ref = timeseries_engine

                class RoutingSession(Session):
                    def get_bind(self, mapper=None, clause=None, **kwargs):
                        if mapper is not None and issubclass(
                            mapper.class_, TimeseriesBaseModel
                        ):
                            return timeseries_engine_ref
                        if (
                            clause is not None
                            and hasattr(clause, "table")
                            and clause.table.name.startswith("timeseries_")
                        ):
                            return timeseries_engine_ref
                        return main_engine_ref

                self._session_maker = sessionmaker(class_=RoutingSession)
            else:
                self._session_maker = sessionmaker(bind=main_engine)

        return self._session_maker

    def create_task_session(self) -> Session:
        """Create a new isolated session for a task. Caller must clean up (rollback/close)."""
        if self._test_session_factory is not None:
            return self._test_session_factory()

        session_maker = self._get_session_maker()
        return session_maker()


_task_session_manager = TaskSessionManager()


def set_test_session_factory(factory: Callable[[], Session] | None):
    """Override create_task_session() to return a shared test session."""
    _task_session_manager.set_test_session_factory(factory)


def create_task_session() -> Session:
    """Create a new isolated session for a task. Caller must clean up (rollback/close)."""
    return _task_session_manager.create_task_session()
