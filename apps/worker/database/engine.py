import dataclasses
import json
from collections.abc import Callable
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, scoped_session, sessionmaker

import database.events  # noqa: F401
from database.models.timeseries import TimeseriesBaseModel
from shared.config import get_config
from shared.timeseries.helpers import is_timeseries_enabled
from shared.utils.ReportEncoder import ReportEncoder

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


# Per-task session support
# Cache for sessionmaker to avoid recreating it
_task_session_maker = None

# Thread-local storage for test session override
# In tests, this can be set to return a shared test session
_test_session_factory: Callable[[], Session] | None = None


def set_test_session_factory(factory: Callable[[], Session] | None):
    """
    Set a factory function that returns a test session.

    This allows tests to override create_task_session() to return a shared test session
    instead of creating new sessions. Set to None to use the default behavior.

    Args:
        factory: Callable that returns a Session, or None to use default behavior
    """
    global _test_session_factory  # noqa: PLW0603
    _test_session_factory = factory


def _get_task_session_maker():
    """Get or create the sessionmaker for per-task sessions."""
    global _task_session_maker  # noqa: PLW0603

    if _task_session_maker is None:
        # Initialize engines if not already done
        if session_factory.main_engine is None:
            # Trigger engine creation
            _ = session_factory.create_session()

        main_engine = session_factory.main_engine
        timeseries_engine = session_factory.timeseries_engine

        if main_engine is None:
            raise RuntimeError(
                "Cannot create task session: database engine not initialized"
            )

        # Create sessionmaker for per-task sessions
        if is_timeseries_enabled() and timeseries_engine is not None:

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

            _task_session_maker = sessionmaker(class_=RoutingSession)
        else:
            _task_session_maker = sessionmaker(bind=main_engine)

    return _task_session_maker


def create_task_session():
    """
    Create a new session for a task.

    This creates an isolated session per task, preventing transaction contamination.
    In tests, this can be overridden to return a shared test session via set_test_session_factory().

    The caller is responsible for cleaning up the session (rollback/close).

    Returns:
        Session: A new SQLAlchemy session instance

    Example:
        db_session = create_task_session()
        try:
            # Use db_session
            result = db_session.query(...).first()
            db_session.commit()
        finally:
            db_session.rollback()
            db_session.close()
    """
    # Check if test session factory is set (for testing)
    if _test_session_factory is not None:
        return _test_session_factory()

    # Default behavior: create a new isolated session
    session_maker = _get_task_session_maker()
    return session_maker()
