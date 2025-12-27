import dataclasses
from decimal import Decimal
from unittest.mock import Mock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from sqlalchemy_utils import get_mapper

from database.base import Base
from database.engine import (
    DatabaseEncoder,
    SessionFactory,
    TaskSessionManager,
    _fix_engine,
    create_all,
    create_task_session,
    json_dumps,
    set_test_session_factory,
)
from database.models import Commit
from database.models.timeseries import Measurement


class TestFixEngine:
    def test_fix_engine_converts_postgres_to_postgresql(self):
        """Test that postgres:// URLs are converted to postgresql://"""
        assert (
            _fix_engine("postgres://user:pass@host:5432/db")
            == "postgresql://user:pass@host:5432/db"
        )

    def test_fix_engine_leaves_postgresql_unchanged(self):
        """Test that postgresql:// URLs are left unchanged"""
        url = "postgresql://user:pass@host:5432/db"
        assert _fix_engine(url) == url

    def test_fix_engine_handles_multiple_occurrences(self):
        """Test that all occurrences are replaced (str.replace replaces all)"""
        # This shouldn't happen in practice, but test the behavior
        # Note: str.replace() replaces ALL occurrences, not just the first
        url = "postgres://user:pass@host:5432/postgres://db"
        result = _fix_engine(url)
        assert result == "postgresql://user:pass@host:5432/postgresql://db"
        assert "postgres://" not in result


class TestDatabaseEncoder:
    def test_database_encoder_dataclass(self):
        """Test that dataclasses are serialized using astuple"""

        @dataclasses.dataclass
        class TestDataClass:
            x: int
            y: str

        obj = TestDataClass(x=1, y="test")
        encoder = DatabaseEncoder()
        result = encoder.default(obj)
        assert result == (1, "test")

    def test_database_encoder_decimal(self):
        """Test that Decimal objects are converted to strings"""
        encoder = DatabaseEncoder()
        result = encoder.default(Decimal("123.45"))
        assert result == "123.45"

    def test_database_encoder_fallback_to_parent(self):
        """Test that other types fall back to parent class"""
        encoder = DatabaseEncoder()
        # Should raise TypeError for unsupported types (parent behavior)
        with pytest.raises(TypeError):
            encoder.default(object())


class TestJsonDumps:
    def test_json_dumps_with_dataclass(self):
        """Test json_dumps with a dataclass"""

        @dataclasses.dataclass
        class TestDataClass:
            x: int
            y: str

        obj = TestDataClass(x=1, y="test")
        data = {"key": obj}
        result = json_dumps(data)
        assert '"key": [1, "test"]' in result

    def test_json_dumps_with_decimal(self):
        """Test json_dumps with a Decimal"""
        data = {"value": Decimal("123.45")}
        result = json_dumps(data)
        assert '"value": "123.45"' in result

    def test_json_dumps_with_regular_types(self):
        """Test json_dumps with regular JSON-serializable types"""
        data = {"string": "test", "number": 42, "bool": True}
        result = json_dumps(data)
        assert '"string": "test"' in result
        assert '"number": 42' in result
        assert '"bool": true' in result


class TestCreateAll:
    def test_create_all_calls_base_metadata_create_all(self, mocker):
        """Test that create_all calls Base.metadata.create_all"""
        mock_engine = Mock()
        mock_create_all = mocker.patch.object(Base.metadata, "create_all")

        create_all(mock_engine)

        mock_create_all.assert_called_once_with(mock_engine)


class TestSessionFactory:
    def test_session_factory_init_calls_fix_engine(
        self, mocker, sqlalchemy_connect_url
    ):
        """Test that __init__ calls _fix_engine on both URLs"""
        mock_fix_engine = mocker.patch(
            "database.engine._fix_engine", side_effect=lambda x: x
        )

        SessionFactory(
            database_url=sqlalchemy_connect_url,
            timeseries_database_url=sqlalchemy_connect_url,
        )

        assert mock_fix_engine.call_count == 2

    def test_session_factory_init_with_none_timeseries_url(
        self, sqlalchemy_connect_url
    ):
        """Test SessionFactory initialization with None timeseries_database_url raises AttributeError"""
        # _fix_engine(None) will raise AttributeError since None doesn't have replace method
        with pytest.raises(AttributeError):
            SessionFactory(
                database_url=sqlalchemy_connect_url,
                timeseries_database_url=None,
            )

    def test_session_factory_create_session_sets_json_serializer(
        self, sqlalchemy_connect_url, mocker
    ):
        """Test that create_session sets json_serializer correctly"""
        mock_create_engine = mocker.patch("database.engine.create_engine")
        mocker.patch("database.engine.is_timeseries_enabled", return_value=False)
        mocker.patch("database.engine.scoped_session")
        # Mock _fix_engine to handle None (though it won't be used when timeseries is disabled)
        mocker.patch(
            "database.engine._fix_engine",
            side_effect=lambda x: x if x is not None else None,
        )

        factory = SessionFactory(
            database_url=sqlalchemy_connect_url,
            timeseries_database_url=sqlalchemy_connect_url,  # Use valid URL instead of None
        )
        factory.create_session()

        # Check that create_engine was called with json_serializer
        call_args = mock_create_engine.call_args
        assert "json_serializer" in call_args.kwargs
        assert call_args.kwargs["json_serializer"] == json_dumps
        assert call_args.kwargs["pool_pre_ping"] is True

    def test_session_factory_create_session_timeseries_url_none(
        self, sqlalchemy_connect_url, mocker
    ):
        """Test create_session when timeseries_database_url is None and timeseries is enabled"""
        mocker.patch("database.engine.is_timeseries_enabled", return_value=True)
        # Mock _fix_engine to return None for None input (simulating the error case)
        mocker.patch(
            "database.engine._fix_engine",
            side_effect=lambda x: x if x is not None else None,
        )

        factory = SessionFactory(
            database_url=sqlalchemy_connect_url,
            timeseries_database_url=None,
        )
        # create_engine will fail with None URL - SQLAlchemy raises AttributeError
        # when trying to instantiate plugins with None
        with pytest.raises(
            (TypeError, ValueError, AttributeError)
        ):  # create_engine will fail with None URL
            factory.create_session()

    def test_session_get_bind_timeseries_disabled(self, sqlalchemy_connect_url, mocker):
        mocker.patch("database.engine.is_timeseries_enabled", return_value=False)

        session_factory = SessionFactory(
            database_url=sqlalchemy_connect_url,
            timeseries_database_url=sqlalchemy_connect_url,
        )
        session = session_factory.create_session()
        assert session_factory.main_engine is not None
        assert session_factory.timeseries_engine is None

        engine = session.get_bind(mapper=get_mapper(Commit))
        assert engine == session_factory.main_engine

        clause = insert(Commit.__table__)
        engine = session.get_bind(clause=clause)
        assert engine == session_factory.main_engine

        engine = session.get_bind(mapper=get_mapper(Measurement))
        assert engine == session_factory.main_engine

        clause = insert(Measurement.__table__)
        engine = session.get_bind(clause=clause)
        assert engine == session_factory.main_engine

    def test_session_get_bind_timeseries_enabled(self, sqlalchemy_connect_url, mocker):
        mocker.patch("database.engine.is_timeseries_enabled", return_value=True)

        session_factory = SessionFactory(
            database_url=sqlalchemy_connect_url,
            timeseries_database_url=sqlalchemy_connect_url,
        )
        session = session_factory.create_session()
        assert session_factory.main_engine is not None
        assert session_factory.timeseries_engine is not None

        engine = session.get_bind(mapper=get_mapper(Commit))
        assert engine == session_factory.main_engine

        clause = insert(Commit.__table__)
        engine = session.get_bind(clause=clause)
        assert engine == session_factory.main_engine

        engine = session.get_bind(mapper=get_mapper(Measurement))
        assert engine == session_factory.timeseries_engine

        clause = insert(Measurement.__table__)
        engine = session.get_bind(clause=clause)
        assert engine == session_factory.timeseries_engine

    def test_routing_session_get_bind_no_mapper_no_clause(
        self, sqlalchemy_connect_url, mocker
    ):
        """Test RoutingSession.get_bind when both mapper and clause are None"""
        mocker.patch("database.engine.is_timeseries_enabled", return_value=True)

        session_factory = SessionFactory(
            database_url=sqlalchemy_connect_url,
            timeseries_database_url=sqlalchemy_connect_url,
        )
        session = session_factory.create_session()

        # When both are None, should return main_engine
        engine = session.get_bind()
        assert engine == session_factory.main_engine

    def test_routing_session_get_bind_clause_without_table_attribute(
        self, sqlalchemy_connect_url, mocker
    ):
        """Test RoutingSession.get_bind when clause doesn't have table attribute"""
        mocker.patch("database.engine.is_timeseries_enabled", return_value=True)

        session_factory = SessionFactory(
            database_url=sqlalchemy_connect_url,
            timeseries_database_url=sqlalchemy_connect_url,
        )
        session = session_factory.create_session()

        # Create a mock clause without table attribute
        mock_clause = Mock()
        del mock_clause.table  # Ensure it doesn't have table attribute

        engine = session.get_bind(clause=mock_clause)
        assert engine == session_factory.main_engine

    def test_routing_session_get_bind_clause_table_not_timeseries(
        self, sqlalchemy_connect_url, mocker
    ):
        """Test RoutingSession.get_bind when clause.table.name doesn't start with timeseries_"""
        mocker.patch("database.engine.is_timeseries_enabled", return_value=True)

        session_factory = SessionFactory(
            database_url=sqlalchemy_connect_url,
            timeseries_database_url=sqlalchemy_connect_url,
        )
        session = session_factory.create_session()

        # Create a mock clause with table that doesn't start with timeseries_
        mock_clause = Mock()
        mock_table = Mock()
        mock_table.name = "regular_table"
        mock_clause.table = mock_table

        engine = session.get_bind(clause=mock_clause)
        assert engine == session_factory.main_engine

    def test_routing_session_get_bind_mapper_not_timeseries_subclass(
        self, sqlalchemy_connect_url, mocker
    ):
        """Test RoutingSession.get_bind when mapper is not a TimeseriesBaseModel subclass"""
        mocker.patch("database.engine.is_timeseries_enabled", return_value=True)

        session_factory = SessionFactory(
            database_url=sqlalchemy_connect_url,
            timeseries_database_url=sqlalchemy_connect_url,
        )
        session = session_factory.create_session()

        # Commit is not a TimeseriesBaseModel subclass
        engine = session.get_bind(mapper=get_mapper(Commit))
        assert engine == session_factory.main_engine


class TestTaskSessionManager:
    def test_task_session_manager_init(self):
        """Test TaskSessionManager initialization"""
        manager = TaskSessionManager()
        assert manager._session_maker is None
        assert manager._test_session_factory is None

    def test_set_test_session_factory(self):
        """Test setting a test session factory"""
        manager = TaskSessionManager()
        mock_factory = Mock()

        manager.set_test_session_factory(mock_factory)

        assert manager._test_session_factory == mock_factory

    def test_set_test_session_factory_none(self):
        """Test clearing test session factory by setting to None"""
        manager = TaskSessionManager()
        manager.set_test_session_factory(Mock())

        manager.set_test_session_factory(None)

        assert manager._test_session_factory is None

    def test_create_task_session_with_test_factory(self):
        """Test create_task_session uses test factory when set"""
        manager = TaskSessionManager()
        mock_session = Mock()
        mock_factory = Mock(return_value=mock_session)

        manager.set_test_session_factory(mock_factory)
        result = manager.create_task_session()

        assert result == mock_session
        mock_factory.assert_called_once()

    def test_get_session_maker_creates_session_if_needed(
        self, sqlalchemy_connect_url, mocker
    ):
        """Test _get_session_maker creates session if main_engine is None"""
        mocker.patch("database.engine.is_timeseries_enabled", return_value=False)
        # Mock the global session_factory
        with patch("database.engine.session_factory") as mock_session_factory:
            mock_session_factory.main_engine = None
            mock_session_factory.timeseries_engine = None
            mock_session_factory.create_session = Mock()
            mock_engine = create_engine(sqlalchemy_connect_url)
            mock_session_factory.create_session.return_value = None

            # After create_session is called, set the engines
            def set_engines():
                mock_session_factory.main_engine = mock_engine

            mock_session_factory.create_session.side_effect = set_engines

            manager = TaskSessionManager()
            session_maker = manager._get_session_maker()

            assert session_maker is not None
            mock_session_factory.create_session.assert_called_once()

    def test_get_session_maker_raises_when_engine_none_after_create(
        self, sqlalchemy_connect_url, mocker
    ):
        """Test _get_session_maker raises RuntimeError when main_engine is None after create_session"""
        mocker.patch("database.engine.is_timeseries_enabled", return_value=False)
        with patch("database.engine.session_factory") as mock_session_factory:
            mock_session_factory.main_engine = None
            mock_session_factory.timeseries_engine = None
            mock_session_factory.create_session = Mock()
            # Even after create_session, main_engine remains None
            mock_session_factory.create_session.return_value = None

            manager = TaskSessionManager()
            with pytest.raises(
                RuntimeError,
                match="Cannot create task session: database engine not initialized",
            ):
                manager._get_session_maker()

    def test_get_session_maker_with_timeseries_enabled(
        self, sqlalchemy_connect_url, mocker
    ):
        """Test _get_session_maker when timeseries is enabled"""
        mocker.patch("database.engine.is_timeseries_enabled", return_value=True)
        with patch("database.engine.session_factory") as mock_session_factory:
            main_engine = create_engine(sqlalchemy_connect_url)
            timeseries_engine = create_engine(sqlalchemy_connect_url)
            mock_session_factory.main_engine = main_engine
            mock_session_factory.timeseries_engine = timeseries_engine

            manager = TaskSessionManager()
            session_maker = manager._get_session_maker()

            assert session_maker is not None
            # Create a session and verify it's a RoutingSession
            session = session_maker()
            assert isinstance(session, Session)

    def test_get_session_maker_with_timeseries_disabled(
        self, sqlalchemy_connect_url, mocker
    ):
        """Test _get_session_maker when timeseries is disabled"""
        mocker.patch("database.engine.is_timeseries_enabled", return_value=False)
        with patch("database.engine.session_factory") as mock_session_factory:
            main_engine = create_engine(sqlalchemy_connect_url)
            mock_session_factory.main_engine = main_engine
            mock_session_factory.timeseries_engine = None

            manager = TaskSessionManager()
            session_maker = manager._get_session_maker()

            assert session_maker is not None
            session = session_maker()
            assert isinstance(session, Session)

    def test_get_session_maker_caches_result(self, sqlalchemy_connect_url, mocker):
        """Test that _get_session_maker caches the session maker"""
        mocker.patch("database.engine.is_timeseries_enabled", return_value=False)
        with patch("database.engine.session_factory") as mock_session_factory:
            main_engine = create_engine(sqlalchemy_connect_url)
            mock_session_factory.main_engine = main_engine
            mock_session_factory.timeseries_engine = None

            manager = TaskSessionManager()
            session_maker1 = manager._get_session_maker()
            session_maker2 = manager._get_session_maker()

            assert session_maker1 is session_maker2

    def test_create_task_session_without_test_factory(
        self, sqlalchemy_connect_url, mocker
    ):
        """Test create_task_session creates session via session maker when no test factory"""
        mocker.patch("database.engine.is_timeseries_enabled", return_value=False)
        with patch("database.engine.session_factory") as mock_session_factory:
            main_engine = create_engine(sqlalchemy_connect_url)
            mock_session_factory.main_engine = main_engine
            mock_session_factory.timeseries_engine = None

            manager = TaskSessionManager()
            session = manager.create_task_session()

            assert isinstance(session, Session)


class TestModuleLevelFunctions:
    def test_set_test_session_factory_module_level(self):
        """Test the module-level set_test_session_factory function"""
        mock_factory = Mock()
        set_test_session_factory(mock_factory)

        # Verify it was set by checking create_task_session uses it
        mock_session = Mock()
        mock_factory.return_value = mock_session
        result = create_task_session()

        assert result == mock_session

        # Clean up
        set_test_session_factory(None)

    def test_create_task_session_module_level(self, sqlalchemy_connect_url, mocker):
        """Test the module-level create_task_session function"""
        mocker.patch("database.engine.is_timeseries_enabled", return_value=False)
        with patch("database.engine.session_factory") as mock_session_factory:
            main_engine = create_engine(sqlalchemy_connect_url)
            mock_session_factory.main_engine = main_engine
            mock_session_factory.timeseries_engine = None

            session = create_task_session()

            assert isinstance(session, Session)
