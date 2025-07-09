import pytest
from django.conf import settings
from django.core.management import call_command
from django.db import connections
from django.test.utils import setup_databases, teardown_databases
from pytest_django.fixtures import (
    _disable_migrations,
    _get_databases_for_setup,
)


def manual_migration(connection_name: str, verbosity: int):
    old_migration_modules = settings.MIGRATION_MODULES.get(connection_name)
    settings.MIGRATION_MODULES[connection_name] = None

    connections[connection_name].creation.create_test_db(
        verbosity=verbosity,
        autoclobber=True,
        keepdb=False,
    )

    with connections[connection_name].cursor() as cursor:
        cursor.execute("SELECT _timescaledb_internal.stop_background_workers();")

    if old_migration_modules:
        settings.MIGRATION_MODULES[connection_name] = old_migration_modules
    else:
        del settings.MIGRATION_MODULES[connection_name]

    call_command("migrate", database=connection_name, app_label=connection_name)

    with connections[connection_name].cursor() as cursor:
        cursor.execute("SELECT _timescaledb_internal.start_background_workers();")


def django_setup_test_db(
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
    manually_migrated_aliases = []

    with django_db_blocker.unblock():
        # we need to manually migrate the timeseries database because we
        # need to stop the background workers after having created the test database
        # but before running the migrations.
        # otherwise the migrations may run at the same time as the background workers
        # and cause a deadlock.
        for name in ["ta_timeseries", "timeseries"]:
            if name in aliases:
                aliases.remove(name)

                manual_migration(name, request.config.option.verbose)
                manually_migrated_aliases.append(name)

        db_cfg = setup_databases(
            verbosity=request.config.option.verbose,
            interactive=False,
            aliases=aliases,
            serialized_aliases=serialized_aliases,
            **setup_databases_args,
        )

    yield

    if not django_db_keepdb:
        with django_db_blocker.unblock():
            for name in manually_migrated_aliases:
                connections[name].creation.destroy_test_db(
                    keepdb=False,
                )

            try:
                teardown_databases(db_cfg, verbosity=request.config.option.verbose)
            except Exception as exc:  # noqa: BLE001
                request.node.warn(
                    pytest.PytestWarning(
                        f"Error when trying to teardown test databases: {exc!r}"
                    )
                )
