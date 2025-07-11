import logging
import os

import django
from django.core.management import call_command
from django.db import connections

from shared.django_apps.utils.config import get_settings_module

#################################################################
# Keep in sync between apps/codecov-api/migrate_timeseries.py and
# apps/worker/migrate_timeseries.py

# Note: this is the only difference from the worker version
parent_module = "codecov"
#################################################################


# Setup Django environment
os.environ.setdefault("DJANGO_SETTINGS_MODULE", get_settings_module(parent_module))
django.setup()

from django.conf import settings  # noqa: E402

logger = logging.getLogger(__name__)


def run_migrate_commands():
    for app, setting in [
        ("timeseries", settings.TIMESERIES_ENABLED),
        ("ta_timeseries", settings.TA_TIMESERIES_ENABLED),
    ]:
        try:
            if setting and (
                f"shared.django_apps.{app}" in settings.INSTALLED_APPS
                or app in settings.INSTALLED_APPS
            ):
                logger.info(f"Running {app} migrations")
                with connections[app].cursor() as cursor:
                    cursor.execute(
                        "SELECT _timescaledb_internal.stop_background_workers();"
                    )
                call_command(
                    "migrate",
                    database=app,
                    app_label=app,
                    settings=os.environ["DJANGO_SETTINGS_MODULE"],
                    verbosity=1,
                )
                with connections[app].cursor() as cursor:
                    cursor.execute(
                        "SELECT _timescaledb_internal.start_background_workers();"
                    )
            else:
                logger.info(f"Skipping {app} migrations")
        except Exception as e:
            logger.error(f"An error occurred: {e}")


if __name__ == "__main__":
    run_migrate_commands()
