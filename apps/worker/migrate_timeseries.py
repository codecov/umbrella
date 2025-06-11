import logging
import os

import django
from django.core.management import call_command
from django.db import connections

from shared.django_apps.utils.config import get_settings_module

# Setup Django environment
os.environ.setdefault("DJANGO_SETTINGS_MODULE", get_settings_module("django_scaffold"))
django.setup()

from django.conf import settings  # noqa: E402

logger = logging.getLogger(__name__)


def run_migrate_commands():
    try:
        if settings.TA_TIMESERIES_ENABLED:
            logger.info("Running ta_timeseries migrations")
            with connections["ta_timeseries"].cursor() as cursor:
                cursor.execute(
                    "SELECT _timescaledb_internal.stop_background_workers();"
                )
            call_command(
                "migrate",
                database="ta_timeseries",
                app_label="ta_timeseries",
                settings=get_settings_module("django_scaffold"),
                verbosity=1,
            )
            with connections["ta_timeseries"].cursor() as cursor:
                cursor.execute(
                    "SELECT _timescaledb_internal.start_background_workers();"
                )
        else:
            logger.info("Skipping ta_timeseries migrations")

    except Exception as e:
        logger.error(f"An error occurred: {e}")


if __name__ == "__main__":
    run_migrate_commands()
