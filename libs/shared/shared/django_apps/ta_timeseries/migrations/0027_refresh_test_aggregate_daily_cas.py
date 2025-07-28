from django.db import migrations

from shared.django_apps.migration_utils import RiskyRunSQL


class Migration(migrations.Migration):
    atomic = False
    dependencies = [
        ("ta_timeseries", "0026_create_test_aggregate_indexes"),
    ]

    operations = [
        RiskyRunSQL(
            """
            CALL refresh_continuous_aggregate(
                'ta_timeseries_test_aggregate_daily',
                NOW() - INTERVAL '60 days',
                NULL
            );
            """,
        ),
        RiskyRunSQL(
            """
            CALL refresh_continuous_aggregate(
                'ta_timeseries_branch_test_aggregate_daily',
                NOW() - INTERVAL '60 days',
                NULL
            );
            """,
        ),
    ]
