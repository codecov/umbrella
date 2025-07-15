from django.db import migrations

from shared.django_apps.migration_utils import RiskyRunSQL


class Migration(migrations.Migration):
    atomic = False
    dependencies = [
        ("ta_timeseries", "0018_create_repo_summary_caggs"),
    ]

    operations = [
        RiskyRunSQL(
            """
            CALL refresh_continuous_aggregate(
                'ta_timeseries_aggregate_hourly',
                NOW() - INTERVAL '60 days',
                NULL
            );
            """,
            reverse_sql="",
        ),
        RiskyRunSQL(
            """
            CALL refresh_continuous_aggregate(
                'ta_timeseries_aggregate_daily',
                NOW() - INTERVAL '60 days',
                NULL
            );
            """,
            reverse_sql="",
        ),
        RiskyRunSQL(
            """
            CALL refresh_continuous_aggregate(
                'ta_timeseries_branch_aggregate_hourly',
                NOW() - INTERVAL '60 days',
                NOW()
            );
            """,
            reverse_sql="-- No reverse operation needed for refresh",
        ),
        RiskyRunSQL(
            """
            CALL refresh_continuous_aggregate(
                'ta_timeseries_branch_aggregate_daily',
                NOW() - INTERVAL '60 days',
                NOW()
            );
            """,
            reverse_sql="-- No reverse operation needed for refresh",
        ),
    ]
