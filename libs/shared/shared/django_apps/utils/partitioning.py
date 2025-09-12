from dateutil.relativedelta import relativedelta
from psqlextra.partitioning import (
    PostgresCurrentTimePartitioningStrategy,
    PostgresPartitioningManager,
    PostgresTimePartitionSize,
)
from psqlextra.partitioning.config import PostgresPartitioningConfig

from shared.config import get_config
from shared.django_apps.upload_breadcrumbs.models import UploadBreadcrumb
from shared.django_apps.user_measurements.models import UserMeasurement

upload_breadcrumbs_retention = get_config(
    "setup", "upload_breadcrumbs", "retention", default={}
)
# Set unlimited to true to keep partitions forever (overrides all other options)
ub_unlimited = upload_breadcrumbs_retention.get("unlimited", False)
# Options to set retention period by years, months, and/or days (3 months by default)
# If multiple options are set, the length will be the addition of all specified periods
ub_years = upload_breadcrumbs_retention.get("years", 0)
ub_months = upload_breadcrumbs_retention.get("months", 3)
ub_days = upload_breadcrumbs_retention.get("days", 0)


# Overlapping partitions will cause errors - https://www.postgresql.org/docs/current/ddl-partitioning.html#DDL-PARTITIONING-DECLARATIVE -> "create partitions"
# Note that the partitioning manager will not perform the actual creation and deletion of partitions automatically, we have a Celery task for that.
manager = PostgresPartitioningManager(
    [
        # 12 partitions ahead, each partition is 1 month
        # Delete partitions older than 3 months
        # Partitions will be named `[table_name]_[year]_[3-letter month name]`
        PostgresPartitioningConfig(
            model=UserMeasurement,
            strategy=PostgresCurrentTimePartitioningStrategy(
                size=PostgresTimePartitionSize(months=1),
                count=12,
                max_age=relativedelta(months=12),
            ),
        ),
        # 3 partitions ahead, each partition is one 1 week
        # Delete partitions older than a specified retention period
        # Partitions will be named `[table_name]_[year]_week_[week number]`
        PostgresPartitioningConfig(
            model=UploadBreadcrumb,
            strategy=PostgresCurrentTimePartitioningStrategy(
                size=PostgresTimePartitionSize(weeks=1),
                count=3,
                max_age=None
                if ub_unlimited
                else relativedelta(
                    years=ub_years,
                    months=ub_months,
                    days=ub_days,
                ),
            ),
        ),
    ]
)
