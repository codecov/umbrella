from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from psqlextra.partitioning import (
    PostgresCurrentTimePartitioningStrategy,
    PostgresPartitioningManager,
    PostgresTimePartitionSize,
)
from psqlextra.partitioning.config import PostgresPartitioningConfig

from shared.django_apps.reports.models import DailyTestRollup
from shared.django_apps.upload_breadcrumbs.models import UploadBreadcrumb
from shared.django_apps.user_measurements.models import UserMeasurement

class PostgresTimeUnitNextPartitioningStrategy(PostgresCurrentTimePartitioningStrategy):
    def get_start_datetime(self) -> datetime:
        """ Add 1 of PostgresTimePartitionUnit"""
        unit_plus_one = {self.size.unit: 1}
        return datetime.now(timezone.utc) + relativedelta(**unit_plus_one)

# Overlapping partitions will cause errors - https://www.postgresql.org/docs/current/ddl-partitioning.html#DDL-PARTITIONING-DECLARATIVE -> "create partitions"
# Note that the partitioning manager will not perform the actual creation and deletion of partitions automatically, we have a Celery task for that.
manager = PostgresPartitioningManager(
    [
        # 12 partitions ahead, each partition is 1 month
        # Delete partitions older than 3 months
        # Partitions will be named `[table_name]_[year]_[3-letter month name]`
        PostgresPartitioningConfig(
            model=UserMeasurement,
            strategy=PostgresTimeUnitNextPartitioningStrategy(
                size=PostgresTimePartitionSize(months=1),
                count=12,
                max_age=relativedelta(months=12),
            ),
        ),
        # 3 partitions ahead, each partition is one month
        # Delete partitions older than 3 months
        # Partitions will be named `[table_name]_[year]_[3-letter month name]`
        PostgresPartitioningConfig(
            model=DailyTestRollup,
            strategy=PostgresTimeUnitNextPartitioningStrategy(
                size=PostgresTimePartitionSize(months=1),
                count=3,
                max_age=relativedelta(months=3),
            ),
        ),
        # 3 partitions ahead, each partition is one 1 week
        # Delete partitions older than 3 months
        # Partitions will be named `[table_name]_[year]_week_[week number]`
        PostgresPartitioningConfig(
            model=UploadBreadcrumb,
            strategy=PostgresTimeUnitNextPartitioningStrategy(
                size=PostgresTimePartitionSize(weeks=1),
                count=3,
                max_age=relativedelta(months=3),
            ),
        ),
    ]
)
