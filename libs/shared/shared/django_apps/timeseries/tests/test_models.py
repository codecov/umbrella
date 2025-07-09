from datetime import UTC, datetime

import pytest
from django.db import connections
from freezegun import freeze_time

from shared.django_apps.timeseries.models import Dataset, Interval, MeasurementSummary

from .factories import DatasetFactory, MeasurementFactory

pytestmark = pytest.mark.django_db(
    databases=[
        "timeseries",
        "default",
    ],
    transaction=True,
)


def test_measurement_agg_1day():
    MeasurementFactory(timestamp=datetime(2022, 1, 1, 0, 0, 0, tzinfo=UTC), value=1)
    MeasurementFactory(timestamp=datetime(2022, 1, 1, 1, 0, 0, tzinfo=UTC), value=2)
    MeasurementFactory(timestamp=datetime(2022, 1, 1, 1, 0, 1, tzinfo=UTC), value=3)
    MeasurementFactory(timestamp=datetime(2022, 1, 2, 0, 0, 0, tzinfo=UTC), value=4)
    MeasurementFactory(timestamp=datetime(2022, 1, 2, 0, 1, 0, tzinfo=UTC), value=5)

    with connections["timeseries"].cursor() as cursor:
        cursor.execute(
            "CALL refresh_continuous_aggregate('timeseries_measurement_summary_1day', '2022-01-01T00:00:00', '2022-02-01T00:00:00')"
        )

    results = MeasurementSummary.agg_by(Interval.INTERVAL_1_DAY).all()

    assert len(results) == 2
    assert results[0].value_avg == 2
    assert results[0].value_min == 1
    assert results[0].value_max == 3
    assert results[0].value_count == 3
    assert results[1].value_avg == 4.5
    assert results[1].value_min == 4
    assert results[1].value_max == 5
    assert results[1].value_count == 2


def test_measurement_agg_7day():
    # Week 1: Monday, Tuesday, Sunday
    MeasurementFactory(timestamp=datetime(2022, 1, 3, tzinfo=UTC), value=1)
    MeasurementFactory(timestamp=datetime(2022, 1, 4, tzinfo=UTC), value=2)
    MeasurementFactory(timestamp=datetime(2022, 1, 9, tzinfo=UTC), value=3)

    # Week 2: Monday, Sunday
    MeasurementFactory(timestamp=datetime(2022, 1, 10, tzinfo=UTC), value=4)
    MeasurementFactory(timestamp=datetime(2022, 1, 16, tzinfo=UTC), value=5)

    with connections["timeseries"].cursor() as cursor:
        cursor.execute(
            "CALL refresh_continuous_aggregate('timeseries_measurement_summary_7day', '2022-01-01T00:00:00', '2022-02-01T00:00:00')"
        )

    results = MeasurementSummary.agg_by(Interval.INTERVAL_7_DAY).all()

    assert len(results) == 2
    assert results[0].value_avg == 2
    assert results[0].value_min == 1
    assert results[0].value_max == 3
    assert results[0].value_count == 3
    assert results[1].value_avg == 4.5
    assert results[1].value_min == 4
    assert results[1].value_max == 5
    assert results[1].value_count == 2


def test_measurement_agg_30day():
    # Timescale's origin for time buckets is 2000-01-03
    # 30 day offsets will be aligned on that origin

    MeasurementFactory(timestamp=datetime(2000, 1, 3, tzinfo=UTC), value=1)
    MeasurementFactory(timestamp=datetime(2000, 1, 4, tzinfo=UTC), value=2)
    MeasurementFactory(timestamp=datetime(2000, 2, 1, tzinfo=UTC), value=3)

    MeasurementFactory(timestamp=datetime(2000, 2, 2, tzinfo=UTC), value=4)
    MeasurementFactory(timestamp=datetime(2000, 2, 11, tzinfo=UTC), value=5)

    with connections["timeseries"].cursor() as cursor:
        cursor.execute(
            "CALL refresh_continuous_aggregate('timeseries_measurement_summary_30day', '2000-01-01T00:00:00', '2000-04-01T00:00:00')"
        )

    results = MeasurementSummary.agg_by(Interval.INTERVAL_30_DAY).all()

    assert len(results) == 2
    assert results[0].value_avg == 2
    assert results[0].value_min == 1
    assert results[0].value_max == 3
    assert results[0].value_count == 3
    assert results[1].value_avg == 4.5
    assert results[1].value_min == 4
    assert results[1].value_max == 5
    assert results[1].value_count == 2


def test_measurement_agg_invalid():
    with pytest.raises(ValueError):
        MeasurementSummary.agg_by("invalid").all()


@freeze_time("2022-01-01T01:00:01+0000")
def test_is_backfilled_true():
    dataset = DatasetFactory()

    Dataset.objects.filter(pk=dataset.pk).update(
        created_at=datetime(2022, 1, 1, 0, 0, 0, tzinfo=UTC),
    )

    dataset.refresh_from_db()
    assert dataset.is_backfilled() == True


@freeze_time("2022-01-01T00:59:59+0000")
def test_is_backfilled_false():
    dataset = DatasetFactory()

    Dataset.objects.filter(pk=dataset.pk).update(
        created_at=datetime(2022, 1, 1, 0, 0, 0, tzinfo=UTC),
    )

    dataset.refresh_from_db()
    assert dataset.is_backfilled() == False


def test_is_backfilled_no_created_at():
    dataset = DatasetFactory()

    Dataset.objects.filter(pk=dataset.pk).update(created_at=None)

    dataset.refresh_from_db()
    assert dataset.is_backfilled() == False
