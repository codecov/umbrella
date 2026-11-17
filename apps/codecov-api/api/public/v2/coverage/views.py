from datetime import datetime

from django import forms
from django.core.exceptions import ValidationError as DjangoValidationError
from drf_spectacular.utils import extend_schema
from rest_framework import mixins, viewsets
from rest_framework.exceptions import APIException, ValidationError

from api.public.v2.schema import repo_parameters
from api.shared.mixins import RepoPropertyMixin
from api.shared.permissions import RepositoryArtifactPermissions
from reports.models import RepositoryFlag
from timeseries.helpers import (
    aggregate_measurements,
    repository_coverage_measurements_with_fallback,
)
from timeseries.models import (
    Interval,
    MeasurementName,
    MeasurementSummary,
    MeasurementSummary1Day,
)

from .filters import MeasurementFilters
from .serializers import MeasurementSerializer


class InvalidInterval(APIException):
    status_code = 422
    default_detail = "You must specify an interval (1d/7d/30d)"
    default_code = "invalid_interval"


intervals = {
    "1d": Interval.INTERVAL_1_DAY,
    "7d": Interval.INTERVAL_7_DAY,
    "30d": Interval.INTERVAL_30_DAY,
}


@extend_schema(parameters=repo_parameters, tags=["Coverage"])
class CoverageViewSet(
    viewsets.GenericViewSet,
    mixins.ListModelMixin,
    RepoPropertyMixin,
):
    permission_classes = [RepositoryArtifactPermissions]
    serializer_class = MeasurementSerializer
    filterset_class = MeasurementFilters

    # this is here so that drf-spectacular can introspect the model filters
    queryset = MeasurementSummary1Day.objects.none()

    def get_queryset(self):
        interval = self.get_measurement_interval()
        start_date = self.get_date_param("start_date")
        end_date = self.get_date_param("end_date")
        return repository_coverage_measurements_with_fallback(
            self.repo,
            interval,
            start_date=start_date if start_date is not None else datetime(2000, 1, 1),
            end_date=end_date if end_date is not None else datetime.now(),
            branch=self.request.query_params.get("branch"),
        )

    def get_date_param(self, name: str) -> datetime | None:
        """
        Parse a datetime query param, raising a 400 if it is malformed rather
        than passing the raw string through to the ORM.
        """
        value = self.request.query_params.get(name)
        if value is None:
            return None
        try:
            return forms.DateTimeField(required=False).clean(value)
        except DjangoValidationError as e:
            raise ValidationError({name: e.messages})

    def get_measurement_interval(self) -> Interval:
        interval_name = self.request.query_params.get("interval")
        if interval_name not in intervals:
            raise InvalidInterval()

        return intervals[interval_name]

    @extend_schema(summary="Coverage trend")
    def list(self, request, *args, **kwargs):
        """
        Returns a paginated list of timeseries measurements aggregated by the specified
        `interval`.  If there are no measurements on `start_date` then the response will include
        1 measurement older than `start_date` so that the coverage value can be carried forward
        if necessary.

        Optionally filterable by:
        * `branch`
        * `start_date`
        * `end_date`
        """
        return super().list(request, *args, **kwargs)


@extend_schema(parameters=repo_parameters, tags=["Flags"])
class FlagCoverageViewSet(CoverageViewSet):
    def get_queryset(self):
        queryset = MeasurementSummary.agg_by(self.get_measurement_interval())

        flag = RepositoryFlag.objects.filter(
            repository_id=self.repo.pk,
            flag_name=self.kwargs["flag_name"],
        ).first()
        if not flag:
            return queryset.none()

        queryset = queryset.filter(
            name=MeasurementName.FLAG_COVERAGE.value,
            owner_id=self.repo.author_id,
            repo_id=self.repo.pk,
            measurable_id=str(flag.pk),
        )

        start_date = self.get_date_param("start_date")
        if start_date is not None:
            queryset = queryset.filter(timestamp_bin__gte=start_date)
        end_date = self.get_date_param("end_date")
        if end_date is not None:
            queryset = queryset.filter(timestamp_bin__lte=end_date)

        return aggregate_measurements(
            queryset, ["timestamp_bin", "owner_id", "repo_id", "measurable_id"]
        )
