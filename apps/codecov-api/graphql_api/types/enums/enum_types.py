from ariadne import EnumType
from graphql import GraphQLError

from codecov_auth.models import RepositoryToken
from compare.commands.compare.interactors.fetch_impacted_files import (
    ImpactedFileParameter,
)
from core.models import Commit
from services.yaml import YamlStates
from shared.plan.constants import TierName, TrialStatus
from timeseries.models import Interval as MeasurementInterval
from timeseries.models import MeasurementName

from .enums import (
    AssetOrdering,
    BundleLoadTypes,
    CoverageLine,
    GoalOnboarding,
    LoginProvider,
    OrderingDirection,
    OrderingParameter,
    PathContentDisplayType,
    PullRequestState,
    RepositoryOrdering,
    SyncProvider,
    TestResultsFilterParameter,
    TestResultsOrderingParameter,
    TypeProjectOnboarding,
    UploadErrorEnum,
    UploadState,
    UploadType,
)


class SafeEnumType(EnumType):
    """
    Subclass of ariadne's EnumType that wraps the bound parse_value with
    TypeError handling. This prevents unhashable input values (e.g. a JSON
    object `{}` sent in place of an enum string) from surfacing internal
    Python implementation details in the GraphQL error message.
    """

    def bind_to_schema(self, schema) -> None:
        super().bind_to_schema(schema)
        graphql_type = schema.type_map.get(self.name)
        if graphql_type is None:
            return
        original_parse_value = graphql_type.parse_value

        def safe_parse_value(input_value):
            try:
                return original_parse_value(input_value)
            except TypeError:
                raise GraphQLError(
                    f"Expected type '{self.name}'. Value must be a string enum member."
                )

        graphql_type.parse_value = safe_parse_value


enum_types = [
    SafeEnumType("RepositoryOrdering", RepositoryOrdering),
    SafeEnumType("OrderingDirection", OrderingDirection),
    SafeEnumType("CoverageLine", CoverageLine),
    SafeEnumType("PathContentDisplayType", PathContentDisplayType),
    SafeEnumType("TypeProjectOnboarding", TypeProjectOnboarding),
    SafeEnumType("GoalOnboarding", GoalOnboarding),
    SafeEnumType("OrderingParameter", OrderingParameter),
    SafeEnumType("PullRequestState", PullRequestState),
    SafeEnumType("UploadState", UploadState),
    SafeEnumType("UploadType", UploadType),
    SafeEnumType("UploadErrorEnum", UploadErrorEnum),
    SafeEnumType("MeasurementInterval", MeasurementInterval),
    SafeEnumType("LoginProvider", LoginProvider),
    SafeEnumType("ImpactedFileParameter", ImpactedFileParameter),
    SafeEnumType("CommitState", Commit.CommitStates),
    SafeEnumType("MeasurementType", MeasurementName),
    SafeEnumType("RepositoryTokenType", RepositoryToken.TokenType),
    SafeEnumType("SyncProvider", SyncProvider),
    SafeEnumType("TierName", TierName),
    SafeEnumType("TrialStatus", TrialStatus),
    SafeEnumType("YamlStates", YamlStates),
    SafeEnumType("BundleLoadTypes", BundleLoadTypes),
    SafeEnumType("TestResultsOrderingParameter", TestResultsOrderingParameter),
    SafeEnumType("TestResultsFilterParameter", TestResultsFilterParameter),
    SafeEnumType("AssetOrdering", AssetOrdering),
]
