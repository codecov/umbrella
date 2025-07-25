import logging
from typing import Any

import sentry_sdk
import yaml
from ariadne import ObjectType
from asgiref.sync import sync_to_async
from graphql import GraphQLResolveInfo

import services.components as components_service
import services.path as path_service
import shared.reports.api_report_service as report_service
from codecov_auth.constants import USE_SENTRY_APP_INDICATOR
from codecov_auth.models import Owner
from core.models import Commit
from graphql_api.actions.commits import commit_status
from graphql_api.actions.comparison import validate_commit_comparison
from graphql_api.actions.path_contents import sort_path_contents
from graphql_api.dataloader.bundle_analysis import (
    load_bundle_analysis_comparison,
    load_bundle_analysis_report,
)
from graphql_api.dataloader.commit import CommitLoader
from graphql_api.dataloader.comparison import ComparisonLoader
from graphql_api.dataloader.owner import OwnerLoader
from graphql_api.helpers.connection import (
    queryset_to_connection,
    queryset_to_connection_sync,
)
from graphql_api.helpers.requested_fields import selected_fields
from graphql_api.types.comparison.comparison import (
    MissingBaseCommit,
    MissingBaseReport,
    MissingComparison,
    MissingHeadReport,
)
from graphql_api.types.enums import OrderingDirection, PathContentDisplayType
from graphql_api.types.errors import MissingCoverage, UnknownPath
from graphql_api.types.errors.errors import UnknownFlags
from reports.models import CommitReport
from services.bundle_analysis import BundleAnalysisComparison, BundleAnalysisReport
from services.comparison import Comparison, ComparisonReport
from services.components import Component
from services.path import Dir, File, ReportPaths
from services.yaml import YamlStates, get_yaml_state
from shared.reports.api_report_service import ReadOnlyReport
from shared.reports.filtered import FilteredReportFile
from shared.reports.resources import ReportFile
from shared.reports.types import ReportTotals

commit_bindable = ObjectType("Commit")
commit_coverage_analytics_bindable = ObjectType("CommitCoverageAnalytics")
commit_bundle_analysis_bindable = ObjectType("CommitBundleAnalysis")

commit_bindable.set_alias("createdAt", "timestamp")
commit_bindable.set_alias("pullId", "pullid")
commit_bindable.set_alias("branchName", "branch")

log = logging.getLogger(__name__)


@commit_bindable.field("author")
def resolve_author(commit: Commit, info: GraphQLResolveInfo) -> Owner | None:
    if commit.author_id:
        return OwnerLoader.loader(info).load(commit.author_id)


@commit_bindable.field("parent")
def resolve_parent(commit: Commit, info: GraphQLResolveInfo) -> Commit | None:
    if commit.parent_commit_id:
        return CommitLoader.loader(info, commit.repository_id).load(
            commit.parent_commit_id
        )


@commit_bindable.field("yaml")
async def resolve_yaml(commit: Commit, info: GraphQLResolveInfo) -> dict:
    command = info.context["executor"].get_command("commit")
    final_yaml = await command.get_final_yaml(commit)
    return yaml.dump(final_yaml)


@commit_bindable.field("yamlState")
async def resolve_yaml_state(
    commit: Commit, info: GraphQLResolveInfo
) -> YamlStates | None:
    command = info.context["executor"].get_command("commit")
    final_yaml = await command.get_final_yaml(commit)
    return get_yaml_state(yaml=final_yaml)


@commit_bindable.field("uploads")
@sync_to_async
def resolve_list_uploads(commit: Commit, info: GraphQLResolveInfo, **kwargs):
    if not commit.commitreport:
        return queryset_to_connection_sync([])

    queryset = commit.commitreport.sessions

    requested_fields = selected_fields(info)

    # the `requested_fields` here are prefixed with `edges.node`, as this is a `Connection`
    # and using `uploads { edges { node { ... } } }` is the way this is queried.
    if "edges.node.flags" in requested_fields:
        queryset = queryset.prefetch_related("flags")
    if "edges.node.errors" in requested_fields:
        queryset = queryset.prefetch_related("errors")

    if not kwargs:  # temp to override kwargs -> return all current uploads
        kwargs["first"] = 999_999

    return queryset_to_connection_sync(
        queryset, ordering=("id",), ordering_direction=OrderingDirection.ASC, **kwargs
    )


@commit_bindable.field("compareWithParent")
@sentry_sdk.trace
async def resolve_compare_with_parent(
    commit: Commit, info: GraphQLResolveInfo, **kwargs: Any
) -> (
    ComparisonReport
    | MissingBaseCommit
    | MissingComparison
    | MissingBaseReport
    | MissingHeadReport
):
    if not commit.parent_commit_id:
        return MissingBaseCommit()

    comparison_loader = ComparisonLoader.loader(info, commit.repository_id)
    commit_comparison = await comparison_loader.load(
        (commit.parent_commit_id, commit.commitid)
    )

    comparison_error = validate_commit_comparison(commit_comparison=commit_comparison)
    if comparison_error:
        return comparison_error

    if commit_comparison.is_processed:
        current_owner = info.context["request"].current_owner
        parent_commit = await CommitLoader.loader(info, commit.repository_id).load(
            commit.parent_commit_id
        )
        should_use_sentry_app = getattr(
            info.context["request"], USE_SENTRY_APP_INDICATOR, False
        )
        comparison = Comparison(
            user=current_owner,
            base_commit=parent_commit,
            head_commit=commit,
            should_use_sentry_app=should_use_sentry_app,
        )
        info.context["comparison"] = comparison

    return ComparisonReport(commit_comparison)


@sentry_sdk.trace
def get_sorted_path_contents(
    current_owner: Owner,
    commit: Commit,
    path: str | None = None,
    filters: dict | None = None,
    should_use_sentry_app: bool = False,
) -> (
    list[File | Dir] | MissingHeadReport | MissingCoverage | UnknownFlags | UnknownPath
):
    # TODO: Might need to add reports here filtered by flags in the future
    report = report_service.build_report_from_commit(
        commit, report_class=ReadOnlyReport
    )
    if not report:
        return MissingHeadReport()

    if filters is None:
        filters = {}
    search_value = filters.get("search_value")
    display_type = filters.get("display_type")

    flags_filter = filters.get("flags", [])
    component_filter = filters.get("components", [])

    component_paths = []
    component_flags = []

    report_flags = report.get_flag_names()

    if component_filter:
        all_components = components_service.commit_components(
            commit, current_owner, should_use_sentry_app=should_use_sentry_app
        )
        filtered_components = components_service.filter_components_by_name_or_id(
            all_components, component_filter
        )

        if not filtered_components:
            return MissingCoverage(
                f"missing coverage for report with components: {component_filter}"
            )

        for component in filtered_components:
            component_paths.extend(component.paths)
            if report_flags:
                component_flags.extend(component.get_matching_flags(report_flags))

    if component_flags:
        if flags_filter:
            flags_filter = list(set(flags_filter) & set(component_flags))
        else:
            flags_filter = component_flags

    if flags_filter and not report_flags:
        return UnknownFlags(f"No coverage with chosen flags: {flags_filter}")

    report_paths = ReportPaths(
        report=report,
        path=path,
        search_term=search_value,
        filter_flags=flags_filter,
        filter_paths=component_paths,
    )

    if len(report_paths.paths) == 0:
        # we do not know about this path

        if (
            path_service.provider_path_exists(
                path, commit, current_owner, should_use_sentry_app=should_use_sentry_app
            )
            is False
        ):
            # file doesn't exist
            return UnknownPath(f"path does not exist: {path}")

        # we're just missing coverage for the file
        return MissingCoverage(f"missing coverage for path: {path}")

    items: list[File | Dir]
    if search_value or display_type == PathContentDisplayType.LIST:
        items = report_paths.full_filelist()
    else:
        items = report_paths.single_directory()
    return sort_path_contents(items, filters)


@commit_bindable.field("pathContents")
@sync_to_async
def resolve_path_contents(
    commit: Commit,
    info: GraphQLResolveInfo,
    path: str | None = None,
    filters: dict | None = None,
) -> Any:
    """
    The file directory tree is a list of all the files and directories
    extracted from the commit report of the latest, head commit.
    The is resolver results in a list that represent the tree with files
    and nested directories.
    """
    current_owner = info.context["request"].current_owner
    should_use_sentry_app = getattr(
        info.context["request"], USE_SENTRY_APP_INDICATOR, False
    )

    contents = get_sorted_path_contents(
        current_owner,
        commit,
        path,
        filters,
        should_use_sentry_app=should_use_sentry_app,
    )
    if isinstance(contents, list):
        return {"results": contents}
    return contents


@commit_bindable.field("deprecatedPathContents")
@sync_to_async
def resolve_deprecated_path_contents(
    commit: Commit,
    info: GraphQLResolveInfo,
    path: str | None = None,
    filters: dict | None = None,
    first: Any = None,
    after: Any = None,
    last: Any = None,
    before: Any = None,
) -> Any:
    """
    The file directory tree is a list of all the files and directories
    extracted from the commit report of the latest, head commit.
    The is resolver results in a list that represent the tree with files
    and nested directories.
    """
    current_owner = info.context["request"].current_owner
    should_use_sentry_app = getattr(
        info.context["request"], USE_SENTRY_APP_INDICATOR, False
    )

    contents = get_sorted_path_contents(
        current_owner,
        commit,
        path,
        filters,
        should_use_sentry_app=should_use_sentry_app,
    )
    if not isinstance(contents, list):
        return contents

    return queryset_to_connection_sync(
        contents,
        ordering_direction=OrderingDirection.ASC,
        first=first,
        last=last,
        before=before,
        after=after,
    )


@commit_bindable.field("errors")
async def resolve_errors(commit, info, error_type):
    command = info.context["executor"].get_command("commit")
    queryset = await command.get_commit_errors(commit, error_type=error_type)
    return await queryset_to_connection(
        queryset,
        ordering=("updated_at",),
        ordering_direction=OrderingDirection.ASC,
    )


@commit_bindable.field("totalUploads")
async def resolve_total_uploads(commit, info):
    command = info.context["executor"].get_command("commit")
    return await command.get_uploads_number(commit)


@commit_bindable.field("bundleStatus")
@sync_to_async
@sentry_sdk.trace
def resolve_bundle_status(commit: Commit, info: GraphQLResolveInfo) -> str | None:
    return commit_status(info, commit, CommitReport.ReportType.BUNDLE_ANALYSIS)


@commit_bindable.field("coverageStatus")
@sync_to_async
@sentry_sdk.trace
def resolve_coverage_status(commit: Commit, info: GraphQLResolveInfo) -> str | None:
    return commit_status(info, commit, CommitReport.ReportType.COVERAGE)


@commit_bindable.field("coverageAnalytics")
def resolve_commit_coverage(commit, info):
    return commit


@commit_bindable.field("bundleAnalysis")
def resolve_commit_bundle_analysis(commit, info):
    return commit


### Commit Coverage Bindable ###


@commit_coverage_analytics_bindable.field("totals")
@sentry_sdk.trace
def resolve_coverage_totals(
    commit: Commit, info: GraphQLResolveInfo
) -> ReportTotals | None:
    command = info.context["executor"].get_command("commit")
    return command.fetch_totals(commit)


@commit_coverage_analytics_bindable.field("flagNames")
@sync_to_async
@sentry_sdk.trace
def resolve_coverage_flags(commit: Commit, info: GraphQLResolveInfo) -> list[str]:
    return commit.full_report.get_flag_names() if commit.full_report else []


@commit_coverage_analytics_bindable.field("coverageFile")
@sync_to_async
@sentry_sdk.trace
def resolve_coverage_file(commit, info, path, flags=None, components=None):
    fallback_file, paths = None, []
    if components:
        all_components = components_service.commit_components(
            commit,
            info.context["request"].current_owner,
            should_use_sentry_app=getattr(
                info.context["request"], USE_SENTRY_APP_INDICATOR, False
            ),
        )
        filtered_components = components_service.filter_components_by_name_or_id(
            all_components, components
        )
        for fc in filtered_components:
            paths.extend(fc.paths)
        fallback_file = FilteredReportFile(ReportFile(path), [])

    commit_report = commit.full_report.filter(flags=flags, paths=paths)
    file_report = commit_report.get(path) or fallback_file

    return {
        "commit_report": commit_report,
        "file_report": file_report,
        "commit": commit,
        "path": path,
        "flags": flags,
        "components": components,
    }


@commit_coverage_analytics_bindable.field("components")
@sync_to_async
@sentry_sdk.trace
def resolve_coverage_components(commit: Commit, info, filters=None) -> list[Component]:
    info.context["component_commit"] = commit
    current_owner = info.context["request"].current_owner
    all_components = components_service.commit_components(
        commit,
        current_owner,
        should_use_sentry_app=getattr(
            info.context["request"],
            USE_SENTRY_APP_INDICATOR,
            False,
        ),
    )

    if filters and filters.get("components"):
        return components_service.filter_components_by_name_or_id(
            all_components, filters["components"]
        )

    return all_components


### Commit Bundle Analysis Bindable ###


@commit_bundle_analysis_bindable.field("bundleAnalysisCompareWithParent")
@sentry_sdk.trace
async def resolve_commit_bundle_analysis_compare_with_parent(
    commit: Commit, info: GraphQLResolveInfo
) -> BundleAnalysisComparison | Any:
    if not commit.parent_commit_id:
        return MissingBaseCommit()
    base_commit = await CommitLoader.loader(info, commit.repository_id).load(
        commit.parent_commit_id
    )

    bundle_analysis_comparison = await sync_to_async(load_bundle_analysis_comparison)(
        base_commit, commit
    )

    # Store the created SQLite DB path in info.context
    # when the request is fully handled, have the file deleted
    if isinstance(bundle_analysis_comparison, BundleAnalysisComparison):
        info.context[
            "request"
        ].bundle_analysis_base_report_db_path = (
            bundle_analysis_comparison.comparison.base_report.db_path
        )
        info.context[
            "request"
        ].bundle_analysis_head_report_db_path = (
            bundle_analysis_comparison.comparison.head_report.db_path
        )

    return bundle_analysis_comparison


@commit_bundle_analysis_bindable.field("bundleAnalysisReport")
@sync_to_async
@sentry_sdk.trace
def resolve_commit_bundle_analysis_report(commit: Commit, info) -> BundleAnalysisReport:
    bundle_analysis_report = load_bundle_analysis_report(commit)

    # Store the created SQLite DB path in info.context
    # when the request is fully handled, have the file deleted
    if isinstance(bundle_analysis_report, BundleAnalysisReport):
        info.context[
            "request"
        ].bundle_analysis_head_report_db_path = bundle_analysis_report.report.db_path

    info.context["commit"] = commit

    return bundle_analysis_report


@commit_bindable.field("latestUploadError")
async def resolve_latest_upload_error(commit, info):
    command = info.context["executor"].get_command("commit")
    return await command.get_latest_upload_error(commit)
