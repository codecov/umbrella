import hashlib

import sentry_sdk
from ariadne import ObjectType, UnionType
from asgiref.sync import sync_to_async
from graphql import GraphQLResolveInfo

from graphql_api.types.errors import ProviderError, UnknownPath
from graphql_api.types.errors.errors import UnknownFlags
from graphql_api.types.segment_comparison.segment_comparison import SegmentComparisons
from services.comparison import (
    Comparison,
    FileComparison,
    ImpactedFile,
    MissingComparisonReport,
)
from shared.reports.types import ReportTotals
from shared.torngit.exceptions import TorngitClientError

impacted_file_bindable = ObjectType("ImpactedFile")


@impacted_file_bindable.field("fileName")
def resolve_file_name(impacted_file: ImpactedFile, info) -> str:
    return impacted_file.file_name


@impacted_file_bindable.field("headName")
def resolve_head_name(impacted_file: ImpactedFile, info) -> str:
    return impacted_file.head_name


@impacted_file_bindable.field("baseName")
def resolve_base_name(impacted_file: ImpactedFile, info) -> str:
    return impacted_file.base_name


@impacted_file_bindable.field("headCoverage")
def resolve_head_coverage(impacted_file: ImpactedFile, info) -> ReportTotals:
    return impacted_file.head_coverage


@impacted_file_bindable.field("baseCoverage")
def resolve_base_coverage(impacted_file: ImpactedFile, info) -> ReportTotals:
    return impacted_file.base_coverage


@impacted_file_bindable.field("patchCoverage")
def resolve_patch_coverage(impacted_file: ImpactedFile, info) -> ReportTotals:
    return impacted_file.patch_coverage


@impacted_file_bindable.field("changeCoverage")
def resolve_change_coverage(impacted_file: ImpactedFile, info) -> float:
    return impacted_file.change_coverage


@impacted_file_bindable.field("hashedPath")
def resolve_hashed_path(impacted_file: ImpactedFile, info) -> str:
    path = impacted_file.head_name
    encoded_path = path.encode()
    md5_path = hashlib.md5(encoded_path)

    return md5_path.hexdigest()


@impacted_file_bindable.field("segments")
@sync_to_async
@sentry_sdk.trace
def resolve_segments(
    impacted_file: ImpactedFile, info: GraphQLResolveInfo, filters: dict | None = None
) -> SegmentComparisons | UnknownPath | ProviderError:
    if filters is None:
        filters = {}
    if "comparison" not in info.context:
        return SegmentComparisons(results=[])

    comparison: Comparison = info.context["comparison"]
    try:
        comparison.validate()
    except MissingComparisonReport:
        return SegmentComparisons(results=[])
    path = impacted_file.head_name

    flags = filters.get("flags") or []

    if flags:
        available_flags = set(comparison.head_report.get_flag_names())
        unknown_flags = set(flags) - available_flags
        if unknown_flags:
            return UnknownFlags(f"No coverage with chosen flags: {unknown_flags}")

    try:
        file_comparison = comparison.get_file_comparison(
            path, with_src=True, bypass_max_diff=True
        )
    except TorngitClientError as e:
        if e.code == 404:
            return UnknownPath(f"path does not exist: {path}")
        else:
            return ProviderError()

    if flags:
        # Build a flag-filtered FileComparison by merging flag-specific reports
        merged_head_file = None
        merged_base_file = None
        for flag_name in flags:
            flag_comp = comparison.flag_comparison(flag_name)
            if flag_comp.head_report is not None:
                flag_head_file = flag_comp.head_report.get(path)
                if flag_head_file is not None:
                    merged_head_file = flag_head_file if merged_head_file is None else merged_head_file | flag_head_file
            if flag_comp.base_report is not None:
                flag_base_file = flag_comp.base_report.get(path)
                if flag_base_file is not None:
                    merged_base_file = flag_base_file if merged_base_file is None else merged_base_file | flag_base_file

        diff_data = comparison.git_comparison["diff"]["files"].get(path)
        file_comparison = FileComparison(
            base_file=merged_base_file,
            head_file=merged_head_file,
            diff_data=diff_data,
            src=file_comparison.src,
            bypass_max_diff=True,
        )

    segments = file_comparison.segments or []

    if filters.get("has_unintended_changes") is True:
        # segments with no diff changes and at least 1 unintended change
        segments = [segment for segment in segments if segment.has_unintended_changes]
    elif filters.get("has_unintended_changes") is False:
        new_segments = []
        for segment in segments:
            if segment.has_diff_changes:
                segment.remove_unintended_changes()
                new_segments.append(segment)
        segments = new_segments

    return SegmentComparisons(results=segments)


@impacted_file_bindable.field("isNewFile")
def resolve_is_new_file(impacted_file: ImpactedFile, info) -> bool:
    base_name = impacted_file.base_name
    head_name = impacted_file.head_name
    return base_name is None and head_name is not None


@impacted_file_bindable.field("isRenamedFile")
def resolve_is_renamed_file(impacted_file: ImpactedFile, info) -> bool:
    base_name = impacted_file.base_name
    head_name = impacted_file.head_name
    return base_name is not None and head_name is not None and base_name != head_name


@impacted_file_bindable.field("isDeletedFile")
def resolve_is_deleted_file(impacted_file: ImpactedFile, info) -> bool:
    base_name = impacted_file.base_name
    head_name = impacted_file.head_name
    return base_name is not None and head_name is None


@impacted_file_bindable.field("missesCount")
def resolve_misses_count(impacted_file: ImpactedFile, info) -> int:
    return impacted_file.misses_count


impacted_files_result_bindable = UnionType("ImpactedFilesResult")


@impacted_files_result_bindable.type_resolver
def resolve_files_result_type(res, *_):
    if isinstance(res, UnknownFlags):
        return "UnknownFlags"
    elif isinstance(res, type({"results": list})):
        return "ImpactedFiles"
