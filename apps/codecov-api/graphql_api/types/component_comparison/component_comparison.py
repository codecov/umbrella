import re

from ariadne import ObjectType
from asgiref.sync import sync_to_async

from compare.models import ComponentComparison
from services.components import Component
from shared.reports.types import ReportTotals

component_comparison_bindable = ObjectType("ComponentComparison")


@component_comparison_bindable.field("id")
def resolve_id(component_comparison: ComponentComparison, info) -> str:
    return component_comparison.component_id


@component_comparison_bindable.field("name")
def resolve_name(component_comparison: ComponentComparison, info) -> str:
    components: dict[str, Component] = info.context["components"]
    component = components.get(component_comparison.component_id)
    if component:
        return component.get_display_name()
    else:
        # not sure when we would ever get here
        # (yaml components out-of-sync with database for some reason)
        return component_comparison.component_id


@component_comparison_bindable.field("baseTotals")
def resolve_base_totals(
    component_comparison: ComponentComparison, info
) -> ReportTotals:
    return component_comparison.base_totals


@component_comparison_bindable.field("headTotals")
def resolve_head_totals(
    component_comparison: ComponentComparison, info
) -> ReportTotals:
    return component_comparison.head_totals


@component_comparison_bindable.field("patchTotals")
@sync_to_async
def resolve_patch_totals(
    component_comparison: ComponentComparison, info
) -> ReportTotals:
    return component_comparison.patch_totals


@component_comparison_bindable.field("hasUnintendedChanges")
def resolve_has_unintended_changes(
    component_comparison: ComponentComparison, info
) -> bool:
    comparison = info.context.get("comparison")
    if comparison is None:
        return False

    components: dict[str, Component] = info.context.get("components", {})
    component = components.get(component_comparison.component_id)
    if component is None:
        return False

    path_patterns = [re.compile(p) for p in (component.paths or [])]

    for impacted_file in comparison.impacted_files_with_unintended_changes:
        file_name = impacted_file.head_name or impacted_file.base_name
        if not file_name:
            continue
        if not path_patterns:
            # Component is flag-based only (no path filter) - any file qualifies
            return True
        for pattern in path_patterns:
            if pattern.search(file_name):
                return True

    return False
