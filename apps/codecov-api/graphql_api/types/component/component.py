from ariadne import ObjectType
from asgiref.sync import sync_to_async

from core.models import Commit
from services.components import Component, component_filtered_report
from shared.reports.types import ReportTotals

component_bindable = ObjectType("Component")


@component_bindable.field("id")
def resolve_id(component: Component, info) -> str:
    return component.component_id


@component_bindable.field("componentId")
def resolve_component_id(component: Component, info) -> str:
    return component.component_id


@component_bindable.field("name")
def resolve_name(component: Component, info) -> str:
    return component.get_display_name()


@component_bindable.field("totals")
@sync_to_async
def resolve_totals(component: Component, info) -> ReportTotals | None:
    commit: Commit = info.context["component_commit"]
    report = commit.full_report
    filtered_report = component_filtered_report(report, [component])
    return filtered_report.totals


def _get_component_totals(
    component: Component, commit: Commit, info
) -> ReportTotals | None:
    cache_key = f"component_totals_{component.component_id}_{commit.commitid}"
    if cache_key in info.context:
        return info.context[cache_key]
    report = commit.full_report
    if report is None:
        info.context[cache_key] = None
        return None
    filtered_report = component_filtered_report(report, [component])
    totals = filtered_report.totals
    info.context[cache_key] = totals
    return totals


@component_bindable.field("percentCovered")
@sync_to_async
def resolve_percent_covered(component: Component, info) -> float | None:
    commit: Commit = info.context["component_commit"]
    totals = _get_component_totals(component, commit, info)
    return totals.coverage if totals else None


@component_bindable.field("lineCount")
@sync_to_async
def resolve_line_count(component: Component, info) -> int | None:
    commit: Commit = info.context["component_commit"]
    totals = _get_component_totals(component, commit, info)
    return totals.lines if totals else None


@component_bindable.field("hitsCount")
@sync_to_async
def resolve_hits_count(component: Component, info) -> int | None:
    commit: Commit = info.context["component_commit"]
    totals = _get_component_totals(component, commit, info)
    return totals.hits if totals else None


@component_bindable.field("percentChange")
@sync_to_async
def resolve_percent_change(component: Component, info) -> float | None:
    commit: Commit = info.context["component_commit"]
    if not commit.parent_commit_id:
        return None
    try:
        parent_commit = Commit.objects.get(
            commitid=commit.parent_commit_id,
            repository_id=commit.repository_id,
        )
    except Commit.DoesNotExist:
        return None
    current_totals = _get_component_totals(component, commit, info)
    parent_totals = _get_component_totals(component, parent_commit, info)
    if current_totals is None or parent_totals is None:
        return None
    if current_totals.coverage is None or parent_totals.coverage is None:
        return None
    return current_totals.coverage - parent_totals.coverage
