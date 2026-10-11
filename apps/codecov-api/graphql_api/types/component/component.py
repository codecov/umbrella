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


def _get_totals(component: Component, info) -> ReportTotals | None:
    commit: Commit = info.context["component_commit"]
    report = commit.full_report
    filtered_report = component_filtered_report(report, [component])
    return filtered_report.totals


@component_bindable.field("percentCovered")
@sync_to_async
def resolve_percent_covered(component: Component, info) -> float | None:
    totals = _get_totals(component, info)
    return float(totals.coverage) if totals and totals.coverage is not None else None


@component_bindable.field("percentChange")
def resolve_percent_change(component: Component, info) -> None:
    # percentChange is not available on Component; requires time-series measurements
    return None


@component_bindable.field("lineCount")
@sync_to_async
def resolve_line_count(component: Component, info) -> int | None:
    totals = _get_totals(component, info)
    return totals.lines if totals else None


@component_bindable.field("hitsCount")
@sync_to_async
def resolve_hits_count(component: Component, info) -> int | None:
    totals = _get_totals(component, info)
    return totals.hits if totals else None
