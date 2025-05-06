from ariadne import ObjectType
from asgiref.sync import sync_to_async

from core.models import Commit
from services.components import Component, component_filtered_report
from shared.reports.types import ReportTotals

component_bindable = ObjectType("Component")


@component_bindable.field("id")
def resolve_id(component: Component, info) -> str:
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
