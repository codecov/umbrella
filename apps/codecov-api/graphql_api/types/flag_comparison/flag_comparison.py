from ariadne import ObjectType

from compare.models import FlagComparison

flag_comparison_bindable = ObjectType("FlagComparison")


@flag_comparison_bindable.field("name")
def resolve_name(flag_comparison: FlagComparison, info) -> str:
    return flag_comparison.repositoryflag.flag_name


@flag_comparison_bindable.field("patchTotals")
def resolve_patch_totals(flag_comparison: FlagComparison, info) -> dict:
    return flag_comparison.patch_totals


@flag_comparison_bindable.field("headTotals")
def resolve_head_totals(flag_comparison: FlagComparison, info) -> dict:
    return flag_comparison.head_totals


@flag_comparison_bindable.field("baseTotals")
def resolve_base_totals(flag_comparison: FlagComparison, info) -> dict:
    return flag_comparison.base_totals


@flag_comparison_bindable.field("changeCode")
def resolve_change_code(flag_comparison: FlagComparison, info) -> float | None:
    head_totals = flag_comparison.head_totals
    base_totals = flag_comparison.base_totals
    if head_totals is None or base_totals is None:
        return None
    head_coverage = head_totals.get("coverage")
    base_coverage = base_totals.get("coverage")
    if head_coverage is None or base_coverage is None:
        return None
    return float(head_coverage) - float(base_coverage)
