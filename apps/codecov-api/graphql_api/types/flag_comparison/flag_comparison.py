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
    head = flag_comparison.head_totals
    base = flag_comparison.base_totals
    if head is None or base is None:
        return None
    head_coverage = head.get("coverage") if isinstance(head, dict) else getattr(head, "coverage", None)
    base_coverage = base.get("coverage") if isinstance(base, dict) else getattr(base, "coverage", None)
    if head_coverage is None or base_coverage is None:
        return None
    return float(head_coverage) - float(base_coverage)
