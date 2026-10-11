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


@flag_comparison_bindable.field("hasUnintendedChanges")
def resolve_has_unintended_changes(flag_comparison: FlagComparison, info) -> bool:
    head_totals = flag_comparison.head_totals
    base_totals = flag_comparison.base_totals
    patch_totals = flag_comparison.patch_totals

    if not head_totals or not base_totals:
        return False

    head_coverage = head_totals.get("coverage")
    base_coverage = base_totals.get("coverage")

    if head_coverage == base_coverage:
        return False

    # Coverage changed between base and head; if the patch has no lines for
    # this flag, the change cannot be attributed to direct code changes.
    patch_lines = (patch_totals or {}).get("lines", 0) or 0
    return patch_lines == 0
