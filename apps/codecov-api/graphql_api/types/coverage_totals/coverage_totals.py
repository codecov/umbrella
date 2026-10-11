from ariadne import ObjectType

coverage_totals_bindable = ObjectType("CoverageTotals")

coverage_totals_bindable.set_alias("percentCovered", "coverage")
coverage_totals_bindable.set_alias("fileCount", "files")
coverage_totals_bindable.set_alias("lineCount", "lines")
coverage_totals_bindable.set_alias("hitsCount", "hits")
coverage_totals_bindable.set_alias("missesCount", "misses")
coverage_totals_bindable.set_alias("partialsCount", "partials")


@coverage_totals_bindable.field("coverageChange")
def resolve_coverage_change(totals, info):
    # Deprecated: coverage change cannot be computed from a single totals object.
    # Use `changeCoverage` on the `Comparison` type instead.
    return None
