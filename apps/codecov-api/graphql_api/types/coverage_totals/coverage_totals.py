from ariadne import ObjectType

coverage_totals_bindable = ObjectType("CoverageTotals")

coverage_totals_bindable.set_alias("percentCovered", "coverage")
coverage_totals_bindable.set_alias("fileCount", "files")
coverage_totals_bindable.set_alias("lineCount", "lines")
coverage_totals_bindable.set_alias("hitsCount", "hits")
coverage_totals_bindable.set_alias("missesCount", "misses")
coverage_totals_bindable.set_alias("partialsCount", "partials")
# Deprecated aliases for backward compatibility
coverage_totals_bindable.set_alias("coveredLines", "hits")
coverage_totals_bindable.set_alias("missedLines", "misses")
coverage_totals_bindable.set_alias("partialLines", "partials")
