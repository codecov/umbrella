from ariadne import ObjectType

coverage_totals_bindable = ObjectType("CoverageTotals")

coverage_totals_bindable.set_alias("percentCovered", "coverage")
coverage_totals_bindable.set_alias("fileCount", "files")
coverage_totals_bindable.set_alias("filesCount", "files")
coverage_totals_bindable.set_alias("lineCount", "lines")
coverage_totals_bindable.set_alias("linesCount", "lines")
coverage_totals_bindable.set_alias("hitsCount", "hits")
coverage_totals_bindable.set_alias("missesCount", "misses")
coverage_totals_bindable.set_alias("partialsCount", "partials")
coverage_totals_bindable.set_alias("branchesCount", "branches")
coverage_totals_bindable.set_alias("methodsCount", "methods")
coverage_totals_bindable.set_alias("sessionsCount", "sessions")
