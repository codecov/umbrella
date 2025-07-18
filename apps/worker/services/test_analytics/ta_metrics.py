from shared.metrics import Counter, Summary

write_tests_summary = Summary(
    "write_tests_summary",
    "The time it takes to write tests to the database",
    ["impl"],
)

read_tests_totals_summary = Summary(
    "read_tests_totals_summary",
    "The time it takes to read tests totals from the database",
    ["impl"],
)

read_failures_summary = Summary(
    "read_failures_summary",
    "The time it takes to read failures from the database",
    ["impl"],
)


read_rollups_from_db_summary = Summary(
    "read_rollups_from_db_summary",
    "The time it takes to read rollups from the database",
    ["impl"],
)

rollup_size_summary = Summary(
    "rollup_size_summary",
    "The size of the rollup",
    ["impl"],
)


process_flakes_summary = Summary(
    "process_flakes_summary",
    "The time it takes to process flakes",
    ["impl"],
)

new_ta_tasks_repo_summary = Counter(
    "new_ta_tasks_repo_summary",
    "Number of repos that were processed using 'new' ta tasks, due to being created after 2025-05-30",
)
