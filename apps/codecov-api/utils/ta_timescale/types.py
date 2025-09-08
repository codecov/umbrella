from dataclasses import dataclass


@dataclass
class TestResultsAggregates:
    __test__ = False

    total_duration: float
    slowest_tests_duration: float
    total_slow_tests: int
    fails: int
    skips: int
    total_duration_percent_change: float | None = None
    slowest_tests_duration_percent_change: float | None = None
    total_slow_tests_percent_change: float | None = None
    fails_percent_change: float | None = None
    skips_percent_change: float | None = None


@dataclass
class FlakeAggregates:
    __test__ = False

    flake_count: int
    flake_rate: float
    flake_count_percent_change: float | None = None
    flake_rate_percent_change: float | None = None
