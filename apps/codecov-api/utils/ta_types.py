import datetime as dt
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


class TestResultsRow:
    __test__ = False

    # the order here must match the order of the fields in the query
    def __init__(
        self,
        name: str,
        failure_rate: float,
        flake_rate: float,
        updated_at: dt.datetime,
        avg_duration: float,
        total_duration: float,
        total_fail_count: int,
        total_flaky_fail_count: int,
        total_pass_count: int,
        total_skip_count: int,
        commits_where_fail: int,
        last_duration: float,
        testsuite: str | None = None,
        flags: list[str] | None = None,
    ):
        self.name = name
        self.testsuite = testsuite
        self.flags = flags or []
        self.failure_rate = failure_rate
        self.flake_rate = flake_rate
        self.updated_at = updated_at
        self.avg_duration = avg_duration
        self.total_duration = total_duration
        self.total_fail_count = total_fail_count
        self.total_flaky_fail_count = total_flaky_fail_count
        self.total_pass_count = total_pass_count
        self.total_skip_count = total_skip_count
        self.commits_where_fail = commits_where_fail
        self.last_duration = last_duration

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "testsuite": self.testsuite,
            "flags": self.flags,
            "failure_rate": self.failure_rate,
            "flake_rate": self.flake_rate,
            "avg_duration": self.avg_duration,
            "total_fail_count": self.total_fail_count,
            "total_flaky_fail_count": self.total_flaky_fail_count,
            "total_pass_count": self.total_pass_count,
            "total_skip_count": self.total_skip_count,
            "commits_where_fail": self.commits_where_fail,
            "last_duration": self.last_duration,
        }
