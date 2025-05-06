import logging
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from decimal import Decimal
from enum import Enum
from typing import TypedDict

log = logging.getLogger(__name__)


@dataclass
class ReportTotals:
    files: int = 0
    lines: int = 0
    hits: int = 0
    misses: int = 0
    partials: int = 0
    # The coverage is a string of a float that's rounded to 5 decimal places (or "100", "0")
    # i.e. "98.76543", "100", "0" are all valid.
    coverage: str | None = 0
    branches: int = 0
    methods: int = 0
    messages: int = 0
    sessions: int = 0
    complexity: int = 0
    complexity_total: int = 0
    diff: int = 0

    def __iter__(self):
        return iter(self.astuple())

    def astuple(self):
        return (
            self.files,
            self.lines,
            self.hits,
            self.misses,
            self.partials,
            self.coverage,
            self.branches,
            self.methods,
            self.messages,
            self.sessions,
            self.complexity,
            self.complexity_total,
            self.diff,
        )

    def to_database(self):
        obj = list(self)
        while obj and obj[-1] in ("0", 0):
            obj.pop()
        return obj

    def asdict(self):
        return asdict(self)

    @classmethod
    def default_totals(cls):
        return cls(
            files=0,
            lines=0,
            hits=0,
            misses=0,
            partials=0,
            coverage=None,
            branches=0,
            methods=0,
            messages=0,
            sessions=0,
            complexity=0,
            complexity_total=0,
            diff=0,
        )


@dataclass
class LineSession:
    __slots__ = ("id", "coverage", "branches", "partials", "complexity")
    id: int
    coverage: Decimal
    branches: list[int] | None
    partials: Sequence[int]
    complexity: int

    def __init__(self, id, coverage, branches=None, partials=None, complexity=None):
        self.id = id
        self.coverage = coverage
        self.branches = branches
        self.partials = partials
        self.complexity = complexity

    def astuple(self):
        if self.branches is None and self.partials is None and self.complexity is None:
            return (self.id, self.coverage)
        return (self.id, self.coverage, self.branches, self.partials, self.complexity)


@dataclass
class ReportLine:
    __slots__ = ("coverage", "type", "sessions", "messages", "complexity")
    coverage: Decimal
    type: str
    sessions: list[LineSession]
    messages: list[str]
    complexity: int | tuple[int, int]

    @classmethod
    def create(
        cls,
        coverage=None,
        type=None,
        sessions=None,
        messages=None,
        complexity=None,
        datapoints=None,
    ):
        if sessions:
            sessions = [
                LineSession(*sess) if not isinstance(sess, LineSession) else sess
                for sess in sessions
                if sess
            ]
        else:
            sessions = []

        return cls(
            coverage=coverage,
            type=type,
            sessions=sessions,
            messages=messages,
            complexity=complexity,
        )

    def astuple(self):
        return (
            self.coverage,
            self.type,
            [s.astuple() for s in self.sessions] if self.sessions else None,
            self.messages,
            self.complexity,
        )


@dataclass
class Change:
    path: str = None
    new: bool = False
    deleted: bool = False
    in_diff: bool = None
    old_path: str = None
    totals: ReportTotals = None

    def __post_init__(self):
        if self.totals is not None:
            if not isinstance(self.totals, ReportTotals):
                self.totals = ReportTotals(*self.totals)


EMPTY = ""

TOTALS_MAP = tuple("fnhmpcbdMsCN")


SessionTotals = ReportTotals


@dataclass
class NetworkFile:
    totals: ReportTotals
    diff_totals: ReportTotals

    def __init__(self, totals=None, diff_totals=None, *args, **kwargs) -> None:
        self.totals = totals
        self.diff_totals = diff_totals

    def astuple(self):
        return (
            self.totals.astuple(),
            # Placeholder for deprecated/broken `session_totals` field.
            # Old reports had a map of session ID to per-session totals here,
            # but they weren't used and a bug caused them to bloat wildly.
            None,
            self.diff_totals.astuple() if self.diff_totals else None,
        )


class ReportHeader(TypedDict):
    pass


class UploadType(Enum):
    COVERAGE = "coverage"
    TEST_RESULTS = "test_results"
    BUNDLE_ANALYSIS = "bundle_analysis"
