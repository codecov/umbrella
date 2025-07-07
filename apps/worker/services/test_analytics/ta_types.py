from dataclasses import dataclass
from typing import Literal


@dataclass
class TestResultsNotificationFailure[T: (str, bytes)]:
    failure_message: str
    display_name: str
    envs: list[str]
    test_id: T
    duration_seconds: float
    build_url: str | None = None


@dataclass
class FlakeInfo:
    failed: int
    count: int


@dataclass
class TACommentInDepthInfo[T: (str, bytes)]:
    failures: list[TestResultsNotificationFailure[T]]
    flaky_tests: dict[T, FlakeInfo]


@dataclass
class TestResultsNotificationPayload[T: (str, bytes)]:
    failed: int
    passed: int
    skipped: int
    info: TACommentInDepthInfo[T] | None = None


@dataclass
class ErrorPayload:
    error_code: Literal["unsupported_file_format", "file_not_in_storage", "warning"]
    error_message: str | None = None
