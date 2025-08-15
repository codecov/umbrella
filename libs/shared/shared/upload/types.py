from enum import Enum, StrEnum
from typing import TypedDict


class UploaderType(Enum):
    LEGACY = "legacy"
    CLI = "cli"


class UploadPipeline(StrEnum):
    CODECOV = "codecov"
    SENTRY = "sentry"


class TAUploadContext(TypedDict):
    commit_sha: str
    branch: str | None
    merged: bool
    pipeline: UploadPipeline
    pull_id: int | None
    storage_path: str
    flags: list[str] | None
