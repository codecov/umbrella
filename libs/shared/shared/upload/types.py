from typing import TypedDict


class TAUploadContext(TypedDict):
    commit_sha: str
    branch: str
    merged: bool
    pull_id: int | None
    flags: list[str] | None
