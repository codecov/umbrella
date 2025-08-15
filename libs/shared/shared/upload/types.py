from typing import TypedDict


class TAUploadContext(TypedDict):
    commit_sha: str
    branch: str
    merged: bool
    pull_id: int | None
    storage_path: str
    flags: list[str] | None
