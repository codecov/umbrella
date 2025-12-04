import io
import json
import pathlib
import tarfile
import time
from types import TracebackType
from typing import Literal, Self

from google.cloud import storage

ARCHIVE_SIZE_THRESHOLD = 4 * 1024 * 1024 * 1024  # 4GB


class _Archiver:
    """Upload files to GCS as chunked tar.gz archives."""

    def __init__(
        self,
        local_dir: pathlib.Path,
        bucket: storage.Bucket,
        prefix: pathlib.PurePosixPath,
    ) -> None:
        self.current_archive = local_dir / "current_archive.tar.gz"
        self.bucket = bucket
        self.prefix = prefix
        self.time = time.time()
        self.counter = -1
        self._next_chunk()

    def _next_chunk(self) -> None:
        self.counter += 1
        self.entries = 0
        self.archive = tarfile.open(name=self.current_archive, mode="w:gz")

    def _upload(self) -> None:
        self.archive.close()
        blob_name = str(self.prefix / f"{self.counter}.tar.gz")
        blob = self.bucket.blob(blob_name)
        blob.upload_from_filename(self.current_archive, content_type="application/gzip")

    def _add_file(
        self, name: str, file_obj: io.BufferedIOBase, size: int | None = None
    ) -> None:
        info = tarfile.TarInfo(name)
        if size is None:
            file_obj.seek(0, 2)
            info.size = file_obj.tell()
            file_obj.seek(0, 0)
        else:
            info.size = size
        info.mode = 0o600
        info.mtime = self.time
        self.archive.addfile(info, file_obj)

        self.entries += 1
        if self.current_archive.stat().st_size >= ARCHIVE_SIZE_THRESHOLD:
            self._upload()
            self._next_chunk()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> Literal[False]:
        if exc_type is None and self.entries > 0:
            self._upload()
        self.archive.close()
        return False

    def upload_json(self, blob_name: str, data: dict) -> None:
        json_str = json.dumps(data, indent=2)
        json_bytes = json_str.encode("utf-8")
        self._add_file(blob_name, io.BytesIO(json_bytes), size=len(json_bytes))
