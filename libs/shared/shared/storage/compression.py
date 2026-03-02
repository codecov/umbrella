import gzip
import importlib.metadata
from typing import IO


class GZipStreamReader:
    def __init__(self, fileobj: IO[bytes]):
        self.data = fileobj
        self.bytes_compressed = 0

    def read(self, size: int = -1, /) -> bytes:
        curr_data = self.data.read(size)

        if not curr_data:
            return b""

        compressed = gzip.compress(curr_data)
        self.bytes_compressed += len(compressed)
        return compressed

    def tell(self) -> int:
        return self.bytes_compressed


def zstd_decoded_by_default() -> bool:
    try:
        version = importlib.metadata.version("urllib3")
    except importlib.metadata.PackageNotFoundError:
        return False

    if version < "2.0.0":
        return False

    try:
        import zstandard  # noqa: F401, PLC0415

        return True
    except ImportError:
        return False
