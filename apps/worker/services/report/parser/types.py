from io import BytesIO, RawIOBase
from typing import Any

from services.path_fixer.fixpaths import clean_toc
from services.report.fixes import get_fixes_from_raw


class ParsedUploadedReportFile:
    def __init__(
        self,
        filename: str | None,
        file_contents: bytes,
        labels: list[str] | None = None,
    ):
        self.filename = filename
        self.contents = file_contents
        self.size = len(self.contents)
        self.labels = labels

    def get_first_line(self):
        return BytesIO(self.contents).readline()


class ConcatenatedIO(RawIOBase):
    """A file-like object that lazily concatenates multiple byte sequences
    (headers and uploaded file contents) without materializing them all into
    a single buffer first."""

    def __init__(self, chunks: list[bytes]):
        self._chunks = chunks
        self._chunk_index = 0
        self._chunk_offset = 0

    def readable(self) -> bool:
        return True

    def readinto(self, b: bytearray) -> int:
        total = 0
        view = memoryview(b)
        while total < len(b) and self._chunk_index < len(self._chunks):
            chunk = self._chunks[self._chunk_index]
            remaining_in_chunk = len(chunk) - self._chunk_offset
            space = len(b) - total
            to_copy = min(remaining_in_chunk, space)
            view[total : total + to_copy] = chunk[
                self._chunk_offset : self._chunk_offset + to_copy
            ]
            total += to_copy
            self._chunk_offset += to_copy
            if self._chunk_offset >= len(chunk):
                self._chunk_index += 1
                self._chunk_offset = 0
        return total


class ParsedRawReport:
    """
    Parsed raw report parent class

    Attributes
    ----------
    toc
        table of contents, this lists the files relevant to the report,
        i.e. the files contained in the repository
    env
        list of env vars in environment of uploader (legacy only)
    uploaded_files
        list of class ParsedUploadedReportFile describing uploaded coverage files
    report_fixes
        list of objects describing report_fixes for each file, the format differs between
        legacy and VersionOne parsed raw report
    """

    def __init__(
        self,
        toc: Any,
        env: Any,
        uploaded_files: list[ParsedUploadedReportFile],
        report_fixes: Any,
    ):
        self.toc = toc
        self.env = env
        self.uploaded_files = uploaded_files
        self.report_fixes = report_fixes

    def has_toc(self) -> bool:
        return self.toc is not None

    def has_env(self) -> bool:
        return self.env is not None

    def has_report_fixes(self) -> bool:
        return self.report_fixes is not None

    @property
    def size(self):
        return sum(f.size for f in self.uploaded_files)

    def content(self) -> BytesIO:
        buffer = BytesIO()
        if self.has_toc():
            for file in self.get_toc():
                buffer.write(f"{file}\n".encode())
            buffer.write(b"<<<<<< network\n\n")
        for file in self.uploaded_files:
            buffer.write(f"# path={file.filename}\n".encode())
            buffer.write(file.contents)
            buffer.write(b"\n<<<<<< EOF\n\n")
        buffer.seek(0)
        return buffer


class VersionOneParsedRawReport(ParsedRawReport):
    """
    report_fixes : Dict[str, Dict[str, any]]
    {
        <path to file>: {
            eof: int | None
            lines: List[int]
        },
        ...
    }
    """

    def get_toc(self) -> list[str]:
        return self.toc

    def get_env(self):
        return self.env

    def get_uploaded_files(self):
        return self.uploaded_files

    def get_report_fixes(self, path_fixer) -> dict[str, dict[str, Any]]:
        return self.report_fixes

    def content_stream(self) -> ConcatenatedIO:
        """Return a lazy file-like object streaming all report content without
        materializing a single combined buffer in memory first."""
        chunks: list[bytes] = []
        if self.has_toc():
            for file in self.get_toc():
                chunks.append(f"{file}\n".encode())
            chunks.append(b"<<<<<< network\n\n")
        for file in self.uploaded_files:
            chunks.append(f"# path={file.filename}\n".encode())
            chunks.append(file.contents)
            chunks.append(b"\n<<<<<< EOF\n\n")
        return ConcatenatedIO(chunks)


class LegacyParsedRawReport(ParsedRawReport):
    """
    report_fixes : BinaryIO
    <filename>:<line number>,<line number>,...
    """

    def get_toc(self) -> list[str]:
        return clean_toc(self.toc.decode(errors="replace").strip())

    def get_env(self):
        return self.env.decode(errors="replace")

    def get_uploaded_files(self):
        return self.uploaded_files

    def get_report_fixes(self, path_fixer) -> dict[str, dict[str, Any]]:
        report_fixes = self.report_fixes.decode(errors="replace")
        return get_fixes_from_raw(report_fixes, path_fixer)
