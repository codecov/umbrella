from datetime import datetime, timezone
from typing import Any

from timestring import Date

from services.report.report_builder import ReportBuilderSession


def normalize_timestamp(timestamp: str | None) -> str | None:
    """
    Normalize a timestamp string.

    Handles millisecond Unix timestamps (13+ digits) by converting them to
    seconds.

    Args:
        timestamp: A timestamp string, which may be:
            - A millisecond Unix timestamp (e.g., "1768258631332")
            - A seconds Unix timestamp (e.g., "1768258631")
            - A date string (e.g., "2026-01-12")

    Returns:
        The normalized timestamp string, or None if input is None/empty.
    """
    if not timestamp:
        return None

    if timestamp.isdigit() and len(timestamp) >= 13:
        # Convert milliseconds to seconds
        return str(int(timestamp) // 1000)

    return timestamp


def is_report_expired(timestamp: str | None, max_age) -> bool:
    """
    Determine whether a report timestamp is older than max_age.

    For pure numeric (Unix epoch seconds) timestamps, uses
    datetime.fromtimestamp() to avoid timestring.Date() misinterpreting
    large integers. For human-readable date strings, falls back to
    timestring.Date() comparison.

    Args:
        timestamp: Raw timestamp string from the report (ms or s epoch, or
                   date string). May be None.
        max_age: A timestring-compatible age string (e.g. "12h ago") or a
                 timestring.Date instance, as returned by yaml_field().

    Returns:
        True if the report is expired, False otherwise.
    """
    normalized = normalize_timestamp(timestamp)
    if not normalized:
        return False

    if normalized.isdigit():
        # Use datetime.fromtimestamp() for pure Unix epoch integers;
        # timestring.Date() cannot reliably parse them.
        report_dt = datetime.fromtimestamp(int(normalized), tz=timezone.utc)
        cutoff_dt = Date(max_age).date
        # Date.date may be naive; normalise to UTC for comparison
        if cutoff_dt.tzinfo is None:
            cutoff_dt = cutoff_dt.replace(tzinfo=timezone.utc)
        return report_dt < cutoff_dt

    return Date(normalized) < max_age


class BaseLanguageProcessor:
    def __init__(self, *args, **kwargs) -> None:
        pass

    def matches_content(self, content: Any, first_line: str, name: str) -> bool:
        """
        Determines whether this processor is capable of processing this file.

        This is meant to be a high-level verification, and should not go through the whole file
        to extensively check if everything is correct.

        One example here is to check something on the first line, or check if a
        certain key is present at the top-level json and has the right type of value under
        it. Or maybe if a certain set ot XML tags that are unique to this format are here.

        As long as this file can make sure to not accidentally try to parse formats that
        belong with other processors, it is not a big deal (for now)

        Args:
            content (Any): The actual report content
            first_line (str): The first line of the report, as a string
            name (str): The filename of the report (as provided by the upload)
        Returns:
            bool: True if we can read this file, False otherwise
        """
        return False

    def process(self, content: Any, report_builder_session: ReportBuilderSession):
        """
        Processes a report uploaded by the user, appending coverage information
        to the provided `ReportBuilderSession`.

        Raises:
            ReportExpiredException: If the report is considered expired
        """
        pass
