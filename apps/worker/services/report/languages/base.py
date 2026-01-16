from typing import Any

from services.report.report_builder import ReportBuilderSession


def normalize_timestamp(timestamp: str | None) -> str | None:
    """
    Normalize a timestamp string for use with timestring.Date().

    Handles millisecond Unix timestamps (13+ digits) by converting them to
    seconds, since timestring.Date() expects seconds-based timestamps.

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
        # Convert milliseconds to seconds for timestring.Date()
        return str(int(timestamp) // 1000)

    return timestamp


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
