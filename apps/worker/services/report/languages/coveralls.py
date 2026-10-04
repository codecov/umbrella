import orjson
import sentry_sdk

from services.report.languages.base import BaseLanguageProcessor
from services.report.report_builder import ReportBuilderSession


class CoverallsProcessor(BaseLanguageProcessor):
    def matches_content(self, content: dict, first_line: str, name: str) -> bool:
        source_files = content.get("source_files")
        if not source_files or not isinstance(source_files, list):
            return False
        first = source_files[0]
        return "name" in first and "coverage" in first

    @sentry_sdk.trace
    def process(
        self, content: dict, report_builder_session: ReportBuilderSession
    ) -> None:
        return from_json(content, report_builder_session)


def from_json(report: dict, report_builder_session: ReportBuilderSession) -> None:
    for file in report["source_files"]:
        name = file.get("name")
        coverage = file.get("coverage")
        if name is None or coverage is None:
            continue

        _file = report_builder_session.create_coverage_file(name)
        if _file is None:
            continue

        # for some reason, the `coverage` field is either a list directly,
        # or a string with a json-encoded list.
        if isinstance(coverage, str):
            coverage = orjson.loads(coverage)

        for ln, cov in enumerate(coverage, start=1):
            if cov is not None:
                _line = report_builder_session.create_coverage_line(cov)
                _file.append(ln, _line)
        report_builder_session.append(_file)
