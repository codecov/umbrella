import re
import typing

from shared.reports.resources import Report

from worker_services.report.languages.base import BaseLanguageProcessor
from worker_services.report.report_builder import (
    CoverageType,
    ReportBuilder,
    ReportBuilderSession,
)


class LuaProcessor(BaseLanguageProcessor):
    def matches_content(self, content: bytes, first_line, name):
        return detect(content)

    def process(
        self, name: str, content: typing.Any, report_builder: ReportBuilder
    ) -> Report:
        report_builder_session = report_builder.create_report_builder_session(name)
        return from_txt(content, report_builder_session)


docs = re.compile(r"^=+\n", re.M).split


def detect(report: bytes):
    return report[:7] == b"======="


def from_txt(string: bytes, report_builder_session: ReportBuilderSession) -> Report:
    filename = None
    ignored_lines = report_builder_session.ignored_lines
    for string in docs(string.decode(errors="replace").replace("\t", " ")):
        string = string.rstrip()
        if string == "Summary":
            filename = None
            continue

        elif string.endswith((".lua", ".lisp")):
            filename = report_builder_session.path_fixer(string)
            if filename is None:
                continue

        elif filename:
            _file = report_builder_session.file_class(
                filename, ignore=ignored_lines.get(filename)
            )
            for ln, source in enumerate(string.splitlines(), start=1):
                try:
                    cov = source.strip().split(" ")[0]
                    cov = 0 if cov[-2:] in ("*0", "0") else int(cov)
                    _file[ln] = report_builder_session.create_coverage_line(
                        filename=filename, coverage=cov, coverage_type=CoverageType.line
                    )

                except Exception:
                    pass

            report_builder_session.append(_file)

    return report_builder_session.output_report()
