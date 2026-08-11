import sentry_sdk
from lxml.etree import Element

from services.report.languages.base import BaseLanguageProcessor
from services.report.report_builder import ReportBuilderSession
from shared.reports.resources import ReportFile


class VbProcessor(BaseLanguageProcessor):
    def matches_content(self, content: Element, first_line: str, name: str) -> bool:
        return content.tag == "results"

    @sentry_sdk.trace
    def process(
        self, content: Element, report_builder_session: ReportBuilderSession
    ) -> None:
        return from_xml(content, report_builder_session)


def from_xml(xml: Element, report_builder_session: ReportBuilderSession) -> None:
    files: dict[str, ReportFile] = {}
    for module in xml.iter("module"):
        # loop through sources
        for sf in module.iter("source_file"):
            _file = report_builder_session.create_coverage_file(
                sf.attrib["path"].replace("\\", "/")
            )
            if _file is not None:
                files[sf.attrib["id"]] = _file

        # loop through each line
        for line in module.iter("range"):
            attr = line.attrib
            _file = files.get(attr["source_id"])
            if _file is None:
                continue

            cov_txt = attr["covered"]
            coverage = 1 if cov_txt == "yes" else 0 if cov_txt == "no" else True
            start_line = int(attr["start_line"])
            end_line = int(attr["end_line"])
            # Record start and end lines of each range rather than every individual
            # line. All lines in a VB range share the same coverage value, so
            # expanding every line is unnecessary and very slow for large .NET
            # codebases with wide line ranges.
            _file.append(
                start_line,
                report_builder_session.create_coverage_line(coverage),
            )
            if end_line != start_line:
                _file.append(
                    end_line,
                    report_builder_session.create_coverage_line(coverage),
                )

    # add files
    for _file in files.values():
        report_builder_session.append(_file)
