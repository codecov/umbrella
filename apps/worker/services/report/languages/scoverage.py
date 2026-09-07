import sentry_sdk
from lxml.etree import Element

from services.report.report_builder import CoverageType, ReportBuilderSession

from .base import BaseLanguageProcessor
from .helpers import child_text


def get_child_value(element: Element, tag: str) -> str:
    """
    Returns the text of the child element with the given tag if it's a child of the given element.
    Else if it's an attribute returns the attribute value instead
    Else returns None
    """
    value = child_text(element, tag)
    # If the child is not an empty string return  (It can't be None)
    if value:
        return value
    return element.attrib.get(tag, "")


class SCoverageProcessor(BaseLanguageProcessor):
    def matches_content(self, content: Element, first_line: str, name: str) -> bool:
        return content.tag == "scoverage" or content.tag == "statements"

    @sentry_sdk.trace
    def process(
            self, content: Element, report_builder_session: ReportBuilderSession
    ) -> None:
        return from_xml(content, report_builder_session)


def from_xml(xml: Element, report_builder_session: ReportBuilderSession) -> None:
    files: dict[str, dict[int, list[Element]]] = {}
    partials_as_hits = report_builder_session.yaml_field(("parsers", "scoverage", "partials_as_hits"),
                                                         False)

    for statement in xml.iter("statement"):
        filename = get_child_value(statement, "source")
        files.setdefault(filename, {}).setdefault(int(get_child_value(statement, "line")), [])
        files[filename][int(get_child_value(statement, "line"))].append(statement)

    for filename, lines in files.items():
        _file = report_builder_session.create_coverage_file(filename)
        if _file is None:
            continue
        for line, statements in lines.items():
            included_statements = [stmt for stmt in statements if get_child_value(stmt, "ignored") != "true"]
            if len(included_statements) == 0:
                continue
            covered_statements = [stmt for stmt in included_statements if
                                  int(get_child_value(stmt, "invocation-count")) > 0]

            is_partial = len(included_statements) > 1 and len(covered_statements) < len(included_statements)

            # CodeCov takes a maxint of 99999 as per shared.helpers.numeric.maxint
            covered_statements_cnt = min(len(covered_statements), 99999)

            # Set to covered ratio if partial coverage, else set to number of invocations of the line.
            # Since Scoverage does not provide line invocations, we set to num of covered statements as an approximation
            # This is similar to Jacoco reports setting covered instructions as the cov value for fully covered lines
            cov = f"{covered_statements_cnt}/{len(included_statements)}" if is_partial else covered_statements_cnt

            # When partials are set as hits, we want to mark all partially covered lines as covered
            if is_partial and partials_as_hits:
                cov = 1

            # We only mark it as a branch if it's a partial coverage
            coverage_type = CoverageType.branch if is_partial else CoverageType.line
            _file.append(line, report_builder_session.create_coverage_line(cov, coverage_type))

        report_builder_session.append(_file)

