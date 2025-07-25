import xml.etree.ElementTree as etree

from services.report.languages import scoverage
from shared.reports.test_utils import convert_report_to_better_readable

from . import create_report_builder_session

xml = """<?xml version="1.0" ?>
<statements>
    <statement>
        <source>source.scala</source>
        <line>1</line>
        <branch>false</branch>
        <count>1</count>
    </statement>
    <statement>
        <source>source.scala</source>
        <line>2</line>
        <branch>true</branch>
        <count>0</count>
        <ignored>false</ignored>
    </statement>
    <statement>
        <source>source.scala</source>
        <line>10</line>
        <branch>true</branch>
        <count>0</count>
        <ignored>true</ignored>
    </statement>
    <statement>
        <source>ignore</source>
        <line>1</line>
        <branch>false</branch>
        <count>0</count>
    </statement>
    <statement>
        <source>source.scala</source>
        <line>3</line>
        <branch>false</branch>
        <count>0</count>
    </statement>
    <statement>
        <source>ignore</source>
        <line>1</line>
        <branch>false</branch>
        <count>0</count>
    </statement>
</statements>
"""


class TestSCoverage:
    def test_report(self):
        def fixes(path):
            if path == "ignore":
                return None
            return path

        report_builder_session = create_report_builder_session(path_fixer=fixes)
        scoverage.from_xml(etree.fromstring(xml), report_builder_session)
        report = report_builder_session.output_report()
        processed_report = convert_report_to_better_readable(report)

        assert processed_report["archive"] == {
            "source.scala": [
                (1, 1, None, [[0, 1]], None, None),
                (2, "0/2", "b", [[0, "0/2"]], None, None),
                (3, 0, None, [[0, 0]], None, None),
            ]
        }
