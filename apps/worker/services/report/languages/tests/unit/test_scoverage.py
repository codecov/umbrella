import xml.etree.ElementTree as etree

from services.report.languages import scoverage
from shared.reports.test_utils import convert_report_to_better_readable

from . import create_report_builder_session

# Old internal format used for scoverage-data/scoverage.coverage.xml
xml = """<?xml version="1.0" ?>
<statements>
    <statement>
        <source>source.scala</source>
        <line>1</line>
        <branch>false</branch>
        <invocation-count>1</invocation-count>
    </statement>
    <statement>
        <source>source.scala</source>
        <line>2</line>
        <branch>true</branch>
        <invocation-count>0</invocation-count>
        <ignored>false</ignored>
    </statement>
    <statement>
        <source>source.scala</source>
        <line>2</line>
        <branch>true</branch>
        <invocation-count>0</invocation-count>
        <ignored>false</ignored>
    </statement>
    <statement>
        <source>source.scala</source>
        <line>10</line>
        <branch>true</branch>
        <invocation-count>0</invocation-count>
        <ignored>true</ignored>
    </statement>
    <statement>
        <source>ignore</source>
        <line>1</line>
        <branch>false</branch>
        <invocation-count>0</invocation-count>
    </statement>
    <statement>
        <source>source.scala</source>
        <line>3</line>
        <branch>false</branch>
        <invocation-count>0</invocation-count>
    </statement>
    <statement>
        <source>ignore</source>
        <line>1</line>
        <branch>false</branch>
        <invocation-count>0</invocation-count>
    </statement>
</statements>
"""

# This is the XML report format for SCoverage reports under scoverage-report/scoverage.xml
# Ref : https://github.com/scoverage/scalac-scoverage-plugin/blob/da4ca495dde236d6b52dd5e823a7d87324c74e57/reporter/src/main/scala/scoverage/reporter/ScoverageXmlWriter.scala#L73
trueformat_xml = """
<scoverage 
statement-count="44" statements-invoked="18" statement-rate="40.91" branch-rate="25.00" version="1.0" timestamp="1740027250469">
	<packages>
		<package name="&lt;empty&gt;" statement-count="44" statements-invoked="18" statement-rate="40.91">
			<classes>
				<class 
                name="CoverageClass" filename="CoverageClass.scala" statement-count="44" statements-invoked="18" statement-rate="40.91" branch-rate="25.00">
					<methods>
						<method 
                        name="&lt;empty&gt;/CoverageClass/test" statement-count="26" statements-invoked="8" statement-rate="30.77" branch-rate="20.00">
							<statements>
								<statement 
                                package="tests" class="CoverageClass" class-type="Class" full-class-name="CoverageClass" source="/src/main/scala/CoverageClass.scala" method="test" start="239" end="248" line="5" branch="false" invocation-count="0" ignored="false"></statement>
								<statement 
                                package="tests" class="CoverageClass" class-type="Class" full-class-name="CoverageClass" source="/src/main/scala/CoverageClass.scala" method="test" start="377" end="386" line="10" branch="false" invocation-count="0" ignored="false"></statement>
								<statement 
                                package="tests" class="CoverageClass" class-type="Class" full-class-name="CoverageClass" source="/src/main/scala/CoverageClass.scala" method="test" start="506" end="532" line="19" branch="false" invocation-count="1" ignored="false"></statement>
								<statement 
                                package="tests" class="CoverageClass" class-type="Class" full-class-name="CoverageClass" source="/src/main/scala/CoverageClass.scala" method="test" start="258" end="271" line="5" branch="true" invocation-count="0" ignored="false"></statement>
								<statement 
                                package="tests" class="CoverageClass" class-type="Class" full-class-name="CoverageClass" source="/src/main/scala/CoverageClass.scala" method="test" start="123" end="132" line="4" branch="false" invocation-count="0" ignored="false"></statement>
								<statement 
                                package="tests" class="CoverageClass" class-type="Class" full-class-name="CoverageClass" source="/src/main/scala/CoverageClass.scala" method="test" start="428" end="441" line="13" branch="false" invocation-count="1" ignored="false"></statement>
								<statement 
                                package="tests" class="CoverageClass" class-type="Class" full-class-name="CoverageClass" source="/src/main/scala/CoverageClass.scala" method="test" start="205" end="214" line="5" branch="false" invocation-count="1" ignored="false"></statement>
								<statement 
                                package="tests" class="CoverageClass" class-type="Class" full-class-name="CoverageClass" source="/src/main/scala/CoverageClass.scala" method="test" start="318" end="327" line="7" branch="false" invocation-count="1" ignored="false"></statement>
								<statement 
                                package="tests" class="CoverageClass" class-type="Class" full-class-name="CoverageClass" source="/src/main/scala/CoverageClass.scala" method="test" start="297" end="303" line="6" branch="false" invocation-count="1" ignored="false"></statement>
								<statement 
                                package="tests" class="CoverageClass" class-type="Class" full-class-name="CoverageClass" source="/src/main/scala/CoverageClass.scala" method="test" start="190" end="196" line="5" branch="false" invocation-count="1" ignored="false"></statement>
								<statement 
                                package="tests" class="CoverageClass" class-type="Class" full-class-name="CoverageClass" source="/src/main/scala/CoverageClass.scala" method="test" start="227" end="233" line="5" branch="false" invocation-count="0" ignored="false"></statement>
								<statement 
                                package="tests" class="CoverageClass" class-type="Class" full-class-name="CoverageClass" source="/src/main/scala/CoverageClass.scala" method="test" start="354" end="360" line="9" branch="false" invocation-count="0" ignored="true"></statement>
								<statement 
                                package="tests" class="CoverageClass" class-type="Class" full-class-name="CoverageClass" source="/src/main/scala/CoverageClass.scala" method="test" start="351" end="500" line="9" branch="true" invocation-count="0" ignored="true"></statement>
								<statement 
                                package="tests" class="CoverageClass" class-type="Class" full-class-name="CoverageClass" source="/src/main/scala/CoverageClass.scala" method="test" start="258" end="271" line="5" branch="false" invocation-count="0" ignored="false"></statement>
								<statement 
                                package="tests" class="CoverageClass" class-type="Class" full-class-name="CoverageClass" source="/src/main/scala/CoverageClass.scala" method="test" start="98" end="107" line="4" branch="false" invocation-count="1" ignored="false"></statement>
								<statement 
                                package="tests" class="CoverageClass" class-type="Class" full-class-name="CoverageClass" source="/src/main/scala/CoverageClass.scala" method="test" start="239" end="248" line="5" branch="true" invocation-count="0" ignored="false"></statement>
								<statement 
                                package="tests" class="CoverageClass" class-type="Class" full-class-name="CoverageClass" source="/src/main/scala/CoverageClass.scala" method="test" start="377" end="386" line="10" branch="true" invocation-count="0" ignored="false"></statement>
								<statement 
                                package="tests" class="CoverageClass" class-type="Class" full-class-name="CoverageClass" source="/src/main/scala/CoverageClass.scala" method="test" start="224" end="273" line="5" branch="true" invocation-count="0" ignored="false"></statement>
								<statement 
                                package="tests" class="CoverageClass" class-type="Class" full-class-name="CoverageClass" source="/src/main/scala/CoverageClass.scala" method="test" start="148" end="161" line="4" branch="false" invocation-count="0" ignored="false"></statement>
								<statement 
                                package="tests" class="CoverageClass" class-type="Class" full-class-name="CoverageClass" source="/src/main/scala/CoverageClass.scala" method="test" start="428" end="441" line="13" branch="true" invocation-count="1" ignored="false"></statement>
								<statement 
                                package="tests" class="CoverageClass" class-type="Class" full-class-name="CoverageClass" source="/src/main/scala/CoverageClass.scala" method="test" start="318" end="327" line="7" branch="true" invocation-count="1" ignored="false"></statement>
								<statement 
                                package="tests" class="CoverageClass" class-type="Class" full-class-name="CoverageClass" source="/src/main/scala/CoverageClass.scala" method="test" start="205" end="214" line="5" branch="true" invocation-count="1" ignored="false"></statement>
							</statements>
						</method>
					</methods>
				</class>
			</classes>
		</package>
	</packages>
</scoverage>
"""


def fixes(path):
    if path == "ignore":
        return None
    return path


class TestSCoverage:
    def test_report(self):
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

    def test_true_format_report(self):
        report_builder_session = create_report_builder_session(path_fixer=fixes)
        scoverage.from_xml(etree.fromstring(trueformat_xml), report_builder_session)
        report = report_builder_session.output_report()
        processed_report = convert_report_to_better_readable(report)

        assert processed_report["archive"] == {
            '/src/main/scala/CoverageClass.scala': [(4, '1/3', 'b', [[0, '1/3']], None, None),
                                                    (5, '3/9', 'b', [[0, '3/9']], None, None),
                                                    (6, 1, None, [[0, 1]], None, None),
                                                    (7, 2, None, [[0, 2]], None, None),
                                                    # 9 is completely ignored and should not be present
                                                    # 10 is completely not covered and should be present as a branch
                                                    (10, '0/2', 'b', [[0, '0/2']], None, None),
                                                    # 13 is branched but fully covered, so it should not be a branch
                                                    (13, 2, None, [[0, 2]], None, None),
                                                    (19, 1, None, [[0, 1]], None, None)]
        }
