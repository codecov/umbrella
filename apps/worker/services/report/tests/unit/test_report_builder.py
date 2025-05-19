from services.report.report_builder import CoverageType, ReportBuilder
from shared.reports.reportfile import ReportFile
from shared.reports.types import ReportLine


def test_report_builder_generate_session(mocker):
    current_yaml, sessionid, ignored_lines, path_fixer = (
        mocker.MagicMock(),
        mocker.MagicMock(),
        mocker.MagicMock(),
        mocker.MagicMock(),
    )
    filepath = "filepath"
    builder = ReportBuilder(current_yaml, sessionid, ignored_lines, path_fixer)
    builder_session = builder.create_report_builder_session(filepath)
    assert builder_session.path_fixer == path_fixer


def test_report_builder_session(mocker):
    current_yaml, sessionid, ignored_lines, path_fixer = (
        {"beta_groups": ["labels"]},
        mocker.MagicMock(),
        mocker.MagicMock(),
        mocker.MagicMock(),
    )
    filepath = "filepath"
    builder = ReportBuilder(current_yaml, sessionid, ignored_lines, path_fixer)
    builder_session = builder.create_report_builder_session(filepath)
    first_file = ReportFile("filename.py")
    first_file.append(2, ReportLine.create(0))
    first_file.append(3, ReportLine.create(0))
    first_file.append(10, ReportLine.create(1, sessions=[(0, 1)]))
    builder_session.append(first_file)
    final_report = builder_session.output_report()
    assert final_report.files == ["filename.py"]
    assert sorted(final_report.get("filename.py").lines) == [
        (2, ReportLine.create(0)),
        (3, ReportLine.create(0)),
        (10, ReportLine.create(1, sessions=[(0, 1)])),
    ]


def test_report_builder_session_only_all_labels(mocker):
    current_yaml, sessionid, ignored_lines, path_fixer = (
        {},
        mocker.MagicMock(),
        mocker.MagicMock(),
        mocker.MagicMock(),
    )
    filepath = "filepath"
    builder = ReportBuilder(current_yaml, sessionid, ignored_lines, path_fixer)
    builder_session = builder.create_report_builder_session(filepath)
    first_file = ReportFile("filename.py")
    first_file.append(2, ReportLine.create(0))
    first_file.append(3, ReportLine.create(0))
    first_file.append(10, ReportLine.create(1, sessions=[(0, 1)]))
    builder_session.append(first_file)
    final_report = builder_session.output_report()
    assert final_report.files == ["filename.py"]
    assert sorted(final_report.get("filename.py").lines) == [
        (2, ReportLine.create(0)),
        (3, ReportLine.create(0)),
        (10, ReportLine.create(1, sessions=[(0, 1)])),
    ]


def test_report_builder_session_create_line(mocker):
    current_yaml, sessionid, ignored_lines, path_fixer = (
        {
            "flag_management": {
                "default_rules": {
                    "carryforward": "true",
                    "carryforward_mode": "labels",
                }
            }
        },
        45,
        mocker.MagicMock(),
        mocker.MagicMock(),
    )
    filepath = "filepath"
    builder = ReportBuilder(current_yaml, sessionid, ignored_lines, path_fixer)
    builder_session = builder.create_report_builder_session(filepath)
    line = builder_session.create_coverage_line(1, CoverageType.branch)
    assert line == ReportLine.create(1, type="b", sessions=[(45, 1)])
