import pytest

from services.processing.merging import compare_reports
from shared.reports.reportfile import ReportFile
from shared.reports.resources import Report
from shared.reports.types import ReportLine
from shared.utils.sessions import Session, SessionType


def _create_report(session_id=0, filename="file.py", lines=None, flags=None):
    """Helper to create a report with specified parameters."""
    report = Report()
    session = Session(
        id=session_id, flags=flags or [], session_type=SessionType.uploaded
    )
    report.add_session(session, use_id_from_session=True)

    if lines:
        report_file = ReportFile(filename)
        for line_num, coverage, sessions in lines:
            report_file.append(line_num, ReportLine.create(coverage, None, sessions))
        report.append(report_file)

    return report


@pytest.mark.unit
@pytest.mark.parametrize(
    "report1_params, report2_params",
    [
        # Identical simple reports
        (
            {"session_id": 0, "filename": "file.py", "lines": [(1, 1, [[0, 1]])]},
            {"session_id": 0, "filename": "file.py", "lines": [(1, 1, [[0, 1]])]},
        ),
        # Empty reports
        (
            {"session_id": 0, "filename": "file.py", "lines": []},
            {"session_id": 0, "filename": "file.py", "lines": []},
        ),
        # Multiple lines with different coverage types
        (
            {
                "session_id": 0,
                "filename": "module.py",
                "lines": [
                    (1, 1, [[0, 1]]),
                    (2, 0, [[0, 0]]),
                    (3, "1/2", [[0, "1/2", ["exit"]]]),
                ],
            },
            {
                "session_id": 0,
                "filename": "module.py",
                "lines": [
                    (1, 1, [[0, 1]]),
                    (2, 0, [[0, 0]]),
                    (3, "1/2", [[0, "1/2", ["exit"]]]),
                ],
            },
        ),
        # Reports with flags
        (
            {
                "session_id": 0,
                "filename": "file.py",
                "lines": [(1, 1, [[0, 1]])],
                "flags": ["unit"],
            },
            {
                "session_id": 0,
                "filename": "file.py",
                "lines": [(1, 1, [[0, 1]])],
                "flags": ["unit"],
            },
        ),
    ],
)
def test_compare_reports_identical_succeeds(report1_params, report2_params):
    """Test that comparing identical reports does not raise an exception."""
    report1 = _create_report(**report1_params)
    report2 = _create_report(**report2_params)

    # Should not raise
    compare_reports(report1, report2)


@pytest.mark.unit
@pytest.mark.parametrize(
    "report1_params, report2_params, expected_error",
    [
        # Different session IDs
        (
            {"session_id": 0, "filename": "file.py", "lines": [(1, 1, [[0, 1]])]},
            {"session_id": 1, "filename": "file.py", "lines": [(1, 1, [[1, 1]])]},
            "Session IDs differ",
        ),
        # Different file lists
        (
            {"session_id": 0, "filename": "file1.py", "lines": [(1, 1, [[0, 1]])]},
            {"session_id": 0, "filename": "file2.py", "lines": [(1, 1, [[0, 1]])]},
            "File lists differ",
        ),
        # Different line numbers
        (
            {"session_id": 0, "filename": "file.py", "lines": [(1, 1, [[0, 1]])]},
            {"session_id": 0, "filename": "file.py", "lines": [(2, 1, [[0, 1]])]},
            "Line numbers differ",
        ),
        # Different coverage values
        (
            {"session_id": 0, "filename": "file.py", "lines": [(1, 1, [[0, 1]])]},
            {"session_id": 0, "filename": "file.py", "lines": [(1, 0, [[0, 0]])]},
            "differ",
        ),
        # Different branches
        (
            {
                "session_id": 0,
                "filename": "file.py",
                "lines": [(1, "1/2", [[0, "1/2", ["exit"]]])],
            },
            {
                "session_id": 0,
                "filename": "file.py",
                "lines": [(1, "1/2", [[0, "1/2", ["1"]]])],
            },
            "Line coverage differs",
        ),
        # Different report totals (different coverage)
        (
            {
                "session_id": 0,
                "filename": "file.py",
                "lines": [(1, 1, [[0, 1]]), (2, 1, [[0, 1]])],
            },
            {
                "session_id": 0,
                "filename": "file.py",
                "lines": [(1, 1, [[0, 1]]), (2, 0, [[0, 0]])],
            },
            "Report totals differ",
        ),
    ],
)
def test_compare_reports_differences_raise_error(
    report1_params, report2_params, expected_error
):
    """Test that comparing different reports raises ValueError with appropriate message."""
    report1 = _create_report(**report1_params)
    report2 = _create_report(**report2_params)

    with pytest.raises(ValueError) as exc_info:
        compare_reports(report1, report2)

    assert expected_error in str(exc_info.value)
