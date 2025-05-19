from fractions import Fraction
from pathlib import Path

import orjson
import pytest

from shared.reports.editable import EditableReport, EditableReportFile
from shared.reports.resources import ReportFile, Session
from shared.reports.test_utils import convert_report_to_better_readable
from shared.reports.types import ReportLine, ReportTotals
from shared.utils.merge import merge_coverage
from shared.utils.sessions import SessionType

current_file = Path(__file__)


def create_sample_line(*, coverage, sessionid=None):
    return ReportLine.create(coverage, sessions=[(sessionid, coverage)])


def test_merge_coverage():
    assert merge_coverage(0.5, Fraction(3, 4)) == Fraction(3, 4)
    assert merge_coverage(2, Fraction(3, 4)) == 2


def test_change_sessionid():
    line = ReportLine.create(1, sessions=[(0, 1)])
    file = EditableReportFile(name="foo.rs")
    file.append(1, line)
    report = EditableReport()
    report.append(file)
    session = Session(0)
    report.add_session(session, use_id_from_session=True)

    report.change_sessionid(0, 123)

    def assert_sessionid(report: EditableReport, id: int):
        assert 0 not in report.sessions
        assert id in report.sessions

        file = report.get("foo.rs")
        assert file.details["present_sessions"] == [id]
        line = file.get(1)
        assert line.sessions[0].id == id

    assert_sessionid(report, 123)

    # also assert a serialization roundtrip:
    report_json, chunks, _totals = report.serialize()
    report_json = orjson.loads(report_json)

    report = EditableReport(
        files=report_json["files"],
        sessions=report_json["sessions"],
        chunks=chunks.decode(),
    )
    assert_sessionid(report, 123)

    report.change_sessionid(123, 234)
    assert_sessionid(report, 234)


class TestEditableReportHelpers:
    def test_line_without_session(self):
        line = ReportLine.create(1, None, [(1, 0), (0, 1)])
        assert EditableReportFile.line_without_multiple_sessions(
            line, {1}
        ) == ReportLine.create(1, None, [(0, 1)])
        assert EditableReportFile.line_without_multiple_sessions(
            line, {0}
        ) == ReportLine.create(0, None, [(1, 0)])
        assert (
            EditableReportFile.line_without_multiple_sessions(
                EditableReportFile.line_without_multiple_sessions(line, {0}), {1}
            )
            == ""
        )


class TestEditableReportFile:
    def test_init(self):
        chunks = "\n".join(
            [
                "{}",
                "[1, null, [[0, 1], [1, 0]]]",
                "",
                "",
                "[0, null, [[0, 0], [1, 0]]]",
                "[1, null, [[0, 1], [1, 1]]]",
                "[1, null, [[0, 0], [1, 1]]]",
                "",
                "",
                '[1, null, [[0, 1], [1, "1/2"]]]',
                '[1, null, [[0, "1/2"], [1, 1]]]',
                "",
                "",
                "[1, null, [[0, 1]]]",
                "[1, null, [[1, 1]]]",
                '["1/2", null, [[0, "1/2"], [1, 0]]]',
                '["1/2", null, [[0, 0], [1, "1/2"]]]',
            ]
        )
        report_file = EditableReportFile(name="file.py", lines=chunks)
        expected_result = [
            (1, ReportLine.create(1, sessions=[(0, 1), (1, 0)])),
            (4, ReportLine.create(0, sessions=[(0, 0), (1, 0)])),
            (5, ReportLine.create(1, sessions=[(0, 1), (1, 1)])),
            (6, ReportLine.create(1, sessions=[(0, 0), (1, 1)])),
            (9, ReportLine.create(1, sessions=[(0, 1), (1, "1/2")])),
            (10, ReportLine.create(1, sessions=[(0, "1/2"), (1, 1)])),
            (13, ReportLine.create(1, sessions=[(0, 1)])),
            (14, ReportLine.create(1, sessions=[(1, 1)])),
            (15, ReportLine.create("1/2", sessions=[(0, "1/2"), (1, 0)])),
            (16, ReportLine.create("1/2", sessions=[(0, 0), (1, "1/2")])),
        ]
        assert list(report_file.lines) == expected_result

    def test_merge_not_previously_set_sessions_header(self):
        chunks = "\n".join(
            [
                "{}",
                "[1, null, [[0, 1], [1, 0]]]",
                "",
                "",
                "[0, null, [[0, 0], [1, 0]]]",
                "[1, null, [[0, 1], [1, 1]]]",
                "[1, null, [[0, 0], [1, 1]]]",
                "",
                "",
                '[1, null, [[0, 1], [1, "1/2"]]]',
                '[1, null, [[0, "1/2"], [1, 1]]]',
                "",
                "",
                "[1, null, [[0, 1]]]",
                "[1, null, [[1, 1]]]",
                '["1/2", null, [[0, "1/2"], [1, 0]]]',
                '["1/2", null, [[0, 0], [1, "1/2"]]]',
            ]
        )
        report_file = EditableReportFile(name="file.py", lines=chunks)
        assert report_file.details == {"present_sessions": [0, 1]}

        new_chunks = "\n".join(
            [
                "{}",
                "[1, null, [[0, 1], [2, 0]]]",
                "",
                "",
                "[0, null, [[2, 0], [1, 0]]]",
                "[1, null, [[2, 1], [3, 1]]]",
                "[1, null, [[0, 0], [1, 1]]]",
                "",
                "",
                '[1, null, [[0, 1], [1, "1/2"]]]',
                '[1, null, [[0, "1/2"], [1, 1]]]',
                "",
                "",
                "[1, null, [[0, 1]]]",
                "[1, null, [[1, 1]]]",
                '["1/2", null, [[0, "1/2"], [1, 0]]]',
                '["1/2", null, [[2, 0], [1, "1/2"]]]',
            ]
        )
        new_report_file = ReportFile(name="file.py", lines=new_chunks)
        report_file.merge(new_report_file)

        assert report_file.details == {"present_sessions": [0, 1, 2, 3]}

    def test_details(self):
        chunks = "\n".join(['{"some_field": "nah"}', "[1, null, [[0, 1], [1, 0]]]", ""])
        report_file = EditableReportFile(name="file.py", lines=chunks)
        assert report_file.details == {"some_field": "nah", "present_sessions": [0, 1]}

    def test_merge_already_previously_set_sessions_header(self):
        chunks = "\n".join(
            [
                '{"present_sessions":[0,1]}',
                "[1, null, [[0, 1], [1, 0]]]",
                "",
                "",
                "[0, null, [[0, 0], [1, 0]]]",
                "[1, null, [[0, 1], [1, 1]]]",
                "[1, null, [[0, 0], [1, 1]]]",
                "",
                "",
                '[1, null, [[0, 1], [1, "1/2"]]]',
                '[1, null, [[0, "1/2"], [1, 1]]]',
                "",
                "",
                "[1, null, [[0, 1]]]",
                "[1, null, [[1, 1]]]",
                '["1/2", null, [[0, "1/2"], [1, 0]]]',
                '["1/2", null, [[0, 0], [1, "1/2"]]]',
            ]
        )
        report_file = EditableReportFile(name="file.py", lines=chunks)
        assert report_file.details == {"present_sessions": [0, 1]}
        new_chunks = "\n".join(
            [
                "{}",
                "[1, null, [[0, 1], [2, 0]]]",
                "",
                "",
                "[0, null, [[2, 0], [1, 0]]]",
                "[1, null, [[2, 1], [3, 1]]]",
                "[1, null, [[0, 0], [1, 1]]]",
                "",
                "",
                '[1, null, [[0, 1], [1, "1/2"]]]',
                '[1, null, [[0, "1/2"], [1, 1]]]',
                "",
                "",
                "[1, null, [[0, 1]]]",
                "[1, null, [[1, 1]]]",
                '["1/2", null, [[0, "1/2"], [1, 0]]]',
                '["1/2", null, [[2, 0], [1, "1/2"]]]',
            ]
        )
        new_report_file = ReportFile(name="file.py", lines=new_chunks)
        report_file.merge(new_report_file)
        assert report_file.details == {"present_sessions": [0, 1, 2, 3]}

    def test_delete_session(self):
        chunks = "\n".join(
            [
                "{}",
                "[1, null, [[0, 1], [1, 0]]]",
                "",
                "",
                "[0, null, [[0, 0], [1, 0]]]",
                "[1, null, [[0, 1], [1, 1]]]",
                "[1, null, [[0, 0], [1, 1]]]",
                "",
                "",
                '[1, null, [[0, 1], [1, "1/2"]]]',
                '[1, null, [[0, "1/2"], [1, 1]]]',
                "",
                "",
                "[1, null, [[0, 1]]]",
                "[1, null, [[1, 1]]]",
                '["1/2", null, [[0, "1/2"], [1, 0]]]',
                '["1/2", null, [[0, 0], [1, "1/2"]]]',
            ]
        )
        report_file = EditableReportFile(name="file.py", lines=chunks)
        assert list(report_file.lines) == [
            (1, ReportLine.create(1, sessions=[(0, 1), (1, 0)])),
            (4, ReportLine.create(0, sessions=[(0, 0), (1, 0)])),
            (5, ReportLine.create(1, sessions=[(0, 1), (1, 1)])),
            (6, ReportLine.create(1, sessions=[(0, 0), (1, 1)])),
            (9, ReportLine.create(1, sessions=[(0, 1), (1, "1/2")])),
            (10, ReportLine.create(1, sessions=[(0, "1/2"), (1, 1)])),
            (13, ReportLine.create(1, sessions=[(0, 1)])),
            (14, ReportLine.create(1, sessions=[(1, 1)])),
            (15, ReportLine.create("1/2", sessions=[(0, "1/2"), (1, 0)])),
            (16, ReportLine.create("1/2", sessions=[(0, 0), (1, "1/2")])),
        ]
        assert report_file.totals == ReportTotals(
            files=0, lines=10, hits=7, misses=1, partials=2, coverage="70.00000"
        )

        report_file.delete_multiple_sessions({1})

        assert list(report_file.lines) == [
            (1, ReportLine.create(1, sessions=[(0, 1)])),
            (4, ReportLine.create(0, sessions=[(0, 0)])),
            (5, ReportLine.create(1, sessions=[(0, 1)])),
            (6, ReportLine.create(0, sessions=[(0, 0)])),
            (9, ReportLine.create(1, sessions=[(0, 1)])),
            (10, ReportLine.create("1/2", sessions=[(0, "1/2")])),
            (13, ReportLine.create(1, sessions=[(0, 1)])),
            (15, ReportLine.create("1/2", sessions=[(0, "1/2")])),
            (16, ReportLine.create(0, sessions=[(0, 0)])),
        ]

        assert report_file.get(1) == ReportLine.create(1, sessions=[(0, 1)])
        assert report_file.get(13) == ReportLine.create(1, sessions=[(0, 1)])
        assert report_file.get(14) is None
        assert report_file.totals == ReportTotals(
            files=0,
            lines=9,
            hits=4,
            misses=3,
            partials=2,
            coverage="44.44444",
        )

        assert report_file.details == {"present_sessions": [0]}

    def test_delete_session_not_present(self):
        chunks = "\n".join(
            [
                '{"present_sessions":[0,1]}',
                "[1, null, [[0, 1], [1, 0]]]",
                "",
                "",
                "[0, null, [[0, 0], [1, 0]]]",
                "[1, null, [[0, 1], [1, 1]]]",
                "[1, null, [[0, 0], [1, 1]]]",
                "",
                "",
                '[1, null, [[0, 1], [1, "1/2"]]]',
                '[1, null, [[0, "1/2"], [1, 1]]]',
                "",
                "",
                "[1, null, [[0, 1]]]",
                "[1, null, [[1, 1]]]",
                '["1/2", null, [[0, "1/2"], [1, 0]]]',
                '["1/2", null, [[0, 0], [1, "1/2"]]]',
            ]
        )
        report_file = EditableReportFile(name="file.py", lines=chunks)
        original_lines = [
            (1, ReportLine.create(1, sessions=[(0, 1), (1, 0)])),
            (4, ReportLine.create(0, sessions=[(0, 0), (1, 0)])),
            (5, ReportLine.create(1, sessions=[(0, 1), (1, 1)])),
            (6, ReportLine.create(1, sessions=[(0, 0), (1, 1)])),
            (9, ReportLine.create(1, sessions=[(0, 1), (1, "1/2")])),
            (10, ReportLine.create(1, sessions=[(0, "1/2"), (1, 1)])),
            (13, ReportLine.create(1, sessions=[(0, 1)])),
            (14, ReportLine.create(1, sessions=[(1, 1)])),
            (15, ReportLine.create("1/2", sessions=[(0, "1/2"), (1, 0)])),
            (16, ReportLine.create("1/2", sessions=[(0, 0), (1, "1/2")])),
        ]
        assert list(report_file.lines) == original_lines
        report_file.delete_multiple_sessions({3})
        assert list(report_file.lines) == original_lines
        assert report_file.details == {"present_sessions": [0, 1]}

    def test_delete_multiple_sessions(self):
        chunks = "\n".join(
            [
                '{"present_sessions":[0,1,2,3]}',
                "[1, null, [[0, 1], [1, 0]]]",
                "",
                "",
                "[0, null, [[2, 0], [1, 0]]]",
                "[1, null, [[0, 1], [3, 1]]]",
                "[1, null, [[0, 0], [1, 1]]]",
                "",
                "",
                '[1, null, [[0, 1], [1, "1/2"], [2, 1], [3, 1]]]',
                '[1, null, [[0, "1/2"], [1, 1]]]',
                "",
                "",
                "[1, null, [[0, 1]]]",
                "[1, null, [[1, 1]]]",
                '["1/2", null, [[0, "1/2"], [1, 0]]]',
                '["1/2", null, [[0, 0], [1, "1/2"]]]',
            ]
        )
        report_file = EditableReportFile(name="file.py", lines=chunks)

        assert list(report_file.lines) == [
            (1, ReportLine.create(1, sessions=[(0, 1), (1, 0)])),
            (4, ReportLine.create(0, sessions=[(2, 0), (1, 0)])),
            (5, ReportLine.create(1, sessions=[(0, 1), (3, 1)])),
            (6, ReportLine.create(1, sessions=[(0, 0), (1, 1)])),
            (9, ReportLine.create(1, sessions=[(0, 1), (1, "1/2"), (2, 1), (3, 1)])),
            (10, ReportLine.create(1, sessions=[(0, "1/2"), (1, 1)])),
            (13, ReportLine.create(1, sessions=[(0, 1)])),
            (14, ReportLine.create(1, sessions=[(1, 1)])),
            (15, ReportLine.create("1/2", sessions=[(0, "1/2"), (1, 0)])),
            (16, ReportLine.create("1/2", sessions=[(0, 0), (1, "1/2")])),
        ]

        report_file.delete_multiple_sessions({1, 3, 5})

        assert list(report_file.lines) == [
            (1, ReportLine.create(1, sessions=[(0, 1)])),
            (4, ReportLine.create(0, sessions=[(2, 0)])),
            (5, ReportLine.create(1, sessions=[(0, 1)])),
            (6, ReportLine.create(0, sessions=[(0, 0)])),
            (9, ReportLine.create(1, sessions=[(0, 1), (2, 1)])),
            (10, ReportLine.create("1/2", sessions=[(0, "1/2")])),
            (13, ReportLine.create(1, sessions=[(0, 1)])),
            (15, ReportLine.create("1/2", sessions=[(0, "1/2")])),
            (16, ReportLine.create(0, sessions=[(0, 0)])),
        ]

        assert report_file.details == {"present_sessions": [0, 2]}


@pytest.fixture
def sample_report():
    report = EditableReport()
    first_file = EditableReportFile("file_1.go")
    first_file.append(
        1,
        ReportLine.create(1, sessions=[(0, 1), (1, 1), (2, 1)], complexity=(10, 2)),
    )
    first_file.append(
        2,
        ReportLine.create(1, sessions=[(0, 1), (1, "1/2"), (2, 1)], complexity=(10, 2)),
    )
    first_file.append(3, ReportLine.create(1, sessions=[(0, 1)], complexity=(10, 2)))
    first_file.append(4, ReportLine.create(1, sessions=[(1, 1)], complexity=(10, 2)))
    first_file.append(5, ReportLine.create(1, sessions=[(2, 1)], complexity=(10, 2)))
    first_file.append(
        6, ReportLine.create(1, sessions=[(0, 1), (1, 1)], complexity=(10, 2))
    )
    first_file.append(
        7, ReportLine.create(1, sessions=[(1, 1), (2, 1)], complexity=(10, 2))
    )
    first_file.append(
        8, ReportLine.create(1, sessions=[(2, 1), (0, 1)], complexity=(10, 2))
    )
    second_file = EditableReportFile("file_2.py")
    second_file.append(12, ReportLine.create(1, sessions=[(0, 1), (1, "1/2"), (2, 0)]))
    second_file.append(
        51,
        ReportLine.create("1/2", type="b", sessions=[(0, "1/3"), (1, "1/2")]),
    )
    single_session_file = EditableReportFile("single_session_file.c")
    single_session_file.append(101, ReportLine.create("1/2", sessions=[(1, "1/2")]))
    single_session_file.append(110, ReportLine.create(1, sessions=[(1, 1)]))
    single_session_file.append(111, ReportLine.create(0, sessions=[(1, 0)]))
    report.append(first_file)
    report.append(second_file)
    report.append(single_session_file)
    report.add_session(Session(id=0, flags=["unit"]))
    report.add_session(
        Session(id=1, flags=["integration"], session_type=SessionType.carriedforward)
    )
    report.add_session(Session(id=2, flags=None))
    return report


class TestEditableReport:
    @pytest.fixture
    def sample_with_labels_report(self):
        first_report = EditableReport()
        first_report.add_session(
            Session(
                flags=["enterprise"],
                sessionid=0,
                session_type=SessionType.carriedforward,
            )
        )
        first_report.add_session(
            Session(
                flags=["enterprise"], sessionid=1, session_type=SessionType.uploaded
            )
        )
        first_report.add_session(
            Session(
                flags=["unit"], sessionid=2, session_type=SessionType.carriedforward
            )
        )
        first_report.add_session(
            Session(flags=["unrelated"], sessionid=3, session_type=SessionType.uploaded)
        )
        first_file = EditableReportFile("first_file.py")
        c = 0
        for _ in range(5):
            for sessionid in range(4):
                first_file.append(
                    c % 7 + 1, create_sample_line(coverage=c, sessionid=sessionid)
                )
                c += 1
        first_file.append(23, ReportLine.create(1, sessions=[(1, 1)]))
        second_file = EditableReportFile("second_file.py")
        first_report.append(first_file)
        first_report.append(second_file)
        assert convert_report_to_better_readable(first_report)["archive"] == {
            "first_file.py": [
                (1, 14, None, [[0, 0], [3, 7], [2, 14]], None, None),
                (2, 15, None, [[1, 1], [0, 8], [3, 15]], None, None),
                (3, 16, None, [[2, 2], [1, 9], [0, 16]], None, None),
                (4, 17, None, [[3, 3], [2, 10], [1, 17]], None, None),
                (5, 18, None, [[0, 4], [3, 11], [2, 18]], None, None),
                (6, 19, None, [[1, 5], [0, 12], [3, 19]], None, None),
                (7, 13, None, [[2, 6], [1, 13]], None, None),
                (23, 1, None, [[1, 1]], None, None),
            ]
        }

        return first_report

    def test_delete_session(self, sample_report):
        report = sample_report

        assert convert_report_to_better_readable(report) == {
            "archive": {
                "file_1.go": [
                    (1, 1, None, [[0, 1], [1, 1], [2, 1]], None, (10, 2)),
                    (2, 1, None, [[0, 1], [1, "1/2"], [2, 1]], None, (10, 2)),
                    (3, 1, None, [[0, 1]], None, (10, 2)),
                    (4, 1, None, [[1, 1]], None, (10, 2)),
                    (5, 1, None, [[2, 1]], None, (10, 2)),
                    (6, 1, None, [[0, 1], [1, 1]], None, (10, 2)),
                    (7, 1, None, [[1, 1], [2, 1]], None, (10, 2)),
                    (8, 1, None, [[2, 1], [0, 1]], None, (10, 2)),
                ],
                "file_2.py": [
                    (12, 1, None, [[0, 1], [1, "1/2"], [2, 0]], None, None),
                    (51, "1/2", "b", [[0, "1/3"], [1, "1/2"]], None, None),
                ],
                "single_session_file.c": [
                    (101, "1/2", None, [[1, "1/2"]], None, None),
                    (110, 1, None, [[1, 1]], None, None),
                    (111, 0, None, [[1, 0]], None, None),
                ],
            },
            "report": {
                "files": {
                    "file_1.go": [
                        0,
                        [0, 8, 8, 0, 0, "100", 0, 0, 0, 0, 80, 16, 0],
                        None,
                        None,
                    ],
                    "file_2.py": [
                        1,
                        [0, 2, 1, 0, 1, "50.00000", 1, 0, 0, 0, 0, 0, 0],
                        None,
                        None,
                    ],
                    "single_session_file.c": [
                        2,
                        [0, 3, 1, 1, 1, "33.33333", 0, 0, 0, 0, 0, 0, 0],
                        None,
                        None,
                    ],
                },
                "sessions": {
                    "0": {
                        "N": None,
                        "a": None,
                        "c": None,
                        "d": None,
                        "e": None,
                        "f": ["unit"],
                        "j": None,
                        "n": None,
                        "p": None,
                        "st": "uploaded",
                        "se": {},
                        "t": None,
                        "u": None,
                    },
                    "1": {
                        "N": None,
                        "a": None,
                        "c": None,
                        "d": None,
                        "e": None,
                        "f": ["integration"],
                        "j": None,
                        "n": None,
                        "p": None,
                        "st": "carriedforward",
                        "se": {},
                        "t": None,
                        "u": None,
                    },
                    "2": {
                        "N": None,
                        "a": None,
                        "c": None,
                        "d": None,
                        "e": None,
                        "f": None,
                        "j": None,
                        "n": None,
                        "p": None,
                        "st": "uploaded",
                        "se": {},
                        "t": None,
                        "u": None,
                    },
                },
            },
            "totals": {
                "C": 80,
                "M": 0,
                "N": 16,
                "b": 1,
                "c": "76.92308",
                "d": 0,
                "diff": None,
                "f": 3,
                "h": 10,
                "m": 1,
                "n": 13,
                "p": 2,
                "s": 3,
            },
        }
        report.delete_multiple_sessions({1})
        expected_result = {
            "archive": {
                "file_1.go": [
                    (1, 1, None, [[0, 1], [2, 1]], None, (10, 2)),
                    (2, 1, None, [[0, 1], [2, 1]], None, (10, 2)),
                    (3, 1, None, [[0, 1]], None, (10, 2)),
                    (5, 1, None, [[2, 1]], None, (10, 2)),
                    (6, 1, None, [[0, 1]], None, (10, 2)),
                    (7, 1, None, [[2, 1]], None, (10, 2)),
                    (8, 1, None, [[2, 1], [0, 1]], None, (10, 2)),
                ],
                "file_2.py": [
                    (12, 1, None, [[0, 1], [2, 0]], None, None),
                    (51, "1/3", "b", [[0, "1/3"]], None, None),
                ],
            },
            "report": {
                "files": {
                    "file_1.go": [
                        0,
                        [0, 7, 7, 0, 0, "100", 0, 0, 0, 0, 70, 14, 0],
                        None,
                        None,
                    ],
                    "file_2.py": [
                        1,
                        [0, 2, 1, 0, 1, "50.00000", 1, 0, 0, 0, 0, 0, 0],
                        None,
                        None,
                    ],
                },
                "sessions": {
                    "0": {
                        "N": None,
                        "a": None,
                        "c": None,
                        "d": None,
                        "e": None,
                        "f": ["unit"],
                        "j": None,
                        "n": None,
                        "p": None,
                        "st": "uploaded",
                        "se": {},
                        "t": None,
                        "u": None,
                    },
                    "2": {
                        "N": None,
                        "a": None,
                        "c": None,
                        "d": None,
                        "e": None,
                        "f": None,
                        "j": None,
                        "n": None,
                        "p": None,
                        "st": "uploaded",
                        "se": {},
                        "t": None,
                        "u": None,
                    },
                },
            },
            "totals": {
                "C": 70,
                "M": 0,
                "N": 14,
                "b": 1,
                "c": "88.88889",
                "d": 0,
                "diff": None,
                "f": 2,
                "h": 8,
                "m": 0,
                "n": 9,
                "p": 1,
                "s": 2,
            },
        }
        res = convert_report_to_better_readable(report)
        assert res["archive"] == expected_result["archive"]
        assert res["report"] == expected_result["report"]
        assert res["totals"] == expected_result["totals"]
        assert res == expected_result
        report.delete_multiple_sessions({0, 2})
        assert convert_report_to_better_readable(report) == {
            "archive": {},
            "report": {"files": {}, "sessions": {}},
            "totals": {
                "C": 0,
                "M": 0,
                "N": 0,
                "b": 0,
                "c": None,
                "d": 0,
                "diff": None,
                "f": 0,
                "h": 0,
                "m": 0,
                "n": 0,
                "p": 0,
                "s": 0,
            },
        }

    def test_add_conflicting_session(self, sample_report):
        report = sample_report
        old_readable = convert_report_to_better_readable(report)
        assert old_readable == {
            "archive": {
                "file_1.go": [
                    (1, 1, None, [[0, 1], [1, 1], [2, 1]], None, (10, 2)),
                    (2, 1, None, [[0, 1], [1, "1/2"], [2, 1]], None, (10, 2)),
                    (3, 1, None, [[0, 1]], None, (10, 2)),
                    (4, 1, None, [[1, 1]], None, (10, 2)),
                    (5, 1, None, [[2, 1]], None, (10, 2)),
                    (6, 1, None, [[0, 1], [1, 1]], None, (10, 2)),
                    (7, 1, None, [[1, 1], [2, 1]], None, (10, 2)),
                    (8, 1, None, [[2, 1], [0, 1]], None, (10, 2)),
                ],
                "file_2.py": [
                    (12, 1, None, [[0, 1], [1, "1/2"], [2, 0]], None, None),
                    (51, "1/2", "b", [[0, "1/3"], [1, "1/2"]], None, None),
                ],
                "single_session_file.c": [
                    (101, "1/2", None, [[1, "1/2"]], None, None),
                    (110, 1, None, [[1, 1]], None, None),
                    (111, 0, None, [[1, 0]], None, None),
                ],
            },
            "report": {
                "files": {
                    "file_1.go": [
                        0,
                        [0, 8, 8, 0, 0, "100", 0, 0, 0, 0, 80, 16, 0],
                        None,
                        None,
                    ],
                    "file_2.py": [
                        1,
                        [0, 2, 1, 0, 1, "50.00000", 1, 0, 0, 0, 0, 0, 0],
                        None,
                        None,
                    ],
                    "single_session_file.c": [
                        2,
                        [0, 3, 1, 1, 1, "33.33333", 0, 0, 0, 0, 0, 0, 0],
                        None,
                        None,
                    ],
                },
                "sessions": {
                    "0": {
                        "N": None,
                        "a": None,
                        "c": None,
                        "d": None,
                        "e": None,
                        "f": ["unit"],
                        "j": None,
                        "n": None,
                        "p": None,
                        "st": "uploaded",
                        "se": {},
                        "t": None,
                        "u": None,
                    },
                    "1": {
                        "N": None,
                        "a": None,
                        "c": None,
                        "d": None,
                        "e": None,
                        "f": ["integration"],
                        "j": None,
                        "n": None,
                        "p": None,
                        "st": "carriedforward",
                        "se": {},
                        "t": None,
                        "u": None,
                    },
                    "2": {
                        "N": None,
                        "a": None,
                        "c": None,
                        "d": None,
                        "e": None,
                        "f": None,
                        "j": None,
                        "n": None,
                        "p": None,
                        "st": "uploaded",
                        "se": {},
                        "t": None,
                        "u": None,
                    },
                },
            },
            "totals": {
                "C": 80,
                "M": 0,
                "N": 16,
                "b": 1,
                "c": "76.92308",
                "d": 0,
                "diff": None,
                "f": 3,
                "h": 10,
                "m": 1,
                "n": 13,
                "p": 2,
                "s": 3,
            },
        }
        report.add_session(
            Session(
                sessionid=3, session_type=SessionType.uploaded, flags=["integration"]
            )
        )
        res = convert_report_to_better_readable(report)
        assert res["archive"] == old_readable["archive"]
        assert res["report"]["files"] == old_readable["report"]["files"]
        assert old_readable["totals"].pop("s") == 3
        assert res["totals"].pop("s") == 4
        assert res["totals"] == old_readable["totals"]
