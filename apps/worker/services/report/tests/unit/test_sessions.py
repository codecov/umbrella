import pytest

from services.processing.merging import clear_carryforward_sessions
from shared.reports.reportfile import ReportFile
from shared.reports.resources import Report
from shared.reports.test_utils import convert_report_to_better_readable
from shared.reports.types import ReportLine
from shared.utils.sessions import Session, SessionType
from shared.yaml import UserYaml


class TestAdjustSession:
    @pytest.fixture
    def sample_first_report(self):
        first_report = Report(
            sessions={
                0: Session(
                    flags=["enterprise"],
                    id=0,
                    session_type=SessionType.carriedforward,
                ),
                1: Session(
                    flags=["enterprise"], id=1, session_type=SessionType.uploaded
                ),
                2: Session(
                    flags=["unit"], id=2, session_type=SessionType.carriedforward
                ),
                3: Session(
                    flags=["unrelated"], id=3, session_type=SessionType.uploaded
                ),
            }
        )
        first_file = ReportFile("first_file.py")
        c = 0
        for _ in range(5):
            for sessionid in range(4):
                first_file.append(
                    c % 7 + 1,
                    self.create_sample_line(
                        coverage=c,
                        sessionid=sessionid,
                    ),
                )
                c += 1
        second_file = ReportFile("second_file.py")
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
            ]
        }
        return first_report

    def create_sample_line(self, *, coverage, sessionid=None):
        return ReportLine.create(coverage, sessions=[(sessionid, coverage)])

    def test_adjust_sessions_no_cf(self, sample_first_report):
        first_value = convert_report_to_better_readable(sample_first_report)
        current_yaml = UserYaml({})
        assert (
            clear_carryforward_sessions(
                sample_first_report, {"enterprise"}, current_yaml
            )
            == set()
        )
        assert first_value == convert_report_to_better_readable(sample_first_report)

    def test_adjust_sessions_full_cf_only(self, sample_first_report):
        current_yaml = UserYaml(
            {
                "flag_management": {
                    "individual_flags": [{"name": "enterprise", "carryforward": True}]
                }
            }
        )
        assert clear_carryforward_sessions(
            sample_first_report, {"enterprise"}, current_yaml
        ) == {0}
        assert convert_report_to_better_readable(sample_first_report) == {
            "archive": {
                "first_file.py": [
                    (1, 14, None, [[3, 7], [2, 14]], None, None),
                    (2, 15, None, [[1, 1], [3, 15]], None, None),
                    (3, 9, None, [[2, 2], [1, 9]], None, None),
                    (4, 17, None, [[3, 3], [2, 10], [1, 17]], None, None),
                    (5, 18, None, [[3, 11], [2, 18]], None, None),
                    (6, 19, None, [[1, 5], [3, 19]], None, None),
                    (7, 13, None, [[2, 6], [1, 13]], None, None),
                ]
            },
            "report": {
                "files": {
                    "first_file.py": [
                        0,
                        [0, 7, 7, 0, 0, "100", 0, 0, 0, 0, 0, 0, 0],
                        None,
                        None,
                    ]
                },
                "sessions": {
                    "1": {
                        "t": None,
                        "d": None,
                        "a": None,
                        "f": ["enterprise"],
                        "c": None,
                        "n": None,
                        "N": None,
                        "j": None,
                        "u": None,
                        "p": None,
                        "e": None,
                        "st": "uploaded",
                        "se": {},
                    },
                    "2": {
                        "t": None,
                        "d": None,
                        "a": None,
                        "f": ["unit"],
                        "c": None,
                        "n": None,
                        "N": None,
                        "j": None,
                        "u": None,
                        "p": None,
                        "e": None,
                        "st": "carriedforward",
                        "se": {},
                    },
                    "3": {
                        "t": None,
                        "d": None,
                        "a": None,
                        "f": ["unrelated"],
                        "c": None,
                        "n": None,
                        "N": None,
                        "j": None,
                        "u": None,
                        "p": None,
                        "e": None,
                        "st": "uploaded",
                        "se": {},
                    },
                },
            },
            "totals": {
                "f": 1,
                "n": 7,
                "h": 7,
                "m": 0,
                "p": 0,
                "c": "100",
                "b": 0,
                "d": 0,
                "M": 0,
                "s": 3,
                "C": 0,
                "N": 0,
                "diff": None,
            },
        }

    def test_adjust_sessions_partial_cf_only_full_deletion_due_to_lost_labels(
        self, sample_first_report
    ):
        current_yaml = UserYaml(
            {
                "flag_management": {
                    "individual_flags": [
                        {
                            "name": "enterprise",
                            "carryforward_mode": "labels",
                            "carryforward": True,
                        }
                    ]
                }
            }
        )

        assert clear_carryforward_sessions(
            sample_first_report, {"enterprise"}, current_yaml
        ) == {0}
        res = convert_report_to_better_readable(sample_first_report)
        assert res["report"]["sessions"] == {
            "1": {
                "t": None,
                "d": None,
                "a": None,
                "f": ["enterprise"],
                "c": None,
                "n": None,
                "N": None,
                "j": None,
                "u": None,
                "p": None,
                "e": None,
                "st": "uploaded",
                "se": {},
            },
            "2": {
                "t": None,
                "d": None,
                "a": None,
                "f": ["unit"],
                "c": None,
                "n": None,
                "N": None,
                "j": None,
                "u": None,
                "p": None,
                "e": None,
                "st": "carriedforward",
                "se": {},
            },
            "3": {
                "t": None,
                "d": None,
                "a": None,
                "f": ["unrelated"],
                "c": None,
                "n": None,
                "N": None,
                "j": None,
                "u": None,
                "p": None,
                "e": None,
                "st": "uploaded",
                "se": {},
            },
        }
        assert res["archive"] == {
            "first_file.py": [
                (1, 14, None, [[3, 7], [2, 14]], None, None),
                (2, 15, None, [[1, 1], [3, 15]], None, None),
                (3, 9, None, [[2, 2], [1, 9]], None, None),
                (4, 17, None, [[3, 3], [2, 10], [1, 17]], None, None),
                (5, 18, None, [[3, 11], [2, 18]], None, None),
                (6, 19, None, [[1, 5], [3, 19]], None, None),
                (7, 13, None, [[2, 6], [1, 13]], None, None),
            ]
        }
