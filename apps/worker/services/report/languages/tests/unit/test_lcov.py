from services.report.languages import lcov
from shared.reports.test_utils import convert_report_to_better_readable

from . import create_report_builder_session

txt = b"""
TN:
SF:file.js
FNDA:76,jsx
FN:76,(anonymous_1)
removed
DA:0,skipped
DA:null,skipped
DA:1,1,46ba21aa66ea047aced7130c2760d7d4
DA:=,=
BRDA:0,1,0,1
BRDA:0,1,0,1
BRDA:1,1,0,1
BRDA:1,1,1,1
end_of_record

TN:
SF:empty.js
FNF:0
FNH:0
DA:0,1
LF:1
LH:1
BRF:0
BRH:0
end_of_record

TN:
SF:file.ts
FNF:0
FNH:0
DA:2,1
LF:1
LH:1
BRF:0
BRH:0
BRDA:1,1,0,1
end_of_record

TN:
SF:file.js
FNDA:76,jsx
FN:76,(anonymous_1)
removed
DA:0,skipped
DA:null,skipped
DA:1,0
DA:=,=
DA:2,1e+0
BRDA:0,0,0,0
BRDA:1,1,1,1
end_of_record

TN:
SF:ignore
end_of_record

TN:
SF:file.cpp
FN:2,not_hit
FN:3,_Zalkfjeo
FN:4,_Gsabebra
FN:78,_Zalkfjeo2
FNDA:1,_ln1_is_skipped
FNDA:,not_hit
FNDA:2,ignored
FNDA:3,ignored
FNDA:4,ignored
FNDA:78,1+e0
DA:1,1
DA:77,0
DA:78,1
BRDA:2,1,0,1
BRDA:2,1,1,-
BRDA:2,1,3,0
BRDA:5,1,0,1
BRDA:5,1,1,1
BRDA:77,3,0,0
BRDA:77,3,1,0
BRDA:77,4,0,0
BRDA:77,4,1,0
end_of_record
"""

negative_count = b"""
TN:
SF:file.js
DA:1,1
DA:2,2
DA:3,0
DA:4,-1
DA:5,-5
DA:6,-20
end_of_record
"""

corrupt_txt = b"""
TN:
SF:foo.cpp

DA:1,1

DA:DA:130,0
DA:0,
DA:,0
DA:
DA:not_int,123
DA:123,not_decimal

FN:just_a_name_no_line

end_of_record
"""


class TestLcov:
    def test_report(self):
        def fixes(path):
            if path == "ignore":
                return None
            assert path in ("file.js", "file.ts", "file.cpp", "empty.js")
            return path

        report_builder_session = create_report_builder_session(path_fixer=fixes)
        lcov.from_txt(txt, report_builder_session)
        report = report_builder_session.output_report()
        processed_report = convert_report_to_better_readable(report)

        assert processed_report["archive"] == {
            "file.cpp": [
                (1, 1, None, [[0, 1]], None, None),
                (2, "1/3", "m", [[0, "1/3", ["1:1", "1:3"], None, None]], None, None),
                (5, "2/2", "b", [[0, "2/2"]], None, None),
                (
                    77,
                    "0/4",
                    "b",
                    [[0, "0/4", ["3:0", "3:1", "4:0", "4:1"], None, None]],
                    None,
                    None,
                ),
                (78, 1, None, [[0, 1]], None, None),
            ],
            "file.js": [
                (1, 1, None, [[0, 1]], None, None),
                (2, 1, None, [[0, 1]], None, None),
            ],
            "file.ts": [(2, 1, None, [[0, 1]], None, None)],
        }

    def test_detect(self):
        processor = lcov.LcovProcessor()
        assert processor.matches_content(b"hello\nend_of_record\n", "", "") is True
        assert processor.matches_content(txt, "", "") is True
        assert processor.matches_content(b"hello_end_of_record", "", "") is False
        assert processor.matches_content(b"", "", "") is False

    def test_negative_execution_count(self):
        report_builder_session = create_report_builder_session()
        lcov.from_txt(negative_count, report_builder_session)
        report = report_builder_session.output_report()
        processed_report = convert_report_to_better_readable(report)

        assert processed_report["archive"] == {
            "file.js": [
                (1, 1, None, [[0, 1]], None, None),
                (2, 2, None, [[0, 2]], None, None),
                (3, 0, None, [[0, 0]], None, None),
                (4, 0, None, [[0, 0]], None, None),
                (5, 0, None, [[0, 0]], None, None),
                (6, 0, None, [[0, 0]], None, None),
            ]
        }

    def test_skips_corrupted_lines(self):
        report_builder_session = create_report_builder_session()
        lcov.from_txt(corrupt_txt, report_builder_session)
        report = report_builder_session.output_report()
        processed_report = convert_report_to_better_readable(report)

        assert processed_report["archive"] == {
            "foo.cpp": [
                (1, 1, None, [[0, 1]], None, None),
            ]
        }

    def test_regression_partial_branch(self):
        # See https://github.com/codecov/feedback/issues/513
        text = b"""
SF:foo.c
DA:1047,731835
BRDA:1047,0,0,0
BRDA:1047,0,1,1
end_of_record
"""
        report_builder_session = create_report_builder_session()
        lcov.from_txt(text, report_builder_session)
        report = report_builder_session.output_report()
        processed_report = convert_report_to_better_readable(report)

        assert processed_report["archive"] == {
            "foo.c": [
                (1047, "1/2", "b", [[0, "1/2", ["0:0"], None, None]], None, None),
            ]
        }

    def test_lcov_partials_as_hits_enabled(self):
        """Test that partial branches become hits when partials_as_hits=True"""
        lcov_data = b"""
TN:
SF:partial_test.c
DA:10,5
BRDA:10,0,0,1
BRDA:10,0,1,0
end_of_record
"""

        def path_fixer(path):
            return path if path == "partial_test.c" else None

        # With partials_as_hits enabled
        report_builder_session = create_report_builder_session(
            current_yaml={"parsers": {"lcov": {"partials_as_hits": True}}},
            path_fixer=path_fixer,
        )

        lcov.from_txt(lcov_data, report_builder_session)
        report = report_builder_session.output_report()
        processed_report = convert_report_to_better_readable(report)

        # Should convert "1/2" partial to hit (1) but keep branch type
        expected = {
            "partial_test.c": [
                (10, 5, None, [[0, 5]], None, None),  # line coverage unchanged
                (
                    10,
                    1,
                    "b",
                    [[0, 1]],
                    None,
                    None,
                ),  # branch converted to hit, keeps branch type
            ]
        }
        assert processed_report["archive"] == expected

    def test_lcov_partials_as_hits_disabled(self):
        """Test that partials remain partial when partials_as_hits=False (default)"""
        lcov_data = b"""
TN:
SF:partial_test.c
DA:10,5
BRDA:10,0,0,1
BRDA:10,0,1,0
end_of_record
"""

        def path_fixer(path):
            return path if path == "partial_test.c" else None

        # With partials_as_hits disabled (default)
        report_builder_session = create_report_builder_session(
            path_fixer=path_fixer,
        )

        lcov.from_txt(lcov_data, report_builder_session)
        report = report_builder_session.output_report()
        processed_report = convert_report_to_better_readable(report)

        # Should keep "1/2" as partial
        expected = {
            "partial_test.c": [
                (10, 5, None, [[0, 5]], None, None),
                (10, "1/2", "b", [[0, "1/2", ["0:1"], None, None]], None, None),
            ]
        }
        assert processed_report["archive"] == expected

    def test_lcov_partials_as_hits_mixed_coverage(self):
        """Test partials_as_hits with mixed hit/miss/partial scenarios"""
        lcov_data = b"""
TN:
SF:mixed_test.c
DA:10,5
DA:20,3
DA:30,0
BRDA:10,0,0,1
BRDA:10,0,1,0
BRDA:20,0,0,1
BRDA:20,0,1,1
BRDA:30,0,0,0
BRDA:30,0,1,0
end_of_record
"""

        def path_fixer(path):
            return path if path == "mixed_test.c" else None

        report_builder_session = create_report_builder_session(
            current_yaml={"parsers": {"lcov": {"partials_as_hits": True}}},
            path_fixer=path_fixer,
        )

        lcov.from_txt(lcov_data, report_builder_session)
        report = report_builder_session.output_report()
        processed_report = convert_report_to_better_readable(report)

        expected = {
            "mixed_test.c": [
                (10, 5, None, [[0, 5]], None, None),
                (10, 1, "b", [[0, 1]], None, None),  # partial -> hit, keeps branch type
                (20, 3, None, [[0, 3]], None, None),
                (20, "2/2", "b", [[0, "2/2"]], None, None),  # hit stays hit
                (30, 0, None, [[0, 0]], None, None),
                (
                    30,
                    "0/2",
                    "b",
                    [[0, "0/2", ["0:0", "0:1"], None, None]],
                    None,
                    None,
                ),  # miss stays miss
            ]
        }
        assert processed_report["archive"] == expected
