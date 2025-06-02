from json import loads

from services.report.languages import simplecov
from shared.reports.test_utils import convert_report_to_better_readable

from . import create_report_builder_session

txt_v17 = """
{
    "timestamp": 1597939304,
    "command_name": "RSpec",
    "files": [
        {
            "filename": "controllers/tests_controller.rb",
            "covered_percent": 27.5,
            "coverage": [
                    1,
                    null,
                    0,
                    "ignored"
            ]
        }
    ]
}
"""

txt_v18 = """
{
    "timestamp": 1597939304,
    "command_name": "RSpec",
    "files": [
        {
            "filename": "controllers/tests_controller.rb",
            "covered_percent": 27.5,
            "coverage": {
                "lines": [
                    1,
                    null,
                    0,
                    "ignored"
                ]
            },
            "covered_strength": 0.275,
            "covered_lines": 11,
            "lines_of_code": 40
        }
    ]
}
"""

txt_v19 = """
{
    "timestamp": 1597939304,
    "meta": {
        "simplecov_version": "0.22.0"
    },
    "files": [
        {
            "filename": "controllers/tests_controller.rb",
            "covered_percent": 27.5,
            "coverage": {
                "lines": [
                    1,
                    null,
                    0,
                    "ignored"
                ]
            },
            "covered_strength": 0.275,
            "covered_lines": 11,
            "lines_of_code": 40
        }
    ]
}
"""


class TestSimplecovProcessor:
    def test_parse_simplecov(self):
        def fixes(path):
            assert path == "controllers/tests_controller.rb"
            return path

        expected_result_archive = {
            "controllers/tests_controller.rb": [
                (1, 1, None, [[0, 1]], None, None),
                (2, None, None, [[0, None]], None, None),
                (3, 0, None, [[0, 0]], None, None),
                (4, -1, None, [[0, -1]], None, None),
            ]
        }

        report_builder_session = create_report_builder_session(path_fixer=fixes)
        simplecov.from_json(loads(txt_v17), report_builder_session)
        report = report_builder_session.output_report()
        processed_report = convert_report_to_better_readable(report)

        assert expected_result_archive == processed_report["archive"]

        simplecov.from_json(loads(txt_v18), report_builder_session)
        report = report_builder_session.output_report()
        processed_report = convert_report_to_better_readable(report)

        assert expected_result_archive == processed_report["archive"]

        simplecov.from_json(loads(txt_v19), report_builder_session)
        report = report_builder_session.output_report()
        processed_report = convert_report_to_better_readable(report)

        assert expected_result_archive == processed_report["archive"]

    def test_process(self):
        def fixes(path):
            assert path == "controllers/tests_controller.rb"
            return path

        expected_result_archive = {
            "controllers/tests_controller.rb": [
                (1, 1, None, [[0, 1]], None, None),
                (2, None, None, [[0, None]], None, None),
                (3, 0, None, [[0, 0]], None, None),
                (4, -1, None, [[0, -1]], None, None),
            ]
        }

        report_builder_session = create_report_builder_session(path_fixer=fixes)
        processor = simplecov.SimplecovProcessor()
        processor.process(loads(txt_v17), report_builder_session)
        report = report_builder_session.output_report()
        processed_report = convert_report_to_better_readable(report)

        assert expected_result_archive == processed_report["archive"]

    def test_matches_simplecov_version(self):
        processor = simplecov.SimplecovProcessor()
        assert processor.matches_content(loads(txt_v19), "", "")

    def test_matches_rspec(self):
        processor = simplecov.SimplecovProcessor()
        assert processor.matches_content(loads(txt_v18), "", "")
