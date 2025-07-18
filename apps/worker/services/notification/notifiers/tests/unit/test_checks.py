from copy import deepcopy
from unittest.mock import Mock

import pytest

from services.decoration import Decoration
from services.notification.notifiers.checks import (
    ChangesChecksNotifier,
    PatchChecksNotifier,
    ProjectChecksNotifier,
)
from services.notification.notifiers.checks.base import ChecksNotifier
from services.notification.notifiers.checks.checks_with_fallback import (
    ChecksWithFallback,
)
from services.notification.notifiers.status import PatchStatusNotifier
from shared.reports.readonly import ReadOnlyReport
from shared.reports.reportfile import ReportFile
from shared.reports.resources import Report
from shared.reports.types import ReportLine
from shared.torngit.exceptions import TorngitClientGeneralError, TorngitError
from shared.torngit.status import Status
from shared.yaml.user_yaml import UserYaml
from tests.helpers import mock_all_plans_and_tiers


@pytest.fixture
def mock_repo_provider(mock_repo_provider):
    compare_result = {
        "diff": {
            "files": {
                "file_1.go": {
                    "type": "modified",
                    "before": None,
                    "segments": [
                        {
                            "header": ["5", "8", "5", "9"],
                            "lines": [
                                " Overview",
                                " --------",
                                " ",
                                "-Main website: `Codecov <https://codecov.io/>`_.",
                                "-Main website: `Codecov <https://codecov.io/>`_.",
                                "+",
                                "+website: `Codecov <https://codecov.io/>`_.",
                                "+website: `Codecov <https://codecov.io/>`_.",
                                " ",
                                " .. code-block:: shell-session",
                                " ",
                            ],
                        },
                        {
                            "header": ["46", "12", "47", "19"],
                            "lines": [
                                " ",
                                " You may need to configure a ``.coveragerc`` file. Learn more `here <http://coverage.readthedocs.org/en/latest/config.html>`_. Start with this `generic .coveragerc <https://gist.github.com/codecov-io/bf15bde2c7db1a011b6e>`_ for example.",
                                " ",
                                "-We highly suggest adding `source` to your ``.coveragerc`` which solves a number of issues collecting coverage.",
                                "+We highly suggest adding ``source`` to your ``.coveragerc``, which solves a number of issues collecting coverage.",
                                " ",
                                " .. code-block:: ini",
                                " ",
                                "    [run]",
                                "    source=your_package_name",
                                "+   ",
                                "+If there are multiple sources, you instead should add ``include`` to your ``.coveragerc``",
                                "+",
                                "+.. code-block:: ini",
                                "+",
                                "+   [run]",
                                "+   include=your_package_name/*",
                                " ",
                                " unittests",
                                " ---------",
                            ],
                        },
                        {
                            "header": ["150", "5", "158", "4"],
                            "lines": [
                                " * Twitter: `@codecov <https://twitter.com/codecov>`_.",
                                " * Email: `hello@codecov.io <hello@codecov.io>`_.",
                                " ",
                                "-We are happy to help if you have any questions. Please contact email our Support at [support@codecov.io](mailto:support@codecov.io)",
                                "-",
                                "+We are happy to help if you have any questions. Please contact email our Support at `support@codecov.io <mailto:support@codecov.io>`_.",
                            ],
                        },
                    ],
                    "stats": {"added": 11, "removed": 4},
                }
            }
        },
        "commits": [
            {
                "commitid": "b92edba44fdd29fcc506317cc3ddeae1a723dd08",
                "message": "Update README.rst",
                "timestamp": "2018-07-09T23:51:16Z",
                "author": {
                    "id": 8398772,
                    "username": "jerrode",
                    "name": "Jerrod",
                    "email": "jerrod@fundersclub.com",
                },
            },
            {
                "commitid": "6ae5f1795a441884ed2847bb31154814ac01ef38",
                "message": "Update README.rst",
                "timestamp": "2018-04-26T08:35:58Z",
                "author": {
                    "id": 11602092,
                    "username": "TomPed",
                    "name": "Thomas Pedbereznak",
                    "email": "tom@tomped.com",
                },
            },
        ],
    }
    mock_repo_provider.get_compare.return_value = compare_result
    return mock_repo_provider


@pytest.fixture
def comparison_with_multiple_changes(sample_comparison):
    first_report = Report()
    second_report = Report()
    # DELETED FILE
    first_deleted_file = ReportFile("deleted.py")
    first_deleted_file.append(10, ReportLine.create(1))
    first_deleted_file.append(12, ReportLine.create(0))
    first_report.append(first_deleted_file)
    # ADDED FILE
    second_added_file = ReportFile("added.py")
    second_added_file.append(99, ReportLine.create(1))
    second_added_file.append(101, ReportLine.create(0))
    second_report.append(second_added_file)
    # MODIFIED FILE
    first_modified_file = ReportFile("modified.py")
    first_modified_file.append(17, ReportLine.create(1))
    first_modified_file.append(18, ReportLine.create(1))
    first_modified_file.append(19, ReportLine.create(1))
    first_modified_file.append(20, ReportLine.create(0))
    first_modified_file.append(21, ReportLine.create(1))
    first_modified_file.append(22, ReportLine.create(1))
    first_modified_file.append(23, ReportLine.create(1))
    first_modified_file.append(24, ReportLine.create(1))
    first_report.append(first_modified_file)
    second_modified_file = ReportFile("modified.py")
    second_modified_file.append(18, ReportLine.create(1))
    second_modified_file.append(19, ReportLine.create(0))
    second_modified_file.append(20, ReportLine.create(0))
    second_modified_file.append(21, ReportLine.create(1))
    second_modified_file.append(22, ReportLine.create(0))
    second_modified_file.append(23, ReportLine.create(0))
    second_modified_file.append(24, ReportLine.create(1))
    second_report.append(second_modified_file)
    # RENAMED WITHOUT CHANGES
    first_renamed_without_changes_file = ReportFile("old_renamed.py")
    first_renamed_without_changes_file.append(1, ReportLine.create(1))
    first_renamed_without_changes_file.append(2, ReportLine.create(1))
    first_renamed_without_changes_file.append(3, ReportLine.create(0))
    first_renamed_without_changes_file.append(4, ReportLine.create(1))
    first_renamed_without_changes_file.append(5, ReportLine.create(0))
    first_report.append(first_renamed_without_changes_file)
    second_renamed_without_changes_file = ReportFile("renamed.py")
    second_renamed_without_changes_file.append(1, ReportLine.create(1))
    second_renamed_without_changes_file.append(2, ReportLine.create(1))
    second_renamed_without_changes_file.append(3, ReportLine.create(0))
    second_renamed_without_changes_file.append(4, ReportLine.create(1))
    second_renamed_without_changes_file.append(5, ReportLine.create(0))
    second_report.append(second_renamed_without_changes_file)
    # RENAMED WITH COVERAGE CHANGES FILE
    first_renamed_file = ReportFile("old_renamed_with_changes.py")
    first_renamed_file.append(2, ReportLine.create(1))
    first_renamed_file.append(3, ReportLine.create(1))
    first_renamed_file.append(5, ReportLine.create(0))
    first_renamed_file.append(8, ReportLine.create(1))
    first_renamed_file.append(13, ReportLine.create(1))
    first_report.append(first_renamed_file)
    second_renamed_file = ReportFile("renamed_with_changes.py")
    second_renamed_file.append(5, ReportLine.create(1))
    second_renamed_file.append(8, ReportLine.create(0))
    second_renamed_file.append(13, ReportLine.create(1))
    second_renamed_file.append(21, ReportLine.create(1))
    second_renamed_file.append(34, ReportLine.create(0))
    second_report.append(second_renamed_file)
    # UNRELATED FILE
    first_unrelated_file = ReportFile("unrelated.py")
    first_unrelated_file.append(1, ReportLine.create(1))
    first_unrelated_file.append(2, ReportLine.create(1))
    first_unrelated_file.append(4, ReportLine.create(1))
    first_unrelated_file.append(16, ReportLine.create(0))
    first_unrelated_file.append(256, ReportLine.create(1))
    first_unrelated_file.append(65556, ReportLine.create(1))
    first_report.append(first_unrelated_file)
    second_unrelated_file = ReportFile("unrelated.py")
    second_unrelated_file.append(2, ReportLine.create(1))
    second_unrelated_file.append(4, ReportLine.create(0))
    second_unrelated_file.append(8, ReportLine.create(0))
    second_unrelated_file.append(16, ReportLine.create(1))
    second_unrelated_file.append(32, ReportLine.create(0))
    second_report.append(second_unrelated_file)
    sample_comparison.project_coverage_base.report = ReadOnlyReport.create_from_report(
        first_report
    )
    sample_comparison.head.report = ReadOnlyReport.create_from_report(second_report)
    return sample_comparison


@pytest.fixture
def multiple_diff_changes():
    return {
        "files": {
            "modified.py": {
                "before": None,
                "segments": [
                    {
                        "header": ["20", "8", "20", "8"],
                        "lines": [
                            "     return k * k",
                            " ",
                            " ",
                            "-def k(l):",
                            "-    return 2 * l",
                            "+def k(var):",
                            "+    return 2 * var",
                            " ",
                            " ",
                            " def sample_function():",
                        ],
                    }
                ],
                "stats": {"added": 2, "removed": 2},
                "type": "modified",
            },
            "renamed.py": {
                "before": "old_renamed.py",
                "segments": [],
                "stats": {"added": 0, "removed": 0},
                "type": "modified",
            },
            "renamed_with_changes.py": {
                "before": "old_renamed_with_changes.py",
                "segments": [],
                "stats": {"added": 0, "removed": 0},
                "type": "modified",
            },
            "added.py": {
                "before": None,
                "segments": [
                    {
                        "header": ["0", "0", "1", ""],
                        "lines": ["+This is an explanation"],
                    }
                ],
                "stats": {"added": 1, "removed": 0},
                "type": "new",
            },
            "deleted.py": {
                "before": "tests/test_sample.py",
                "stats": {"added": 0, "removed": 0},
                "type": "deleted",
            },
        }
    }


class TestChecksWithFallback:
    def test_checks_403_failure(self, sample_comparison, mocker, mock_repo_provider):
        mock_repo_provider.create_check_run = Mock(
            side_effect=TorngitClientGeneralError(
                403, response_data="No Access", message="No Access"
            )
        )

        checks_notifier = PatchChecksNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"flags": ["flagone"]},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
        )
        status_notifier = mocker.MagicMock(
            PatchStatusNotifier(
                repository=sample_comparison.head.commit.repository,
                title="title",
                notifier_yaml_settings={"flags": ["flagone"]},
                notifier_site_settings=True,
                current_yaml=UserYaml({}),
                repository_service=mock_repo_provider,
            )
        )
        status_notifier.notify.return_value = "success"
        fallback_notifier = ChecksWithFallback(
            checks_notifier=checks_notifier, status_notifier=status_notifier
        )
        assert fallback_notifier.name == "checks-patch-with-fallback"
        assert fallback_notifier.title == "title"
        assert fallback_notifier.is_enabled() == True
        assert fallback_notifier.notification_type.value == "checks_patch"
        assert fallback_notifier.decoration_type is None

        res = fallback_notifier.notify(sample_comparison)
        fallback_notifier.store_results(sample_comparison, res)
        assert status_notifier.notify.call_count == 1
        assert fallback_notifier.name == "checks-patch-with-fallback"
        assert fallback_notifier.title == "title"
        assert fallback_notifier.is_enabled() == True
        assert fallback_notifier.notification_type.value == "checks_patch"
        assert fallback_notifier.decoration_type is None
        assert res == "success"

    def test_checks_failure(self, sample_comparison, mocker, mock_repo_provider):
        mock_repo_provider.get_commit_statuses.return_value = Status([])
        mock_repo_provider.create_check_run = Mock(
            side_effect=TorngitClientGeneralError(
                409, response_data="No Access", message="No Access"
            )
        )

        checks_notifier = PatchChecksNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"flags": ["flagone"]},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
        )
        status_notifier = mocker.MagicMock(
            PatchStatusNotifier(
                repository=sample_comparison.head.commit.repository,
                title="title",
                notifier_yaml_settings={"flags": ["flagone"]},
                notifier_site_settings=True,
                current_yaml=UserYaml({}),
                repository_service=mock_repo_provider,
            )
        )
        status_notifier.notify.return_value = "success"
        fallback_notifier = ChecksWithFallback(
            checks_notifier=checks_notifier, status_notifier=status_notifier
        )
        assert fallback_notifier.name == "checks-patch-with-fallback"
        assert fallback_notifier.title == "title"
        assert fallback_notifier.is_enabled() == True
        assert fallback_notifier.notification_type.value == "checks_patch"
        assert fallback_notifier.decoration_type is None

        res = fallback_notifier.notify(sample_comparison)
        assert res.notification_successful == False
        assert res.explanation == "client_side_error_provider"

        mock_repo_provider.create_check_run = Mock(side_effect=TorngitError())

        res = fallback_notifier.notify(sample_comparison)
        assert res.notification_successful == False
        assert res.explanation == "server_side_error_provider"

        mock_repo_provider.create_check_run.return_value = 1234
        mock_repo_provider.update_check_run = Mock(side_effect=TorngitError())
        res = fallback_notifier.notify(sample_comparison)
        assert res.notification_successful == False
        assert res.explanation == "server_side_error_provider"

    def test_checks_no_pull(self, sample_comparison_without_pull, mocker):
        comparison = sample_comparison_without_pull
        checks_notifier = PatchChecksNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"flags": ["flagone"]},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=None,
        )
        status_notifier = mocker.MagicMock(
            PatchStatusNotifier(
                repository=comparison.head.commit.repository,
                title="title",
                notifier_yaml_settings={"flags": ["flagone"]},
                notifier_site_settings=True,
                current_yaml=UserYaml({}),
                repository_service=None,
            )
        )
        status_notifier.notify.return_value = "success"
        fallback_notifier = ChecksWithFallback(
            checks_notifier=checks_notifier, status_notifier=status_notifier
        )
        result = fallback_notifier.notify(sample_comparison_without_pull)
        assert result == "success"
        assert status_notifier.notify.call_count == 1

    def test_notify_pull_request_not_in_provider(
        self, dbsession, sample_comparison_database_pull_without_provider, mocker
    ):
        comparison = sample_comparison_database_pull_without_provider
        checks_notifier = PatchChecksNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"flags": ["flagone"]},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=None,
        )
        status_notifier = mocker.MagicMock(
            PatchStatusNotifier(
                repository=comparison.head.commit.repository,
                title="title",
                notifier_yaml_settings={"flags": ["flagone"]},
                notifier_site_settings=True,
                current_yaml=UserYaml({}),
                repository_service=None,
            )
        )
        status_notifier.notify.return_value = "success"
        fallback_notifier = ChecksWithFallback(
            checks_notifier=checks_notifier, status_notifier=status_notifier
        )
        result = fallback_notifier.notify(comparison)
        assert result == "success"
        assert status_notifier.notify.call_count == 1

    def test_notify_closed_pull_request(self, dbsession, sample_comparison, mocker):
        sample_comparison.pull.state = "closed"

        checks_notifier = PatchChecksNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"flags": ["flagone"]},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=None,
        )
        status_notifier = mocker.MagicMock(
            PatchStatusNotifier(
                repository=sample_comparison.head.commit.repository,
                title="title",
                notifier_yaml_settings={"flags": ["flagone"]},
                notifier_site_settings=True,
                current_yaml=UserYaml({}),
                repository_service=None,
            )
        )
        status_notifier.notify.return_value = "success"
        fallback_notifier = ChecksWithFallback(
            checks_notifier=checks_notifier, status_notifier=status_notifier
        )
        result = fallback_notifier.notify(sample_comparison)
        assert result == "success"
        assert status_notifier.notify.call_count == 1


class TestBaseChecksNotifier:
    def test_create_annotations_single_segment(self, sample_comparison):
        notifier = ChecksNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=None,
        )
        diff = {
            "files": {
                "file_1.go": {
                    "type": "modified",
                    "before": "None",
                    "segments": [
                        {
                            "header": ["5", "8", "5", "9"],
                            "lines": [
                                " Overview",
                                " --------",
                                " ",
                                "-Main website: `Codecov <https://codecov.io/>`_.",
                                "-Main website: `Codecov <https://codecov.io/>`_.",
                                "+",
                                "+website: `Codecov <https://codecov.io/>`_.",
                                "+website: `Codecov <https://codecov.io/>`_.",
                                " ",
                                " .. code-block:: shell-session",
                                " ",
                            ],
                        }
                    ],
                    "totals": True,
                }
            }
        }
        expected_annotations = [
            {
                "path": "file_1.go",
                "start_line": 10,
                "end_line": 10,
                "annotation_level": "warning",
                "message": "Added line #L10 was not covered by tests",
            }
        ]
        annotations = notifier.create_annotations(sample_comparison, diff)
        assert expected_annotations == annotations

    def test_create_annotations_multiple_segments(self, sample_comparison):
        notifier = ChecksNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=None,
        )
        diff = {
            "files": {
                "file_1.go": {
                    "type": "modified",
                    "before": "None",
                    "segments": [
                        {
                            "header": ["1", "1", "1", "1"],
                            "lines": [
                                " ",
                                "+ You may need to configure a ``.coveragerc`` file. Learn more `here <http://coverage.readthedocs.org/en/latest/config.html>`_. Start with this `generic .coveragerc <https://gist.github.com/codecov-io/bf15bde2c7db1a011b6e>`_ for example.",
                                " ",
                                "-We highly suggest adding `source` to your ``.coveragerc`` which solves a number of issues collecting coverage.",
                                "+We highly suggest adding ``source`` to your ``.coveragerc``, which solves a number of issues collecting coverage.",
                                " ---------",
                            ],
                        },
                        {
                            "header": ["5", "8", "5", "9"],
                            "lines": [
                                " Overview",
                                " --------",
                                " ",
                                "-Main website: `Codecov <https://codecov.io/>`_.",
                                "-Main website: `Codecov <https://codecov.io/>`_.",
                                "+",
                                "+website: `Codecov <https://codecov.io/>`_.",
                                "+website: `Codecov <https://codecov.io/>`_.",
                                " ",
                                " .. code-block:: shell-session",
                                " ",
                            ],
                        },
                        {
                            "header": ["150", "5", "158", "4"],
                            "lines": [
                                " * Twitter: `@codecov <https://twitter.com/codecov>`_.",
                                " * Email: `hello@codecov.io <hello@codecov.io>`_.",
                                " ",
                                "-We are happy to help if you have any questions. Please contact email our Support at [support@codecov.io](mailto:support@codecov.io)",
                                "-",
                                "+We are happy to help if you have any questions. Please contact email our Support at `support@codecov.io <mailto:support@codecov.io>`_.",
                            ],
                        },
                    ],
                    "totals": True,
                }
            }
        }
        expected_annotations = [
            {
                "path": "file_1.go",
                "start_line": 2,
                "end_line": 2,
                "annotation_level": "warning",
                "message": "Added line #L2 was not covered by tests",
            },
            {
                "path": "file_1.go",
                "start_line": 10,
                "end_line": 10,
                "annotation_level": "warning",
                "message": "Added line #L10 was not covered by tests",
            },
        ]
        annotations = notifier.create_annotations(sample_comparison, diff)
        assert expected_annotations == annotations

    def test_get_lines_to_annotate_no_consecutive_lines(self, sample_comparison):
        notifier = ChecksNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=None,
        )
        files_with_change = [
            {
                "type": "modified",
                "path": "file_1.go",
                "additions": [
                    {"head_line": 1},
                    {"head_line": 2},
                    {"head_line": 3},
                    {"head_line": 5},
                    {"head_line": 6},
                    {"head_line": 8},
                ],
            }
        ]
        expected_result = [
            {
                "type": "new_line",
                "line": 2,
                "coverage": 0,
                "path": "file_1.go",
                "end_line": 2,
            },
            {
                "type": "new_line",
                "line": 6,
                "coverage": 0,
                "path": "file_1.go",
                "end_line": 6,
            },
        ]
        result = notifier.get_lines_to_annotate(sample_comparison, files_with_change)
        assert expected_result == result

    def test_get_lines_to_annotate_consecutive_lines(self, sample_comparison):
        notifier = ChecksNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=None,
        )
        report = Report()
        first_deleted_file = ReportFile("file_1.go")
        first_deleted_file.append(1, ReportLine.create(0))
        first_deleted_file.append(2, ReportLine.create(0))
        first_deleted_file.append(3, ReportLine.create(0))
        first_deleted_file.append(5, ReportLine.create(0))
        report.append(first_deleted_file)
        sample_comparison.head.report = report
        files_with_change = [
            {
                "type": "modified",
                "path": "file_1.go",
                "additions": [
                    {"head_line": 1},
                    {"head_line": 2},
                    {"head_line": 3},
                    {"head_line": 5},
                    {"head_line": 6},
                    {"head_line": 8},
                ],
            }
        ]
        expected_result = [
            {
                "type": "new_line",
                "line": 1,
                "coverage": 0,
                "path": "file_1.go",
                "end_line": 3,
            },
            {
                "type": "new_line",
                "line": 5,
                "coverage": 0,
                "path": "file_1.go",
                "end_line": 5,
            },
        ]
        result = notifier.get_lines_to_annotate(sample_comparison, files_with_change)
        assert expected_result == result


class TestPatchChecksNotifier:
    def test_paginate_annotations(
        self, sample_comparison, mock_repo_provider, mock_configuration
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        notifier = PatchChecksNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"flags": ["flagone"]},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
        )
        sample_array = list(range(1, 61, 1))
        expected_result = [list(range(1, 51, 1)), list(range(51, 61, 1))]
        result = list(notifier.paginate_annotations(sample_array))
        assert expected_result == result

    def test_build_flag_payload(
        self, sample_comparison, mock_repo_provider, mock_configuration, snapshot
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        notifier = PatchChecksNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"flags": ["flagone"]},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
        )
        result = notifier.build_payload(sample_comparison)
        assert snapshot("json") == result

    def test_build_upgrade_payload(
        self, sample_comparison, mock_repo_provider, mock_configuration, snapshot
    ):
        mock_configuration.params["setup"] = {
            "codecov_url": "test.example.br",
            "codecov_dashboard_url": "test.example.br",
        }
        notifier = PatchChecksNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
            decoration_type=Decoration.upgrade,
        )
        result = notifier.build_payload(sample_comparison)
        assert snapshot("json") == result

    @pytest.mark.django_db
    def test_build_default_payload(
        self, sample_comparison, mock_repo_provider, mock_configuration, snapshot
    ):
        mock_all_plans_and_tiers()
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        notifier = PatchChecksNotifier(
            repository=sample_comparison.head.commit.repository,
            title="default",
            notifier_yaml_settings={},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
        )
        result = notifier.build_payload(sample_comparison)
        assert snapshot("json") == result

    def test_build_payload_target_coverage_failure(
        self, sample_comparison, mock_repo_provider, mock_configuration, snapshot
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        notifier = PatchChecksNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"target": "70%", "paths": ["pathone"]},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
        )
        result = notifier.build_payload(sample_comparison)
        assert snapshot("json") == result

    def test_build_payload_without_base_report(
        self,
        sample_comparison_without_base_report,
        mock_repo_provider,
        mock_configuration,
        snapshot,
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison_without_base_report
        notifier = PatchChecksNotifier(
            repository=comparison.head.commit.repository,
            title="default",
            notifier_yaml_settings={},
            notifier_site_settings=True,
            current_yaml=UserYaml({"github_checks": {"annotations": True}}),
            repository_service=mock_repo_provider,
        )
        result = notifier.build_payload(comparison)
        assert snapshot("json") == result

    def test_build_payload_target_coverage_failure_within_threshold(
        self, sample_comparison, mock_repo_provider, mock_configuration, snapshot
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        third_file = ReportFile("file_3.c")
        third_file.append(100, ReportLine.create(1, sessions=[[0, 1]]))
        third_file.append(101, ReportLine.create(1, sessions=[[0, 1]]))
        third_file.append(102, ReportLine.create(1, sessions=[[0, 1]]))
        third_file.append(103, ReportLine.create(1, sessions=[[0, 1]]))
        report = sample_comparison.project_coverage_base.report.inner_report
        report.append(third_file)
        sample_comparison.project_coverage_base.report = (
            ReadOnlyReport.create_from_report(report)
        )
        notifier = PatchChecksNotifier(
            repository=sample_comparison.head.commit.repository,
            title="default",
            notifier_yaml_settings={"threshold": "5"},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
        )
        result = notifier.build_payload(sample_comparison)
        assert snapshot("json") == result

    def test_build_payload_with_multiple_changes(
        self,
        comparison_with_multiple_changes,
        mock_repo_provider,
        mock_configuration,
        multiple_diff_changes,
        snapshot,
    ):
        json_diff = multiple_diff_changes
        original_value = deepcopy(multiple_diff_changes)
        mock_repo_provider.get_compare.return_value = {"diff": json_diff}

        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        notifier = PatchChecksNotifier(
            repository=comparison_with_multiple_changes.head.commit.repository,
            title="default",
            notifier_yaml_settings={},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
        )

        result = notifier.build_payload(comparison_with_multiple_changes)
        assert snapshot("json") == result

        # assert that the value of diff was not changed
        for filename in original_value["files"]:
            assert original_value["files"][filename].get(
                "segments"
            ) == multiple_diff_changes["files"][filename].get("segments")

    def test_github_checks_annotations_yaml(
        self,
        comparison_with_multiple_changes,
        mock_repo_provider,
        mock_configuration,
        multiple_diff_changes,
    ):
        mock_repo_provider.get_compare.return_value = {"diff": multiple_diff_changes}
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"

        empty_annotations = []

        # checks_yaml_field can be None
        notifier = PatchChecksNotifier(
            repository=comparison_with_multiple_changes.head.commit.repository,
            title="default",
            notifier_yaml_settings={},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
        )
        result = notifier.build_payload(comparison_with_multiple_changes)
        assert empty_annotations == result["output"]["annotations"]

        # checks_yaml_field can be dict
        notifier = PatchChecksNotifier(
            repository=comparison_with_multiple_changes.head.commit.repository,
            title="default",
            notifier_yaml_settings={},
            notifier_site_settings=True,
            current_yaml=UserYaml({"github_checks": {"one": "two"}}),
            repository_service=mock_repo_provider,
        )
        result = notifier.build_payload(comparison_with_multiple_changes)
        assert empty_annotations == result["output"]["annotations"]

        # checks_yaml_field can be bool
        notifier = PatchChecksNotifier(
            repository=comparison_with_multiple_changes.head.commit.repository,
            title="default",
            notifier_yaml_settings={},
            notifier_site_settings=True,
            current_yaml=UserYaml({"github_checks": False}),
            repository_service=mock_repo_provider,
        )
        result = notifier.build_payload(comparison_with_multiple_changes)
        assert empty_annotations == result["output"]["annotations"]

        # checks_yaml_field with annotations
        notifier = PatchChecksNotifier(
            repository=comparison_with_multiple_changes.head.commit.repository,
            title="default",
            notifier_yaml_settings={},
            notifier_site_settings=True,
            current_yaml=UserYaml({"github_checks": {"annotations": True}}),
            repository_service=mock_repo_provider,
        )
        expected_annotations = [
            {
                "path": "modified.py",
                "start_line": 23,
                "end_line": 23,
                "annotation_level": "warning",
                "message": "Added line #L23 was not covered by tests",
            }
        ]
        result = notifier.build_payload(comparison_with_multiple_changes)
        assert expected_annotations == result["output"]["annotations"]

    def test_build_payload_no_diff(
        self, sample_comparison, mock_repo_provider, mock_configuration, snapshot
    ):
        mock_repo_provider.get_compare.return_value = {
            "diff": {
                "files": {
                    "file_1.go": {
                        "type": "modified",
                        "before": None,
                        "segments": [
                            {
                                "header": ["15", "8", "15", "9"],
                                "lines": [
                                    " Overview",
                                    " --------",
                                    " ",
                                    "-Main website: `Codecov <https://codecov.io/>`_.",
                                    "-Main website: `Codecov <https://codecov.io/>`_.",
                                    "+",
                                    "+website: `Codecov <https://codecov.io/>`_.",
                                    "+website: `Codecov <https://codecov.io/>`_.",
                                    " ",
                                    " .. code-block:: shell-session",
                                    " ",
                                ],
                            }
                        ],
                        "stats": {"added": 11, "removed": 4},
                    }
                }
            }
        }
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        notifier = PatchChecksNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"flags": ["flagone"]},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
        )
        assert notifier.is_enabled()
        result = notifier.build_payload(sample_comparison)
        assert snapshot("json") == result

    def test_send_notification(
        self, sample_comparison, mocker, mock_repo_provider, snapshot
    ):
        comparison = sample_comparison
        payload = {
            "state": "success",
            "output": {"title": "Codecov Report", "summary": "Summary"},
            "url": "https://app.codecov.io/gh/codecov/worker/compare/100?src=pr&el=continue&utm_medium=referral&utm_source=github&utm_content=checks&utm_campaign=pr+comments&utm_term=codecov",
        }
        mock_repo_provider.create_check_run.return_value = 2234563
        mock_repo_provider.update_check_run.return_value = "success"
        notifier = PatchChecksNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
        )
        result = notifier.send_notification(sample_comparison, payload)
        assert result.notification_successful == True
        assert result.explanation is None
        assert snapshot("json") == result.data_sent

    def test_send_notification_annotations_paginations(
        self, sample_comparison, mocker, mock_repo_provider, snapshot
    ):
        comparison = sample_comparison
        payload = {
            "state": "success",
            "output": {
                "title": "Codecov Report",
                "summary": "Summary",
                "annotations": list(range(1, 61, 1)),
            },
        }
        mock_repo_provider.create_check_run.return_value = 2234563
        mock_repo_provider.update_check_run.return_value = "success"
        notifier = PatchChecksNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
        )
        expected_calls = [
            {
                "output": {
                    "title": "Codecov Report",
                    "summary": "Summary",
                    "annotations": list(range(1, 51, 1)),
                },
                "url": None,
            },
            {
                "output": {
                    "title": "Codecov Report",
                    "summary": "Summary",
                    "annotations": list(range(51, 61, 1)),
                },
                "url": None,
            },
        ]
        result = notifier.send_notification(sample_comparison, payload)
        assert result.notification_successful == True
        assert result.explanation is None
        calls = [call[1] for call in mock_repo_provider.update_check_run.call_args_list]
        assert expected_calls == calls
        assert mock_repo_provider.update_check_run.call_count == 2
        assert snapshot("json") == result.data_sent

    def test_notify(
        self,
        sample_comparison,
        mocker,
        mock_repo_provider,
        mock_configuration,
        snapshot,
    ):
        mock_repo_provider.get_commit_statuses.return_value = Status([])
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison
        mock_repo_provider.create_check_run.return_value = 2234563
        mock_repo_provider.update_check_run.return_value = "success"
        notifier = PatchChecksNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"paths": ["pathone"]},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
        )
        result = notifier.notify(sample_comparison)
        assert result.notification_successful == True
        assert result.explanation is None
        assert snapshot("json") == result.data_sent

    def test_notify_passing_empty_upload(
        self,
        sample_comparison,
        mocker,
        mock_repo_provider,
        mock_configuration,
        snapshot,
    ):
        mock_repo_provider.get_commit_statuses.return_value = Status([])
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison
        mock_repo_provider.create_check_run.return_value = 2234563
        mock_repo_provider.update_check_run.return_value = "success"
        notifier = PatchChecksNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"paths": ["pathone"]},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
            decoration_type=Decoration.passing_empty_upload,
        )
        result = notifier.notify(sample_comparison)
        assert result.notification_successful == True
        assert result.explanation is None
        assert snapshot("json") == result.data_sent

    def test_notify_failing_empty_upload(
        self,
        sample_comparison,
        mocker,
        mock_repo_provider,
        mock_configuration,
        snapshot,
    ):
        mock_repo_provider.get_commit_statuses.return_value = Status([])
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison
        mock_repo_provider.create_check_run.return_value = 2234563
        mock_repo_provider.update_check_run.return_value = "success"
        notifier = PatchChecksNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"paths": ["pathone"]},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
            decoration_type=Decoration.failing_empty_upload,
        )
        result = notifier.notify(sample_comparison)
        assert result.notification_successful == True
        assert result.explanation is None
        assert snapshot("json") == result.data_sent

    def test_notification_exception(
        self, sample_comparison, mock_repo_provider, mock_configuration
    ):
        mock_repo_provider.get_commit_statuses.return_value = Status([])
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        notifier = PatchChecksNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
        )

        # Test exception handling when there's a TorngitClientError
        mock_repo_provider.get_compare = Mock(
            side_effect=TorngitClientGeneralError(
                400, response_data="Error", message="Error"
            )
        )
        result = notifier.notify(sample_comparison)
        assert result.notification_successful == False
        assert result.explanation == "client_side_error_provider"
        assert result.data_sent is None

        # Test exception handling when there's a TorngitError
        mock_repo_provider.get_compare = Mock(side_effect=TorngitError())
        result = notifier.notify(sample_comparison)
        assert result.notification_successful == False
        assert result.explanation == "server_side_error_provider"
        assert result.data_sent is None

    def test_notification_exception_not_fit(self, sample_comparison, mocker):
        notifier = ChecksNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=None,
        )
        mocker.patch.object(
            ChecksNotifier, "can_we_set_this_status", return_value=False
        )
        result = notifier.notify(sample_comparison)
        assert not result.notification_attempted
        assert result.notification_successful is None
        assert result.explanation == "not_fit_criteria"
        assert result.data_sent is None
        assert result.data_received is None

    def test_notification_exception_preexisting_commit_status(
        self, sample_comparison, mocker, mock_repo_provider
    ):
        comparison = sample_comparison
        notifier = ProjectChecksNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"paths": ["file_1.go"]},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
        )
        mock_repo_provider.get_commit_statuses.return_value = Status(
            [
                {
                    "time": "2024-10-01T22:34:52Z",
                    "state": "success",
                    "description": "42.85% (+0.00%) compared to 36be7f3",
                    "context": "codecov/project/title",
                }
            ]
        )
        result = notifier.notify(sample_comparison)
        assert not result.notification_attempted
        assert result.notification_successful is None
        assert result.explanation == "preexisting_commit_status"
        assert result.data_sent is None
        assert result.data_received is None

    def test_checks_with_after_n_builds(self, sample_comparison, mocker):
        notifier = ChecksNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"flags": ["unit"]},
            notifier_site_settings=True,
            current_yaml=UserYaml(
                {
                    "coverage": {
                        "status": {"project": True, "patch": True, "changes": True}
                    },
                    "flag_management": {
                        "default_rules": {"carryforward": False},
                        "individual_flags": [
                            {
                                "name": "unit",
                                "statuses": [{"type": "patch"}],
                                "after_n_builds": 3,
                            },
                        ],
                    },
                }
            ),
            repository_service=None,
        )

        mocker.patch.object(ChecksNotifier, "can_we_set_this_status", return_value=True)
        result = notifier.notify(sample_comparison)
        assert not result.notification_attempted
        assert result.notification_successful is None
        assert result.explanation == "need_more_builds"
        assert result.data_sent is None
        assert result.data_received is None


class TestChangesChecksNotifier:
    def test_build_payload(
        self, sample_comparison, mock_repo_provider, mock_configuration, snapshot
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        notifier = ChangesChecksNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
        )

        result = notifier.build_payload(sample_comparison)
        assert snapshot("json") == result
        assert notifier.notification_type.value == "checks_changes"

    def test_build_upgrade_payload(
        self, sample_comparison, mock_repo_provider, mock_configuration, snapshot
    ):
        mock_configuration.params["setup"] = {
            "codecov_url": "test.example.br",
            "codecov_dashboard_url": "test.example.br",
        }
        notifier = ChangesChecksNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
            decoration_type=Decoration.upgrade,
        )

        result = notifier.build_payload(sample_comparison)
        assert snapshot("json") == result

    def test_build_payload_with_multiple_changes(
        self,
        comparison_with_multiple_changes,
        mock_repo_provider,
        mock_configuration,
        multiple_diff_changes,
        snapshot,
    ):
        json_diff = multiple_diff_changes
        mock_repo_provider.get_compare.return_value = {"diff": json_diff}

        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        notifier = ChangesChecksNotifier(
            repository=comparison_with_multiple_changes.head.commit.repository,
            title="title",
            notifier_yaml_settings={},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
        )
        result = notifier.build_payload(comparison_with_multiple_changes)
        assert snapshot("json") == result

    def test_build_payload_without_base_report(
        self,
        sample_comparison_without_base_report,
        mock_repo_provider,
        mock_configuration,
        snapshot,
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison_without_base_report
        notifier = ChangesChecksNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
        )
        result = notifier.build_payload(comparison)
        assert snapshot("json") == result

    def test_build_failing_empty_upload_payload(
        self, sample_comparison, mock_repo_provider, mock_configuration, snapshot
    ):
        mock_configuration.params["setup"] = {
            "codecov_url": "test.example.br",
            "codecov_dashboard_url": "test.example.br",
        }
        notifier = ChangesChecksNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
            decoration_type=Decoration.failing_empty_upload,
        )
        result = notifier.build_payload(sample_comparison)
        assert snapshot("json") == result


class TestProjectChecksNotifier:
    def test_analytics_url(
        self, sample_comparison, mock_repo_provider, mock_configuration, snapshot
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "codecov.io"
        repo = sample_comparison.head.commit.repository
        base_commit = sample_comparison.project_coverage_base.commit
        head_commit = sample_comparison.head.commit
        payload = {
            "state": "success",
            "output": {
                "title": f"60.00% (+10.00%) compared to {base_commit.commitid[:7]}",
                "summary": f"[View this Pull Request on Codecov](codecov.io/gh/test_build_default_payload/{repo.name}/pull/{sample_comparison.pull.pullid}?dropdown=coverage&src=pr&el=h1)\n\n60.00% (+10.00%) compared to {base_commit.commitid[:7]}",
                "text": "\n".join(
                    [
                        f"## [Codecov](codecov.io/gh/test_build_default_payload/{repo.name}/pull/{sample_comparison.pull.pullid}?dropdown=coverage&src=pr&el=h1) Report",
                        f"> Merging [#{sample_comparison.pull.pullid}](codecov.io/gh/test_build_default_payload/{repo.name}/pull/{sample_comparison.pull.pullid}?src=pr&el=desc) ({head_commit.commitid[:7]}) into [master](codecov.io/gh/test_build_default_payload/{repo.name}/commit/{sample_comparison.project_coverage_base.commit.commitid}?el=desc) ({base_commit.commitid[:7]}) will **increase** coverage by `10.00%`.",
                        "> The diff coverage is `66.67%`.",
                        "",
                        f"| [Files with missing lines](codecov.io/gh/test_build_default_payload/{repo.name}/pull/{sample_comparison.pull.pullid}?src=pr&el=tree) | Coverage Δ | Complexity Δ | |",
                        "|---|---|---|---|",
                        f"| [file\\_1.go](codecov.io/gh/test_build_default_payload/{repo.name}/pull/{sample_comparison.pull.pullid}?src=pr&el=tree&filepath=file_1.go#diff-ZmlsZV8xLmdv) | `62.50% <66.67%> (+12.50%)` | `10.00 <0.00> (-1.00)` | :arrow_up: |",
                        f"| [file\\_2.py](codecov.io/gh/test_build_default_payload/{repo.name}/pull/{sample_comparison.pull.pullid}?src=pr&el=tree&filepath=file_2.py#diff-ZmlsZV8yLnB5) | `50.00% <0.00%> (ø)` | `0.00% <0.00%> (ø%)` | |",
                        "",
                    ]
                ),
            },
        }
        notifier = ProjectChecksNotifier(
            repository=sample_comparison.head.commit.repository,
            title="default",
            notifier_yaml_settings={},
            notifier_site_settings=True,
            current_yaml={"comment": {"layout": "files"}},
            repository_service=mock_repo_provider,
        )
        result = notifier.send_notification(sample_comparison, payload)
        assert snapshot("json") == result.data_sent

    def test_build_flag_payload(
        self, sample_comparison, mock_repo_provider, mock_configuration, snapshot
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        notifier = ProjectChecksNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"flags": ["flagone"]},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
        )
        result = notifier.build_payload(sample_comparison)
        base_commit = sample_comparison.project_coverage_base.commit

        assert snapshot("json") == result
        assert notifier.notification_type.value == "checks_project"

    def test_build_upgrade_payload(
        self, sample_comparison, mock_repo_provider, mock_configuration, snapshot
    ):
        mock_configuration.params["setup"] = {
            "codecov_url": "test.example.br",
            "codecov_dashboard_url": "test.example.br",
        }
        notifier = ProjectChecksNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
            decoration_type=Decoration.upgrade,
        )
        result = notifier.build_payload(sample_comparison)
        assert snapshot("json") == result

    def test_build_passing_empty_upload_payload(
        self, sample_comparison, mock_repo_provider, mock_configuration, snapshot
    ):
        mock_configuration.params["setup"] = {
            "codecov_url": "test.example.br",
            "codecov_dashboard_url": "test.example.br",
        }
        notifier = ProjectChecksNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
            decoration_type=Decoration.passing_empty_upload,
        )
        result = notifier.build_payload(sample_comparison)
        assert snapshot("json") == result

    @pytest.mark.django_db
    def test_build_default_payload(
        self, sample_comparison, mock_repo_provider, mock_configuration, snapshot
    ):
        mock_all_plans_and_tiers()
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        notifier = ProjectChecksNotifier(
            repository=sample_comparison.head.commit.repository,
            title="default",
            notifier_yaml_settings={},
            notifier_site_settings=True,
            current_yaml={"comment": {"layout": "files"}},
            repository_service=mock_repo_provider,
        )
        result = notifier.build_payload(sample_comparison)
        assert snapshot("json") == result

    @pytest.mark.django_db
    def test_build_default_payload_with_flags(
        self, sample_comparison, mock_repo_provider, mock_configuration, snapshot
    ):
        mock_all_plans_and_tiers()
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        notifier = ProjectChecksNotifier(
            repository=sample_comparison.head.commit.repository,
            title="default",
            notifier_yaml_settings={},
            notifier_site_settings=True,
            current_yaml={"comment": {"layout": "files, flags"}},
            repository_service=mock_repo_provider,
        )
        result = notifier.build_payload(sample_comparison)
        assert snapshot("json") == result

    @pytest.mark.django_db
    def test_build_default_payload_with_flags_and_footer(
        self, sample_comparison, mock_repo_provider, mock_configuration, snapshot
    ):
        mock_all_plans_and_tiers()
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        notifier = ProjectChecksNotifier(
            repository=sample_comparison.head.commit.repository,
            title="default",
            notifier_yaml_settings={},
            notifier_site_settings=True,
            current_yaml={"comment": {"layout": "files, flags, footer"}},
            repository_service=mock_repo_provider,
        )
        result = notifier.build_payload(sample_comparison)
        assert snapshot("json") == result

    def test_build_default_payload_comment_off(
        self, sample_comparison, mock_repo_provider, mock_configuration, snapshot
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        notifier = ProjectChecksNotifier(
            repository=sample_comparison.head.commit.repository,
            title="default",
            notifier_yaml_settings={},
            notifier_site_settings=True,
            current_yaml={"comment": False},
            repository_service=mock_repo_provider,
        )
        result = notifier.build_payload(sample_comparison)
        assert snapshot("json") == result

    def test_build_default_payload_negative_change_comment_off(
        self,
        sample_comparison_negative_change,
        mock_repo_provider,
        mock_configuration,
        snapshot,
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        notifier = ProjectChecksNotifier(
            repository=sample_comparison_negative_change.head.commit.repository,
            title="default",
            notifier_yaml_settings={"removed_code_behavior": "removals_only"},
            notifier_site_settings=True,
            current_yaml={"comment": False},
            repository_service=mock_repo_provider,
        )
        result = notifier.build_payload(sample_comparison_negative_change)
        assert snapshot("json") == result

    def test_build_payload_not_auto(
        self, sample_comparison, mock_repo_provider, mock_configuration, snapshot
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        notifier = ProjectChecksNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"target": "57%", "flags": ["flagone"]},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
        )
        result = notifier.build_payload(sample_comparison)
        assert snapshot("json") == result

    def test_build_payload_no_base_report(
        self,
        sample_comparison_without_base_report,
        mock_repo_provider,
        mock_configuration,
        snapshot,
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison_without_base_report
        notifier = ProjectChecksNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"flags": ["flagone"]},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
        )
        result = notifier.build_payload(comparison)
        assert snapshot("json") == result

    def test_check_notify_no_path_match(
        self,
        sample_comparison,
        mocker,
        mock_repo_provider,
        mock_configuration,
        snapshot,
    ):
        mock_repo_provider.get_commit_statuses.return_value = Status([])
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison
        mock_repo_provider.create_check_run.return_value = 2234563
        mock_repo_provider.update_check_run.return_value = "success"

        notifier = ProjectChecksNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"paths": ["pathone"]},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
        )
        result = notifier.notify(sample_comparison)
        assert snapshot("json") == result.to_dict()

    def test_check_notify_single_path_match(
        self,
        sample_comparison,
        mocker,
        mock_repo_provider,
        mock_configuration,
        snapshot,
    ):
        mock_repo_provider.get_commit_statuses.return_value = Status([])
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison
        mock_repo_provider.create_check_run.return_value = 2234563
        mock_repo_provider.update_check_run.return_value = "success"

        notifier = ProjectChecksNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"paths": ["file_1.go"]},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
        )

        result = notifier.notify(sample_comparison)
        assert snapshot("json") == result.to_dict()

    def test_check_notify_multiple_path_match(
        self,
        sample_comparison,
        mocker,
        mock_repo_provider,
        mock_configuration,
        snapshot,
    ):
        mock_repo_provider.get_commit_statuses.return_value = Status([])
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison
        mock_repo_provider.create_check_run.return_value = 2234563
        mock_repo_provider.update_check_run.return_value = "success"

        notifier = ProjectChecksNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"paths": ["file_2.py", "file_1.go"]},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
        )

        result = notifier.notify(sample_comparison)
        assert snapshot("json") == result.to_dict()

    def test_check_notify_with_paths(
        self,
        sample_comparison,
        mocker,
        mock_repo_provider,
        mock_configuration,
        snapshot,
    ):
        mock_repo_provider.get_commit_statuses.return_value = Status([])
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison

        mock_repo_provider.create_check_run.return_value = 2234563
        mock_repo_provider.update_check_run.return_value = "success"

        notifier = ProjectChecksNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={},
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
        )
        result = notifier.notify(sample_comparison)
        assert snapshot("json") == result.to_dict()

    def test_notify_pass_behavior_when_coverage_not_uploaded(
        self,
        sample_comparison_coverage_carriedforward,
        mock_repo_provider,
        mock_configuration,
        snapshot,
    ):
        mock_repo_provider.get_commit_statuses.return_value = Status([])
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        mock_repo_provider.create_check_run.return_value = 2234563
        mock_repo_provider.update_check_run.return_value = "success"
        notifier = ProjectChecksNotifier(
            repository=sample_comparison_coverage_carriedforward.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "flag_coverage_not_uploaded_behavior": "pass",
                "flags": ["integration", "missing"],
            },
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
        )
        result = notifier.notify(sample_comparison_coverage_carriedforward)
        assert snapshot("json") == result.to_dict()

    def test_notify_pass_behavior_when_coverage_uploaded(
        self,
        sample_comparison_coverage_carriedforward,
        mock_repo_provider,
        mock_configuration,
        snapshot,
    ):
        mock_repo_provider.get_commit_statuses.return_value = Status([])
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        mock_repo_provider.create_check_run.return_value = 2234563
        mock_repo_provider.update_check_run.return_value = "success"
        notifier = ProjectChecksNotifier(
            repository=sample_comparison_coverage_carriedforward.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "flag_coverage_not_uploaded_behavior": "pass",
                "flags": ["unit"],
            },
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
        )

        result = notifier.notify(sample_comparison_coverage_carriedforward)
        assert snapshot("json") == result.to_dict()

    def test_notify_include_behavior_when_coverage_not_uploaded(
        self,
        sample_comparison_coverage_carriedforward,
        mock_repo_provider,
        mock_configuration,
        snapshot,
    ):
        mock_repo_provider.get_commit_statuses.return_value = Status([])
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        mock_repo_provider.create_check_run.return_value = 2234563
        mock_repo_provider.update_check_run.return_value = "success"
        notifier = ProjectChecksNotifier(
            repository=sample_comparison_coverage_carriedforward.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "flag_coverage_not_uploaded_behavior": "include",
                "flags": ["integration", "enterprise"],
            },
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
        )

        result = notifier.notify(sample_comparison_coverage_carriedforward)
        assert snapshot("json") == result.to_dict()

    def test_notify_exclude_behavior_when_coverage_not_uploaded(
        self,
        sample_comparison_coverage_carriedforward,
        mock_repo_provider,
        mock_configuration,
        snapshot,
    ):
        mock_repo_provider.get_commit_statuses.return_value = Status([])
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        mock_repo_provider.create_check_run.return_value = 2234563
        mock_repo_provider.update_check_run.return_value = "success"
        notifier = ProjectChecksNotifier(
            repository=sample_comparison_coverage_carriedforward.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "flag_coverage_not_uploaded_behavior": "exclude",
                "flags": ["missing"],
            },
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
        )

        result = notifier.notify(sample_comparison_coverage_carriedforward)
        assert snapshot("json") == result.to_dict()

    def test_notify_exclude_behavior_when_coverage_uploaded(
        self,
        sample_comparison_coverage_carriedforward,
        mock_repo_provider,
        mock_configuration,
        snapshot,
    ):
        mock_repo_provider.get_commit_statuses.return_value = Status([])
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        mock_repo_provider.create_check_run.return_value = 2234563
        mock_repo_provider.update_check_run.return_value = "success"
        notifier = ProjectChecksNotifier(
            repository=sample_comparison_coverage_carriedforward.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "flag_coverage_not_uploaded_behavior": "exclude",
                "flags": ["unit"],
            },
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
        )

        result = notifier.notify(sample_comparison_coverage_carriedforward)
        assert snapshot("json") == result.to_dict()

    def test_notify_exclude_behavior_when_some_coverage_uploaded(
        self,
        sample_comparison_coverage_carriedforward,
        mock_repo_provider,
        mock_configuration,
        snapshot,
    ):
        mock_repo_provider.get_commit_statuses.return_value = Status([])
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        mock_repo_provider.create_check_run.return_value = 2234563
        mock_repo_provider.update_check_run.return_value = "success"
        notifier = ProjectChecksNotifier(
            repository=sample_comparison_coverage_carriedforward.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "flag_coverage_not_uploaded_behavior": "exclude",
                "flags": [
                    "unit",
                    "missing",
                    "integration",
                ],  # only "unit" was uploaded, but this should still notify
            },
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
        )

        result = notifier.notify(sample_comparison_coverage_carriedforward)
        assert snapshot("json") == result.to_dict()

    def test_notify_exclude_behavior_no_flags(
        self,
        sample_comparison_coverage_carriedforward,
        mock_repo_provider,
        mock_configuration,
        snapshot,
    ):
        mock_repo_provider.get_commit_statuses.return_value = Status([])
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        mock_repo_provider.create_check_run.return_value = 2234563
        mock_repo_provider.update_check_run.return_value = "success"
        notifier = ProjectChecksNotifier(
            repository=sample_comparison_coverage_carriedforward.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "flag_coverage_not_uploaded_behavior": "exclude",
                "flags": None,
            },
            notifier_site_settings=True,
            current_yaml=UserYaml({}),
            repository_service=mock_repo_provider,
        )

        # should send the check as normal if there are no flags
        result = notifier.notify(sample_comparison_coverage_carriedforward)
        assert snapshot("json") == result.to_dict()

    def test_build_payload_comments_true(
        self, sample_comparison, mock_configuration, snapshot
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        notifier = ProjectChecksNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={},
            notifier_site_settings={},
            current_yaml={"comment": True},
            repository_service=None,
        )
        res = notifier.build_payload(sample_comparison)
        assert snapshot("json") == res

    def test_build_payload_comments_false(
        self, sample_comparison, mock_configuration, snapshot
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        notifier = ProjectChecksNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={},
            notifier_site_settings={},
            current_yaml={"comment": False},
            repository_service=None,
        )
        res = notifier.build_payload(sample_comparison)
        assert snapshot("json") == res
