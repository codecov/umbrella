from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import PropertyMock

import pytest

from database.models.core import Commit, Pull, Repository
from database.tests.factories import RepositoryFactory
from database.tests.factories.core import (
    CommitFactory,
    GithubAppInstallationFactory,
    OwnerFactory,
    PullFactory,
)
from services.comparison import ComparisonContext, ComparisonProxy
from services.comparison.types import Comparison, FullCommit, ReportUploadedCount
from services.decoration import Decoration
from services.notification.notifiers.base import NotificationResult
from services.notification.notifiers.comment import CommentNotifier
from services.notification.notifiers.mixins.message.helpers import (
    diff_to_string,
    format_number_to_str,
    sort_by_importance,
)
from services.notification.notifiers.mixins.message.sections import (
    AnnouncementSectionWriter,
    ComponentsSectionWriter,
    FileSectionWriter,
    HeaderSectionWriter,
    MessagesToUserSectionWriter,
    NewFilesSectionWriter,
    NewFooterSectionWriter,
    _get_tree_cell,
)
from services.notification.notifiers.tests.conftest import generate_sample_comparison
from services.repository import EnrichedPull
from services.yaml.reader import get_components_from_yaml
from shared.plan.constants import DEFAULT_FREE_PLAN, PlanName
from shared.reports.readonly import ReadOnlyReport
from shared.reports.resources import Report, ReportFile
from shared.reports.types import Change, ReportLine, ReportTotals
from shared.torngit.exceptions import (
    TorngitClientError,
    TorngitClientGeneralError,
    TorngitObjectNotFoundError,
    TorngitServerUnreachableError,
)
from shared.utils.sessions import Session
from shared.validation.types import CoverageCommentRequiredChanges
from tests.helpers import mock_all_plans_and_tiers


@pytest.fixture
def is_not_first_pull(mocker):
    mocker.patch(
        "database.models.core.Pull.is_first_coverage_pull",
        return_value=False,
        new_callable=PropertyMock,
    )


@pytest.fixture
def sample_comparison_bunch_empty_flags(request, dbsession, mocker):
    """
    This is what this fixture has regarding to flags
    - first is on both reports, both with 100% coverage (the common already existing result)
    - second is declared on both reports, but is only on the head report, with 50% coverage
    - third is declared on both reports, but only has coverage information on the base report,
    with 50% coverage
    - fourth is declared on both reports, and has no coverage information anywhere
    - fifthis only declared at the head report, but has no coverage information there
    - sixth is only declared at the head report, and has information: 100% coverage there
    - seventh is only declared at the base report, but has no coverage information there
    - eighth is only declared at the base report, and has 100% coverage there
    """
    head_report = Report()
    head_report.add_session(Session(flags=["first"]))
    head_report.add_session(Session(flags=["second"]))
    head_report.add_session(Session(flags=["third"]))
    head_report.add_session(Session(flags=["fourth"]))
    head_report.add_session(Session(flags=["fifth"]))
    head_report.add_session(Session(flags=["sixth"]))
    base_report = Report()
    base_report.add_session(Session(flags=["first"]))
    base_report.add_session(Session(flags=["second"]))
    base_report.add_session(Session(flags=["third"]))
    base_report.add_session(Session(flags=["fourth"]))
    base_report.add_session(Session(flags=["seventh"]))
    base_report.add_session(Session(flags=["eighth"]))
    # assert False
    file_1 = ReportFile("space.py")
    file_1.append(1, ReportLine.create(1, sessions=[(0, 1), (1, "1/2")]))
    file_1.append(40, ReportLine.create(1, sessions=[(5, 1), (1, 1)]))
    head_report.append(file_1)
    file_2 = ReportFile("jupiter.py")
    file_2.append(1, ReportLine.create(1, sessions=[(0, 1), (2, "1/2")]))
    file_2.append(40, ReportLine.create(1, sessions=[(5, 1), (2, 1)]))
    base_report.append(file_2)
    mocker.patch(
        "shared.bots.github_apps.get_github_integration_token",
        return_value="github-integration-token",
    )
    return generate_sample_comparison(
        "test-owner",
        dbsession,
        ReadOnlyReport.create_from_report(base_report),
        ReadOnlyReport.create_from_report(head_report),
    )


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
                "commitid": "{comparison.project_coverage_base.commit[:7]}44fdd29fcc506317cc3ddeae1a723dd08",
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

    branch_result = {"name": "test", "sha": "aaaaaaa"}

    mock_repo_provider.get_compare.return_value = compare_result
    mock_repo_provider.post_comment.return_value = {}
    mock_repo_provider.edit_comment.return_value = {}
    mock_repo_provider.delete_comment.return_value = {}
    mock_repo_provider.get_branch.side_effect = TorngitClientGeneralError(
        404, None, "Branch not found"
    )
    return mock_repo_provider


class TestCommentNotifierHelpers:
    @pytest.mark.django_db
    def test_sort_by_importance(self):
        modified_change = Change(
            path="modified.py",
            in_diff=True,
            totals=ReportTotals(
                files=0,
                lines=0,
                hits=-2,
                misses=1,
                partials=0,
                coverage=-23.333330000000004,
                branches=0,
                methods=0,
                messages=0,
                sessions=0,
                complexity=0,
                complexity_total=0,
                diff=0,
            ),
        )
        renamed_with_changes_change = Change(
            path="renamed_with_changes.py",
            in_diff=True,
            old_path="old_renamed_with_changes.py",
            totals=ReportTotals(
                files=0,
                lines=0,
                hits=-1,
                misses=1,
                partials=0,
                coverage=-20.0,
                branches=0,
                methods=0,
                messages=0,
                sessions=0,
                complexity=0,
                complexity_total=0,
                diff=0,
            ),
        )
        unrelated_change = Change(
            path="unrelated.py",
            in_diff=False,
            totals=ReportTotals(
                files=0,
                lines=0,
                hits=-3,
                misses=2,
                partials=0,
                coverage=-43.333330000000004,
                branches=0,
                methods=0,
                messages=0,
                sessions=0,
                complexity=0,
                complexity_total=0,
                diff=0,
            ),
        )
        added_change = Change(
            path="added.py", new=True, in_diff=None, old_path=None, totals=None
        )
        deleted_change = Change(path="deleted.py", deleted=True)
        changes = [
            modified_change,
            renamed_with_changes_change,
            unrelated_change,
            added_change,
            deleted_change,
        ]
        res = sort_by_importance(changes)
        expected_result = [
            unrelated_change,
            modified_change,
            renamed_with_changes_change,
            deleted_change,
            added_change,
        ]
        assert expected_result == res

    @pytest.mark.django_db
    def test_format_number_to_str(self):
        assert "<0.1" == format_number_to_str(
            {"coverage": {"precision": 1}}, Decimal("0.001")
        )
        assert "10.0" == format_number_to_str(
            {"coverage": {"precision": 1}}, Decimal("10.001")
        )
        assert "10.1" == format_number_to_str(
            {"coverage": {"precision": 1, "round": "up"}}, Decimal("10.001")
        )

    @pytest.mark.django_db
    def test_diff_to_string_case_1(self):
        case_1 = (
            "master",
            ReportTotals(10),
            "stable",
            ReportTotals(11),
            [
                "@@         Coverage Diff          @@",
                "##        master   stable   +/-   ##",
                "====================================",
                "====================================",
                "  Files       10       11    +1     ",
                "",
            ],
        )
        case = case_1
        base_title, base_totals, head_title, head_totals, expected_result = case
        diff = diff_to_string({}, base_title, base_totals, head_title, head_totals)
        assert diff == expected_result

    @pytest.mark.django_db
    def test_diff_to_string_case_2(self):
        case_2 = (
            "master",
            ReportTotals(files=10, coverage="12.0", complexity="10.0"),
            "stable",
            ReportTotals(files=10, coverage="15.0", complexity="9.0"),
            [
                "@@             Coverage Diff              @@",
                "##             master   stable      +/-   ##",
                "============================================",
                "+ Coverage     12.00%   15.00%   +3.00%     ",
                "- Complexity   10.00%    9.00%   -1.00%     ",
                "============================================",
                "  Files            10       10              ",
                "",
            ],
        )
        case = case_2
        base_title, base_totals, head_title, head_totals, expected_result = case
        diff = diff_to_string({}, base_title, base_totals, head_title, head_totals)
        assert diff == expected_result

    @pytest.mark.django_db
    def test_diff_to_string_case_3(self):
        case_3 = (
            "master",
            ReportTotals(files=100),
            "#1",
            ReportTotals(files=200, lines=2, hits=6, misses=7, partials=8, branches=3),
            [
                "@@          Coverage Diff          @@",
                "##           master    #1    +/-   ##",
                "=====================================",
                "=====================================",
                "  Files         100   200   +100     ",
                "  Lines           0     2     +2     ",
                "  Branches        0     3     +3     ",
                "=====================================",
                "+ Hits            0     6     +6     ",
                "- Misses          0     7     +7     ",
                "- Partials        0     8     +8     ",
            ],
        )
        case = case_3
        base_title, base_totals, head_title, head_totals, expected_result = case
        diff = diff_to_string({}, base_title, base_totals, head_title, head_totals)
        assert diff == expected_result

    @pytest.mark.django_db
    def test_diff_to_string_case_4(self):
        case_4 = (
            "master",
            ReportTotals(files=10, coverage="12.0", complexity=10),
            "stable",
            ReportTotals(files=10, coverage="15.0", complexity=9),
            [
                "@@             Coverage Diff              @@",
                "##             master   stable      +/-   ##",
                "============================================",
                "+ Coverage     12.00%   15.00%   +3.00%     ",
                "+ Complexity       10        9       -1     ",
                "============================================",
                "  Files            10       10              ",
                "",
            ],
        )
        case = case_4
        base_title, base_totals, head_title, head_totals, expected_result = case
        diff = diff_to_string({}, base_title, base_totals, head_title, head_totals)
        assert diff == expected_result

    @pytest.mark.django_db
    def test_diff_to_string_case_different_types(self):
        case_1 = (
            "master",
            ReportTotals(10, coverage="54.43"),
            "stable",
            ReportTotals(11, coverage=0),
            [
                "@@             Coverage Diff             @@",
                "##           master   stable       +/-   ##",
                "===========================================",
                "- Coverage   54.43%        0   -54.43%     ",
                "===========================================",
                "  Files          10       11        +1     ",
                "",
            ],
        )
        case = case_1
        base_title, base_totals, head_title, head_totals, expected_result = case
        diff = diff_to_string({}, base_title, base_totals, head_title, head_totals)
        assert diff == expected_result


@pytest.mark.usefixtures("is_not_first_pull")
class TestCommentNotifier:
    @pytest.fixture(autouse=True)
    def setup(self):
        mock_all_plans_and_tiers()

    @pytest.mark.django_db
    def test_is_enabled_settings_individual_settings_false(self, dbsession):
        repository = RepositoryFactory.create()
        dbsession.add(repository)
        dbsession.flush()
        notifier = CommentNotifier(
            repository=repository,
            title="some_title",
            notifier_yaml_settings=False,
            notifier_site_settings=None,
            current_yaml={},
            repository_service=None,
        )
        assert not notifier.is_enabled()

    @pytest.mark.django_db
    def test_is_enabled_settings_individual_settings_none(self, dbsession):
        repository = RepositoryFactory.create()
        dbsession.add(repository)
        dbsession.flush()
        notifier = CommentNotifier(
            repository=repository,
            title="some_title",
            notifier_yaml_settings=None,
            notifier_site_settings=None,
            current_yaml={},
            repository_service=None,
        )
        assert not notifier.is_enabled()

    @pytest.mark.django_db
    def test_is_enabled_settings_individual_settings_true(self, dbsession):
        repository = RepositoryFactory.create()
        dbsession.add(repository)
        dbsession.flush()
        notifier = CommentNotifier(
            repository=repository,
            title="some_title",
            notifier_yaml_settings=True,
            notifier_site_settings=None,
            current_yaml={},
            repository_service=None,
        )
        assert not notifier.is_enabled()

    @pytest.mark.django_db
    def test_is_enabled_settings_individual_settings_dict(self, dbsession):
        repository = RepositoryFactory.create()
        dbsession.add(repository)
        dbsession.flush()
        notifier = CommentNotifier(
            repository=repository,
            title="some_title",
            notifier_yaml_settings={"layout": "reach, diff, flags, files, footer"},
            notifier_site_settings=None,
            current_yaml={},
            repository_service=None,
        )
        assert notifier.is_enabled()

    @pytest.mark.django_db
    def test_create_message_files_section(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_comparison,
        mocker,
        snapshot,
    ):
        comparison = sample_comparison
        mocker.patch.object(comparison, "get_behind_by", return_value=0)
        mocker.patch.object(
            comparison,
            "get_diff",
            return_value={
                "files": {
                    "file_1.go": {
                        "type": "modified",
                        "before": None,
                        "segments": [
                            {
                                "header": ["105", "8", "105", "9"],
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
                                "header": ["1046", "12", "1047", "19"],
                                "lines": [
                                    " ",
                                    " You may need to configure a ``.coveragerc`` file. Learn more",
                                    " ",
                                    "-We highly suggest adding `source` to your ``.coveragerc``",
                                    "+We highly suggest adding ``source`` to your ``.coveragerc`",
                                    " ",
                                    " .. code-block:: ini",
                                    " ",
                                    "    [run]",
                                    "    source=your_package_name",
                                    "+   ",
                                    "+If there are multiple sources, you instead should add ``include",
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
                                "header": ["10150", "5", "10158", "4"],
                                "lines": [
                                    " * Twitter: `@codecov <https://twitter.com/codecov>`_.",
                                    " * Email: `hello@codecov.io <hello@codecov.io>`_.",
                                    " ",
                                    "-We are happy to help if you have any questions. ",
                                    "-",
                                    "+We are happy to help if you have any questions. .",
                                ],
                            },
                        ],
                        "stats": {"added": 11, "removed": 4},
                    },
                    "file_2.py": {
                        "type": "modified",
                        "before": None,
                        "segments": [
                            {
                                "header": ["10", "8", "10", "9"],
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
                                "header": ["50", "12", "51", "19"],
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
                        ],
                        "stats": {"added": 11, "removed": 4},
                    },
                }
            },
        )
        pull_dict = {"base": {"branch": "master"}}
        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"layout": "files"},
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        pull = comparison.pull
        repository = sample_comparison.head.commit.repository
        res = notifier.create_message(comparison, pull_dict, {"layout": "files"})
        assert snapshot("txt") == "\n".join(res)

    @pytest.mark.django_db
    def test_create_message_with_github_app_comment(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_comparison,
        mocker,
    ):
        comparison = sample_comparison
        comparison.context = ComparisonContext(gh_is_using_codecov_commenter=True)
        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "files",
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        res = notifier.build_message(comparison)
        assert (
            res[0]
            == ":warning: Please install the !['codecov app svg image'](https://github.com/codecov/engineering-team/assets/152432831/e90313f4-9d3a-4b63-8b54-cfe14e7ec20d) to ensure uploads and comments are reliably processed by Codecov."
        )

    @pytest.mark.django_db
    def test_build_message(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_comparison,
        snapshot,
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison
        pull = comparison.pull
        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"layout": "reach, diff, flags, files, footer"},
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        repository = sample_comparison.head.commit.repository
        result = notifier.build_message(comparison)
        assert snapshot("txt") == "\n".join(result)

    @pytest.mark.django_db
    def test_build_message_flags_empty_coverage(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_comparison_bunch_empty_flags,
        snapshot,
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison_bunch_empty_flags
        pull = comparison.pull
        notifier = CommentNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"layout": "flags"},
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        repository = comparison.head.commit.repository
        result = notifier.build_message(comparison)
        assert snapshot("txt") == "\n".join(result)

    @pytest.mark.django_db
    def test_build_message_more_sections(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_comparison,
        snapshot,
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison
        pull = comparison.pull
        all_sections = [
            "reach, diff, flags, files, footer",
            "changes",
            "file",
            "header",
            "suggestions",
            "sunburst",
            "uncovered",
            "random_section",
        ]
        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"layout": ",".join(all_sections)},
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        repository = sample_comparison.head.commit.repository
        result = notifier.build_message(comparison)
        assert snapshot("txt") == "\n".join(result)

    @pytest.mark.django_db
    def test_build_upgrade_message(
        self,
        request,
        dbsession,
        mocker,
        mock_configuration,
        with_sql_functions,
        sample_comparison,
        snapshot,
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        mocker.patch("services.license.is_enterprise", return_value=False)
        comparison = sample_comparison
        pull = comparison.enriched_pull.database_pull
        repository = sample_comparison.head.commit.repository
        notifier = CommentNotifier(
            repository=repository,
            title="title",
            notifier_yaml_settings={"layout": "reach, diff, flags, files, footer"},
            notifier_site_settings=True,
            current_yaml={},
            repository_service=None,
            decoration_type=Decoration.upgrade,
        )
        result = notifier.build_message(comparison)
        assert snapshot("txt") == "\n".join(result)

    @pytest.mark.django_db
    def test_build_limited_upload_message(
        self,
        request,
        dbsession,
        mocker,
        mock_configuration,
        with_sql_functions,
        sample_comparison,
        snapshot,
    ):
        mock_configuration.params["setup"] = {
            "codecov_url": "test.example.br",
            "codecov_dashboard_url": "test.example.br",
        }
        comparison = sample_comparison
        pull = comparison.enriched_pull.database_pull
        repository = sample_comparison.head.commit.repository
        notifier = CommentNotifier(
            repository=repository,
            title="title",
            notifier_yaml_settings={"layout": "reach, diff, flags, files, footer"},
            notifier_site_settings=True,
            current_yaml={},
            repository_service=None,
            decoration_type=Decoration.upload_limit,
        )
        result = notifier.build_message(comparison)
        assert snapshot("txt") == "\n".join(result)

    @pytest.mark.django_db
    def test_build_passing_empty_upload(
        self,
        request,
        dbsession,
        mocker,
        mock_configuration,
        with_sql_functions,
        sample_comparison,
        snapshot,
    ):
        mock_configuration.params["setup"] = {
            "codecov_url": "test.example.br",
            "codecov_dashboard_url": "test.example.br",
        }
        comparison = sample_comparison
        repository = sample_comparison.head.commit.repository
        notifier = CommentNotifier(
            repository=repository,
            title="title",
            notifier_yaml_settings={"layout": "reach, diff, flags, files, footer"},
            notifier_site_settings=True,
            current_yaml={},
            repository_service=None,
            decoration_type=Decoration.passing_empty_upload,
        )
        result = notifier.build_message(comparison)
        assert snapshot("txt") == "\n".join(result)

    @pytest.mark.django_db
    def test_build_failing_empty_upload(
        self,
        request,
        dbsession,
        mocker,
        mock_configuration,
        with_sql_functions,
        sample_comparison,
        snapshot,
    ):
        mock_configuration.params["setup"] = {
            "codecov_url": "test.example.br",
            "codecov_dashboard_url": "test.example.br",
        }
        comparison = sample_comparison
        repository = sample_comparison.head.commit.repository
        notifier = CommentNotifier(
            repository=repository,
            title="title",
            notifier_yaml_settings={"layout": "reach, diff, flags, files, footer"},
            notifier_site_settings=True,
            current_yaml={},
            repository_service=None,
            decoration_type=Decoration.failing_empty_upload,
        )
        result = notifier.build_message(comparison)
        assert snapshot("txt") == "\n".join(result)

    @pytest.mark.django_db
    def test_processing_upload(
        self,
        request,
        dbsession,
        mocker,
        mock_configuration,
        with_sql_functions,
        sample_comparison,
        snapshot,
    ):
        mock_configuration.params["setup"] = {
            "codecov_url": "test.example.br",
            "codecov_dashboard_url": "test.example.br",
        }
        comparison = sample_comparison
        repository = sample_comparison.head.commit.repository
        notifier = CommentNotifier(
            repository=repository,
            title="title",
            notifier_yaml_settings={"layout": "reach, diff, flags, files, footer"},
            notifier_site_settings=True,
            current_yaml={},
            repository_service=None,
            decoration_type=Decoration.processing_upload,
        )
        result = notifier.build_message(comparison)
        assert snapshot("txt") == "\n".join(result)

    @pytest.mark.django_db
    def test_build_upgrade_message_enterprise(
        self,
        request,
        dbsession,
        mocker,
        mock_configuration,
        with_sql_functions,
        sample_comparison,
        snapshot,
    ):
        mocker.patch("services.license.is_enterprise", return_value=True)

        encrypted_license = "wxWEJyYgIcFpi6nBSyKQZQeaQ9Eqpo3SXyUomAqQOzOFjdYB3A8fFM1rm+kOt2ehy9w95AzrQqrqfxi9HJIb2zLOMOB9tSy52OykVCzFtKPBNsXU/y5pQKOfV7iI3w9CHFh3tDwSwgjg8UsMXwQPOhrpvl2GdHpwEhFdaM2O3vY7iElFgZfk5D9E7qEnp+WysQwHKxDeKLI7jWCnBCBJLDjBJRSz0H7AfU55RQDqtTrnR+rsLDHOzJ80/VxwVYhb"
        mock_configuration.params["setup"]["enterprise_license"] = encrypted_license
        mock_configuration.params["setup"]["codecov_dashboard_url"] = (
            "https://codecov.mysite.com"
        )

        comparison = sample_comparison
        pull = comparison.enriched_pull.database_pull
        repository = sample_comparison.head.commit.repository
        notifier = CommentNotifier(
            repository=repository,
            title="title",
            notifier_yaml_settings={"layout": "reach, diff, flags, files, footer"},
            notifier_site_settings=True,
            current_yaml={},
            repository_service=None,
            decoration_type=Decoration.upgrade,
        )
        result = notifier.build_message(comparison)
        assert snapshot("txt") == "\n".join(result)

    @pytest.mark.django_db
    def test_build_message_hide_complexity(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_comparison,
        snapshot,
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison
        pull = comparison.pull
        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"layout": "reach, diff, flags, files, footer"},
            notifier_site_settings=True,
            current_yaml={"codecov": {"ui": {"hide_complexity": True}}},
            repository_service=mock_repo_provider,
        )
        repository = sample_comparison.head.commit.repository
        result = notifier.build_message(comparison)
        assert snapshot("txt") == "\n".join(result)

    @pytest.mark.django_db
    def test_build_message_no_base_report(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_comparison_without_base_report,
        snapshot,
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison_without_base_report
        pull = comparison.pull
        notifier = CommentNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"layout": "reach, diff, flags, files, footer"},
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        repository = comparison.head.commit.repository
        result = notifier.build_message(comparison)
        assert snapshot("txt") == "\n".join(result)

    @pytest.mark.django_db
    def test_build_message_no_base_commit(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_comparison_without_base_with_pull,
        snapshot,
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison_without_base_with_pull
        pull = comparison.pull
        notifier = CommentNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"layout": "reach, diff, flags, files, footer"},
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        repository = comparison.head.commit.repository
        result = notifier.build_message(comparison)

        assert snapshot("txt") == "\n".join(result)

    @pytest.mark.django_db
    def test_build_message_no_change(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_comparison_no_change,
        snapshot,
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison_no_change
        pull = comparison.pull

        notifier = CommentNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"layout": "reach, diff, flags, files, footer"},
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        repository = comparison.head.commit.repository
        result = notifier.build_message(comparison)

        assert snapshot("txt") == "\n".join(result)

    @pytest.mark.django_db
    def test_build_message_negative_change(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_comparison_negative_change,
        snapshot,
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison_negative_change
        pull = comparison.pull
        notifier = CommentNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"layout": "reach, diff, flags, files, footer"},
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        repository = comparison.head.commit.repository
        result = notifier.build_message(comparison)
        assert snapshot("txt") == "\n".join(result)

    @pytest.mark.django_db
    def test_build_message_negative_change_tricky_rounding(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_comparison_negative_change,
        snapshot,
    ):
        # This example was taken from a real PR in which we had issues with rounding
        # That's why the numbers will be.... dramatic
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison_negative_change
        pull = comparison.pull
        notifier = CommentNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"layout": "diff"},
            notifier_site_settings=True,
            current_yaml={"coverage": {"precision": 2, "round": "down"}},
            repository_service=mock_repo_provider,
        )
        repository = comparison.head.commit.repository
        # Change the reports
        new_base_report = Report()
        new_base_file = ReportFile("file.py")
        # Produce numbers that we know can be tricky for rounding
        for i in range(1, 6760):
            new_base_file.append(i, ReportLine.create(1))
        for i in range(6760, 7631):
            new_base_file.append(i, ReportLine.create(0))
        new_base_report.append(new_base_file)
        comparison.project_coverage_base.report = ReadOnlyReport.create_from_report(
            new_base_report
        )
        new_head_report = Report()
        new_head_file = ReportFile("file.py")
        for i in range(1, 6758):
            new_head_file.append(i, ReportLine.create(1))
        for i in range(6758, 7632):
            new_head_file.append(i, ReportLine.create(0))
        new_head_report.append(new_head_file)
        comparison.head.report = ReadOnlyReport.create_from_report(new_head_report)
        result = notifier.build_message(comparison)

        assert snapshot("txt") == "\n".join(result)

    @pytest.mark.django_db
    def test_build_message_negative_change_tricky_rounding_newheader(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_comparison_negative_change,
        snapshot,
    ):
        # This example was taken from a real PR in which we had issues with rounding
        # That's why the numbers will be.... dramatic
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison_negative_change
        pull = comparison.pull
        notifier = CommentNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"layout": "newheader"},
            notifier_site_settings=True,
            current_yaml={"coverage": {"precision": 2, "round": "down"}},
            repository_service=mock_repo_provider,
        )
        repository = comparison.head.commit.repository
        # Change the reports
        new_base_report = Report()
        new_base_file = ReportFile("file.py")
        # Produce numbers that we know can be tricky for rounding
        for i in range(1, 6760):
            new_base_file.append(i, ReportLine.create(1))
        for i in range(6760, 7631):
            new_base_file.append(i, ReportLine.create(0))
        new_base_report.append(new_base_file)
        comparison.project_coverage_base.report = ReadOnlyReport.create_from_report(
            new_base_report
        )
        new_head_report = Report()
        new_head_file = ReportFile("file.py")
        for i in range(1, 6758):
            new_head_file.append(i, ReportLine.create(1))
        for i in range(6758, 7632):
            new_head_file.append(i, ReportLine.create(0))
        new_head_report.append(new_head_file)
        comparison.head.report = ReadOnlyReport.create_from_report(new_head_report)
        result = notifier.build_message(comparison)
        assert snapshot("txt") == "\n".join(result)

    @pytest.mark.django_db
    def test_build_message_show_carriedforward_flags_no_cf_coverage(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_comparison,
        snapshot,
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison
        pull = comparison.pull
        notifier = CommentNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "reach, diff, flags, files, footer",
                "show_carryforward_flags": True,
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        repository = comparison.head.commit.repository
        result = notifier.build_message(comparison)

        assert snapshot("txt") == "\n".join(result)

    @pytest.mark.django_db
    def test_build_message_with_without_flags(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_comparison_coverage_carriedforward,
        snapshot,
    ):
        # Without flags table
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison_coverage_carriedforward
        pull = comparison.pull
        notifier = CommentNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "reach, diff, files, footer",
                "show_carryforward_flags": True,
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        repository = comparison.head.commit.repository
        result = notifier.build_message(comparison)

        assert snapshot("txt") == "\n".join(result)

        # With flags table
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison_coverage_carriedforward
        pull = comparison.pull
        notifier = CommentNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "reach, diff, flags, files, footer",
                "show_carryforward_flags": True,
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        repository = comparison.head.commit.repository
        result = notifier.build_message(comparison)
        assert snapshot("txt") == "\n".join(result)

    @pytest.mark.django_db
    def test_build_message_show_carriedforward_flags_has_cf_coverage(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_comparison_coverage_carriedforward,
        snapshot,
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison_coverage_carriedforward
        pull = comparison.pull
        notifier = CommentNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "reach, diff, flags, files, footer",
                "show_carryforward_flags": True,
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        repository = comparison.head.commit.repository
        result = notifier.build_message(comparison)
        assert snapshot("txt") == "\n".join(result)

    @pytest.mark.django_db
    def test_build_message_hide_carriedforward_flags_has_cf_coverage(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_comparison_coverage_carriedforward,
        snapshot,
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison_coverage_carriedforward
        pull = comparison.pull
        notifier = CommentNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "reach, diff, flags, files, footer",
                "show_carryforward_flags": False,
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        repository = comparison.head.commit.repository
        result = notifier.build_message(comparison)
        assert snapshot("txt") == "\n".join(result)

    @pytest.mark.django_db
    def test_build_message_default_layout(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_report_without_flags,
        sample_comparison,
        snapshot,
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison
        pull = comparison.pull
        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={},
            notifier_site_settings=True,
            current_yaml={"codecov": {"ui": {"hide_complexity": True}}},
            repository_service=mock_repo_provider,
        )
        repository = sample_comparison.head.commit.repository
        result = notifier.build_message(comparison)
        assert snapshot("txt") == "\n".join(result)

    @pytest.mark.django_db
    def test_send_actual_notification_spammy(
        self, dbsession, mock_configuration, mock_repo_provider, sample_comparison
    ):
        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "reach, diff, flags, files, footer",
                "behavior": "spammy",
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        data = {"message": ["message"], "commentid": "12345", "pullid": 98}
        mock_repo_provider.post_comment.return_value = {"id": 9865}
        result = notifier.send_actual_notification(data)
        assert result.notification_attempted
        assert result.notification_successful
        assert result.explanation is None
        assert result.data_sent == data
        assert result.data_received == {"id": 9865}
        mock_repo_provider.post_comment.assert_called_with(98, "message")
        assert not mock_repo_provider.edit_comment.called
        assert not mock_repo_provider.delete_comment.called

    @pytest.mark.django_db
    def test_build_message_no_flags(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_report_without_flags,
        sample_comparison,
        snapshot,
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        pull = sample_comparison.pull
        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"layout": "reach, diff, flags, files, footer"},
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        sample_comparison.head.report = ReadOnlyReport.create_from_report(
            sample_report_without_flags
        )
        comparison = sample_comparison
        repository = sample_comparison.head.commit.repository
        result = notifier.build_message(sample_comparison)

        assert snapshot("txt") == "\n".join(result)

    @pytest.mark.django_db
    def test_send_actual_notification_new_no_permissions(
        self, dbsession, mock_configuration, mock_repo_provider, sample_comparison
    ):
        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "reach, diff, flags, files, footer",
                "behavior": "new",
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        data = {"message": ["message"], "commentid": "12345", "pullid": 98}
        mock_repo_provider.post_comment.return_value = {"id": 9865}
        mock_repo_provider.delete_comment.side_effect = TorngitClientError(
            "code", "response", "message"
        )
        result = notifier.send_actual_notification(data)
        assert result.notification_attempted
        assert not result.notification_successful
        assert result.explanation == "no_permissions"
        assert result.data_sent == data
        assert result.data_received is None
        mock_repo_provider.delete_comment.assert_called_with(98, "12345")
        assert not mock_repo_provider.post_comment.called
        assert not mock_repo_provider.edit_comment.called

    @pytest.mark.django_db
    def test_send_actual_notification_new(
        self, dbsession, mock_configuration, mock_repo_provider, sample_comparison
    ):
        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "reach, diff, flags, files, footer",
                "behavior": "new",
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        data = {"message": ["message"], "commentid": "12345", "pullid": 98}
        mock_repo_provider.post_comment.return_value = {"id": 9865}
        mock_repo_provider.delete_comment.return_value = True
        result = notifier.send_actual_notification(data)
        assert result.notification_attempted
        assert result.notification_successful
        assert result.explanation is None
        assert result.data_sent == data
        assert result.data_received == {"id": 9865}
        mock_repo_provider.post_comment.assert_called_with(98, "message")
        assert not mock_repo_provider.edit_comment.called
        mock_repo_provider.delete_comment.assert_called_with(98, "12345")

    @pytest.mark.django_db
    def test_send_actual_notification_new_no_permissions_post(
        self, dbsession, mock_configuration, mock_repo_provider, sample_comparison
    ):
        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "reach, diff, flags, files, footer",
                "behavior": "new",
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        data = {"message": ["message"], "commentid": None, "pullid": 98}
        mock_repo_provider.post_comment.side_effect = TorngitClientError(
            "code", "response", "message"
        )
        mock_repo_provider.edit_comment.side_effect = TorngitClientError(
            "code", "response", "message"
        )
        result = notifier.send_actual_notification(data)
        assert result.notification_attempted
        assert not result.notification_successful
        assert result.explanation == "comment_posting_permissions"
        assert result.data_sent == data
        assert result.data_received is None
        mock_repo_provider.post_comment.assert_called_with(98, "message")
        assert not mock_repo_provider.delete_comment.called
        assert not mock_repo_provider.edit_comment.called

    @pytest.mark.django_db
    def test_send_actual_notification_new_deleted_comment(
        self, dbsession, mock_configuration, mock_repo_provider, sample_comparison
    ):
        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "reach, diff, flags, files, footer",
                "behavior": "new",
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        data = {"message": ["message"], "commentid": "12345", "pullid": 98}
        mock_repo_provider.post_comment.return_value = {"id": 9865}
        mock_repo_provider.delete_comment.side_effect = TorngitObjectNotFoundError(
            "response", "message"
        )
        result = notifier.send_actual_notification(data)
        assert result.notification_attempted
        assert result.notification_successful
        assert result.explanation is None
        assert result.data_sent == data
        assert result.data_received == {"id": 9865}
        mock_repo_provider.post_comment.assert_called_with(98, "message")
        assert not mock_repo_provider.edit_comment.called
        mock_repo_provider.delete_comment.assert_called_with(98, "12345")

    @pytest.mark.django_db
    def test_send_actual_notification_once_deleted_comment(
        self, dbsession, mock_configuration, mock_repo_provider, sample_comparison
    ):
        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "reach, diff, flags, files, footer",
                "behavior": "once",
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        data = {"message": ["message"], "commentid": "12345", "pullid": 98}
        mock_repo_provider.post_comment.return_value = {"id": 9865}
        mock_repo_provider.edit_comment.side_effect = TorngitObjectNotFoundError(
            "response", "message"
        )
        result = notifier.send_actual_notification(data)
        assert result.notification_attempted is False
        assert result.notification_successful is None
        assert result.explanation == "comment_deleted"
        assert result.data_sent == data
        assert result.data_received is None
        assert not mock_repo_provider.post_comment.called
        mock_repo_provider.edit_comment.assert_called_with(98, "12345", "message")
        assert not mock_repo_provider.delete_comment.called

    @pytest.mark.django_db
    def test_send_actual_notification_once_non_existing_comment(
        self, dbsession, mock_configuration, mock_repo_provider, sample_comparison
    ):
        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "reach, diff, flags, files, footer",
                "behavior": "once",
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        data = {"message": ["message"], "commentid": None, "pullid": 98}
        mock_repo_provider.post_comment.return_value = {"id": 9865}
        mock_repo_provider.edit_comment.side_effect = TorngitObjectNotFoundError(
            "response", "message"
        )
        result = notifier.send_actual_notification(data)
        assert result.notification_attempted
        assert result.notification_successful
        assert result.explanation is None
        assert result.data_sent == data
        assert result.data_received == {"id": 9865}
        mock_repo_provider.post_comment.assert_called_with(98, "message")
        assert not mock_repo_provider.delete_comment.called
        assert not mock_repo_provider.edit_comment.called

    @pytest.mark.django_db
    def test_send_actual_notification_once(
        self, dbsession, mock_configuration, mock_repo_provider, sample_comparison
    ):
        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "reach, diff, flags, files, footer",
                "behavior": "once",
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        data = {"message": ["message"], "commentid": "12345", "pullid": 98}
        mock_repo_provider.post_comment.return_value = {"id": 9865}
        mock_repo_provider.edit_comment.return_value = {"id": "49"}
        result = notifier.send_actual_notification(data)
        assert result.notification_attempted
        assert result.notification_successful
        assert result.explanation is None
        assert result.data_sent == data
        assert result.data_received == {"id": "49"}
        assert not mock_repo_provider.post_comment.called
        mock_repo_provider.edit_comment.assert_called_with(98, "12345", "message")
        assert not mock_repo_provider.delete_comment.called

    @pytest.mark.django_db
    def test_send_actual_notification_once_no_permissions(
        self, dbsession, mock_configuration, mock_repo_provider, sample_comparison
    ):
        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "reach, diff, flags, files, footer",
                "behavior": "once",
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        data = {"message": ["message"], "commentid": "12345", "pullid": 98}
        mock_repo_provider.post_comment.return_value = {"id": 9865}
        mock_repo_provider.edit_comment.side_effect = TorngitClientError(
            "code", "response", "message"
        )
        result = notifier.send_actual_notification(data)
        assert result.notification_attempted
        assert not result.notification_successful
        assert result.explanation == "no_permissions"
        assert result.data_sent == data
        assert result.data_received is None
        assert not mock_repo_provider.post_comment.called
        mock_repo_provider.edit_comment.assert_called_with(98, "12345", "message")
        assert not mock_repo_provider.delete_comment.called

    @pytest.mark.django_db
    def test_send_actual_notification_default(
        self, dbsession, mock_configuration, mock_repo_provider, sample_comparison
    ):
        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "reach, diff, flags, files, footer",
                "behavior": "default",
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        data = {"message": ["message"], "commentid": "12345", "pullid": 98}
        mock_repo_provider.post_comment.return_value = {"id": 9865}
        mock_repo_provider.edit_comment.return_value = {"id": "49"}
        result = notifier.send_actual_notification(data)
        assert result.notification_attempted
        assert result.notification_successful
        assert result.explanation is None
        assert result.data_sent == data
        assert result.data_received == {"id": "49"}
        assert not mock_repo_provider.post_comment.called
        mock_repo_provider.edit_comment.assert_called_with(98, "12345", "message")
        assert not mock_repo_provider.delete_comment.called

    @pytest.mark.django_db
    def test_send_actual_notification_default_no_permissions_edit(
        self, dbsession, mock_configuration, mock_repo_provider, sample_comparison
    ):
        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "reach, diff, flags, files, footer",
                "behavior": "default",
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        data = {"message": ["message"], "commentid": "12345", "pullid": 98}
        mock_repo_provider.post_comment.return_value = {"id": 9865}
        mock_repo_provider.edit_comment.side_effect = TorngitClientError(
            "code", "response", "message"
        )
        result = notifier.send_actual_notification(data)
        assert result.notification_attempted
        assert result.notification_successful
        assert result.explanation is None
        assert result.data_sent == data
        assert result.data_received == {"id": 9865}
        mock_repo_provider.post_comment.assert_called_with(98, "message")
        mock_repo_provider.edit_comment.assert_called_with(98, "12345", "message")
        assert not mock_repo_provider.delete_comment.called

    @pytest.mark.django_db
    def test_send_actual_notification_default_no_permissions_twice(
        self, dbsession, mock_configuration, mock_repo_provider, sample_comparison
    ):
        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "reach, diff, flags, files, footer",
                "behavior": "default",
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        data = {"message": ["message"], "commentid": "12345", "pullid": 98}
        mock_repo_provider.post_comment.side_effect = TorngitClientError(
            "code", "response", "message"
        )
        mock_repo_provider.edit_comment.side_effect = TorngitClientError(
            "code", "response", "message"
        )
        result = notifier.send_actual_notification(data)
        assert result.notification_attempted
        assert not result.notification_successful
        assert result.explanation == "comment_posting_permissions"
        assert result.data_sent == data
        assert result.data_received is None
        mock_repo_provider.post_comment.assert_called_with(98, "message")
        mock_repo_provider.edit_comment.assert_called_with(98, "12345", "message")
        assert not mock_repo_provider.delete_comment.called

    @pytest.mark.django_db
    def test_send_actual_notification_default_comment_not_found(
        self, dbsession, mock_configuration, mock_repo_provider, sample_comparison
    ):
        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "reach, diff, flags, files, footer",
                "behavior": "default",
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        data = {"message": ["message"], "commentid": "12345", "pullid": 98}
        mock_repo_provider.post_comment.return_value = {"id": 9865}
        mock_repo_provider.edit_comment.side_effect = TorngitObjectNotFoundError(
            "response", "message"
        )
        result = notifier.send_actual_notification(data)
        assert result.notification_attempted
        assert result.notification_successful
        assert result.explanation is None
        assert result.data_sent == data
        assert result.data_received == {"id": 9865}
        mock_repo_provider.edit_comment.assert_called_with(98, "12345", "message")
        mock_repo_provider.post_comment.assert_called_with(98, "message")
        assert not mock_repo_provider.delete_comment.called

    @pytest.mark.django_db
    def test_notify_no_pull_request(self, dbsession, sample_comparison_without_pull):
        notifier = CommentNotifier(
            repository=sample_comparison_without_pull.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "reach, diff, flags, files, footer",
                "behavior": "default",
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=None,
        )
        result = notifier.notify(sample_comparison_without_pull)
        assert not result.notification_attempted
        assert result.notification_successful == False
        assert result.explanation == "no_pull_request"
        assert result.data_sent is None
        assert result.data_received is None

    @pytest.mark.django_db
    def test_notify_pull_head_doesnt_match(self, dbsession, sample_comparison):
        sample_comparison.pull.head = "aaaaaaaaaa"
        dbsession.flush()
        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "reach, diff, flags, files, footer",
                "behavior": "default",
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=None,
        )
        result = notifier.notify(sample_comparison)
        assert not result.notification_attempted
        assert result.notification_successful is False
        assert result.explanation == "pull_head_does_not_match"
        assert result.data_sent is None
        assert result.data_received is None

    @pytest.mark.django_db
    def test_notify_pull_request_not_in_provider(
        self, dbsession, sample_comparison_database_pull_without_provider
    ):
        notifier = CommentNotifier(
            repository=sample_comparison_database_pull_without_provider.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "reach, diff, flags, files, footer",
                "behavior": "default",
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=None,
        )
        result = notifier.notify(sample_comparison_database_pull_without_provider)
        assert not result.notification_attempted
        assert result.notification_successful is False
        assert result.explanation == "pull_request_not_in_provider"
        assert result.data_sent is None
        assert result.data_received is None

    @pytest.mark.django_db
    def test_notify_server_unreachable(self, mocker, dbsession, sample_comparison):
        mocked_send_actual_notification = mocker.patch.object(
            CommentNotifier,
            "send_actual_notification",
            side_effect=TorngitServerUnreachableError(),
        )
        mocked_build_message = mocker.patch.object(
            CommentNotifier, "build_message", return_value=["title", "content"]
        )
        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "reach, diff, flags, files, footer",
                "behavior": "default",
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=None,
        )
        result = notifier.notify(sample_comparison)
        assert result.notification_attempted
        assert not result.notification_successful
        assert result.explanation == "provider_issue"
        assert result.data_sent == {
            "commentid": None,
            "message": ["title", "content"],
            "pullid": sample_comparison.pull.pullid,
        }
        assert result.data_received is None

    @pytest.mark.django_db
    def test_store_results(self, dbsession, sample_comparison):
        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "reach, diff, flags, files, footer",
                "behavior": "default",
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=None,
        )
        result = NotificationResult(
            notification_attempted=True,
            notification_successful=True,
            explanation=None,
            data_sent=None,
            data_received={"id": 578263422},
        )
        notifier.store_results(sample_comparison, result)
        assert sample_comparison.pull.commentid == 578263422
        dbsession.flush()
        assert sample_comparison.pull.commentid == 578263422
        dbsession.refresh(sample_comparison.pull)
        assert sample_comparison.pull.commentid == "578263422"

    @pytest.mark.django_db
    def test_store_results_deleted_comment(self, dbsession, sample_comparison):
        sample_comparison.pull.commentid = 12
        dbsession.flush()
        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "reach, diff, flags, files, footer",
                "behavior": "default",
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=None,
        )
        result = NotificationResult(
            notification_attempted=True,
            notification_successful=True,
            explanation=None,
            data_sent=None,
            data_received={"deleted_comment": True},
        )
        notifier.store_results(sample_comparison, result)
        assert sample_comparison.pull.commentid is None
        dbsession.flush()
        assert sample_comparison.pull.commentid is None
        dbsession.refresh(sample_comparison.pull)
        assert sample_comparison.pull.commentid is None

    @pytest.mark.django_db
    def test_store_results_no_succesfull_result(self, dbsession, sample_comparison):
        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "reach, diff, flags, files, footer",
                "behavior": "default",
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=None,
        )
        result = NotificationResult(
            notification_attempted=True,
            notification_successful=False,
            explanation=None,
            data_sent=None,
            data_received={"id": "yadayada"},
        )
        notifier.store_results(sample_comparison, result)
        assert sample_comparison.pull.commentid is None
        dbsession.flush()
        assert sample_comparison.pull.commentid is None
        dbsession.refresh(sample_comparison.pull)
        assert sample_comparison.pull.commentid is None

    @pytest.mark.django_db
    def test_notify_unable_to_fetch_info(self, dbsession, mocker, sample_comparison):
        mocked_build_message = mocker.patch.object(
            CommentNotifier,
            "build_message",
            side_effect=TorngitClientError("code", "response", "message"),
        )
        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "reach, diff, flags, files, footer",
                "behavior": "default",
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=None,
        )
        result = notifier.notify(sample_comparison)
        assert not result.notification_attempted
        assert result.notification_successful is False
        assert result.explanation == "unable_build_message"
        assert result.data_sent is None
        assert result.data_received is None

    @pytest.mark.django_db
    def test_notify_not_enough_builds(self, dbsession, sample_comparison):
        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "reach, diff, flags, files, footer",
                "behavior": "default",
                "after_n_builds": 5,
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=None,
        )
        result = notifier.notify(sample_comparison)
        assert not result.notification_attempted
        assert result.notification_successful is False
        assert result.explanation == "not_enough_builds"
        assert result.data_sent is None
        assert result.data_received is None

    @pytest.mark.django_db
    @pytest.mark.asyncio
    @pytest.mark.parametrize("pull_state", ["open", "closed"])
    async def test_notify_with_enough_builds(
        self, dbsession, sample_comparison, mocker, pull_state
    ):
        build_message_mocker = mocker.patch.object(
            CommentNotifier,
            "build_message",
            return_value="message_test_notify_with_enough_builds",
        )
        send_comment_default_behavior_mocker = mocker.patch.object(
            CommentNotifier,
            "send_comment_default_behavior",
            return_value={
                "notification_attempted": True,
                "notification_successful": True,
                "explanation": None,
                "data_received": None,
            },
        )
        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "reach, diff, flags, files, footer",
                "behavior": "default",
                "after_n_builds": 1,
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mocker.MagicMock(),
        )
        sample_comparison.pull.state = pull_state
        dbsession.flush()
        dbsession.refresh(sample_comparison.pull)
        result = notifier.notify(sample_comparison)
        assert result.notification_attempted
        assert result.notification_successful
        assert result.explanation is None
        assert result.data_sent == {
            "commentid": None,
            "message": "message_test_notify_with_enough_builds",
            "pullid": sample_comparison.pull.pullid,
        }
        assert result.data_received is None

    @pytest.mark.django_db
    def test_notify_exact_same_report_diff_unrelated_report(
        self, sample_comparison_no_change, mock_repo_provider
    ):
        compare_result = {
            "diff": {
                "files": {
                    "README.md": {
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
                                    " -",
                                ],
                            },
                        ],
                        "stats": {"added": 11, "removed": 4},
                    }
                }
            }
        }
        mock_repo_provider.get_compare.return_value = compare_result
        notifier = CommentNotifier(
            repository=sample_comparison_no_change.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "reach, diff, flags, files, footer",
                "behavior": "default",
                "after_n_builds": 1,
                "require_changes": [CoverageCommentRequiredChanges.any_change.value],
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        res = notifier.notify(sample_comparison_no_change)
        assert res.notification_attempted is False
        assert res.notification_successful is False
        assert res.explanation == "changes_required"
        assert res.data_sent is None
        assert res.data_received is None

    @pytest.mark.django_db
    def test_notify_exact_same_report_diff_unrelated_report_update_comment(
        self, sample_comparison_no_change, mock_repo_provider
    ):
        compare_result = {
            "diff": {
                "files": {
                    "README.md": {
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
                                    " -",
                                ],
                            },
                        ],
                        "stats": {"added": 11, "removed": 4},
                    }
                }
            }
        }
        mock_repo_provider.get_compare.return_value = compare_result
        sample_comparison_no_change.pull.commentid = 12
        mock_repo_provider.edit_comment.return_value = {"id": 12}
        notifier = CommentNotifier(
            repository=sample_comparison_no_change.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "reach, diff, flags, files, footer",
                "behavior": "default",
                "after_n_builds": 1,
                "require_changes": [CoverageCommentRequiredChanges.any_change.value],
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        res = notifier.notify(sample_comparison_no_change)
        assert res.notification_attempted is True
        assert res.notification_successful is True
        mock_repo_provider.edit_comment.assert_called()

    @pytest.mark.django_db
    def test_message_hide_details_github(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_comparison,
        snapshot,
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison
        comparison.repository_service.service = "github"
        pull = comparison.pull
        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"layout": "reach", "hide_comment_details": True},
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        repository = sample_comparison.head.commit.repository
        result = notifier.build_message(comparison)
        assert snapshot("txt") == "\n".join(result)

    @pytest.mark.django_db
    def test_message_announcements_only(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_comparison,
        snapshot,
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison
        comparison.repository_service.service = "github"
        pull = comparison.pull
        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"layout": "announcements"},
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        repository = sample_comparison.head.commit.repository
        result = notifier.build_message(comparison)
        assert snapshot("txt") == "\n".join(result)

    @pytest.mark.django_db
    def test_message_hide_details_bitbucket(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_comparison,
        snapshot,
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison
        comparison.repository_service.service = "bitbucket"
        pull = comparison.pull
        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"layout": "reach", "hide_comment_details": True},
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        repository = sample_comparison.head.commit.repository
        result = notifier.build_message(comparison)
        assert snapshot("txt") == "\n".join(result)


class TestFileSectionWriter:
    def test_filesection_no_extra_settings(self, sample_comparison, mocker, snapshot):
        section_writer = FileSectionWriter(
            sample_comparison.head.commit.repository,
            "layout",
            show_complexity=False,
            settings={},
            current_yaml={},
        )
        changes = [
            Change(
                path="file_2.py",
                in_diff=True,
                totals=ReportTotals(
                    files=0,
                    lines=0,
                    hits=-2,
                    misses=1,
                    partials=0,
                    coverage=-23.333330000000004,
                    branches=0,
                    methods=0,
                    messages=0,
                    sessions=0,
                    complexity=0,
                    complexity_total=0,
                    diff=0,
                ),
            ),
            Change(
                path="unrelated.py",
                in_diff=False,
                totals=ReportTotals(
                    files=0,
                    lines=0,
                    hits=-3,
                    misses=2,
                    partials=0,
                    coverage=-43.333330000000004,
                    branches=0,
                    methods=0,
                    messages=0,
                    sessions=0,
                    complexity=0,
                    complexity_total=0,
                    diff=0,
                ),
            ),
            Change(path="added.py", new=True, in_diff=None, old_path=None, totals=None),
        ]
        lines = list(
            section_writer.write_section(
                sample_comparison,
                {
                    "files": {
                        "file_1.go": {
                            "type": "added",
                            "totals": ReportTotals(
                                lines=3,
                                hits=2,
                                misses=1,
                                coverage=66.66,
                                branches=0,
                                methods=0,
                                messages=0,
                                sessions=0,
                                complexity=0,
                                complexity_total=0,
                                diff=0,
                            ),
                        }
                    }
                },
                changes,
                links={"pull": "pull.link"},
            )
        )
        assert snapshot("txt") == "\n".join(lines)

    @pytest.mark.parametrize(
        "test_analytics_enabled,bundle_analysis_enabled",
        [(False, False), (False, True), (True, False), (True, True)],
    )
    @pytest.mark.django_db
    def test_build_cross_pollination_message(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_comparison,
        test_analytics_enabled,
        bundle_analysis_enabled,
        snapshot,
    ):
        mock_all_plans_and_tiers()
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison
        comparison.pull.is_first_coverage_pull = False
        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"layout": "reach, diff, flags, files, footer"},
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        repository = sample_comparison.head.commit.repository
        if bundle_analysis_enabled:
            repository.languages = ["javascript"]
        if test_analytics_enabled:
            repository.test_analytics_enabled = False
        dbsession.flush()
        result = notifier.build_message(comparison)

        assert snapshot("txt") == "\n".join(result)

    def test_get_tree_cell(self):
        typ = "added"
        path = "path/to/test_file.go"
        metrics = "| this is where the metrics go |"
        line = _get_tree_cell(
            typ=typ, path=path, metrics=metrics, compare="pull.link", is_critical=False
        )
        assert (
            line
            == f"| [path/to/test\\_file.go](pull.link?src=pr&el=tree&filepath=path%2Fto%2Ftest_file.go#diff-cGF0aC90by90ZXN0X2ZpbGUuZ28=) {metrics}"
        )

    def test_get_tree_cell_with_critical(self):
        typ = "added"
        path = "path/to/test_file.go"
        metrics = "| this is where the metrics go |"
        line = _get_tree_cell(
            typ=typ, path=path, metrics=metrics, compare="pull.link", is_critical=True
        )
        assert (
            line
            == f"| [path/to/test\\_file.go](pull.link?src=pr&el=tree&filepath=path%2Fto%2Ftest_file.go#diff-cGF0aC90by90ZXN0X2ZpbGUuZ28=) **Critical** {metrics}"
        )

    def test_filesection_hide_project_cov(self, sample_comparison, mocker, snapshot):
        section_writer = NewFilesSectionWriter(
            sample_comparison.head.commit.repository,
            "layout",
            show_complexity=False,
            settings={"hide_project_coverage": True},
            current_yaml={},
        )
        changes = [
            Change(
                path="unrelated.py",
                in_diff=False,
                totals=ReportTotals(
                    files=0,
                    lines=0,
                    hits=-3,
                    misses=2,
                    partials=0,
                    coverage=-43.333330000000004,
                    branches=0,
                    methods=0,
                    messages=0,
                    sessions=0,
                    complexity=0,
                    complexity_total=0,
                    diff=0,
                ),
            ),
            Change(path="added.py", new=True, in_diff=None, old_path=None, totals=None),
        ]
        lines = list(
            section_writer.write_section(
                sample_comparison,
                {
                    "files": {
                        "file_1.go": {
                            "type": "added",
                            "totals": ReportTotals(
                                lines=3,
                                hits=2,
                                misses=1,
                                coverage=66.66,
                                branches=0,
                                methods=0,
                                messages=0,
                                sessions=0,
                                complexity=0,
                                complexity_total=0,
                                diff=0,
                            ),
                        },
                        "file_2.py": {
                            "type": "added",
                            "totals": ReportTotals(
                                lines=3,
                                hits=-2,
                                misses=2,
                                partials=3,
                                coverage=-23.333330000000004,
                                branches=0,
                                methods=0,
                                messages=0,
                                sessions=0,
                                complexity=0,
                                complexity_total=0,
                                diff=0,
                            ),
                        },
                    }
                },
                changes,
                links={"pull": "pull.link"},
            )
        )

        assert snapshot("txt") == "\n".join(lines)

    def test_filesection_hide_project_cov_with_changed_files_but_no_missing_lines(
        self, sample_comparison, mocker
    ):
        section_writer = NewFilesSectionWriter(
            sample_comparison.head.commit.repository,
            "layout",
            show_complexity=False,
            settings={"hide_project_coverage": True},
            current_yaml={},
        )
        changes = [
            Change(
                path="unrelated.py",
                in_diff=False,
                totals=ReportTotals(
                    files=0,
                    lines=0,
                    hits=-3,
                    misses=2,
                    partials=0,
                    coverage=-43.333330000000004,
                    branches=0,
                    methods=0,
                    messages=0,
                    sessions=0,
                    complexity=0,
                    complexity_total=0,
                    diff=0,
                ),
            ),
            Change(path="added.py", new=True, in_diff=None, old_path=None, totals=None),
        ]
        lines = list(
            section_writer.write_section(
                sample_comparison,
                {
                    "files": {
                        "file_1.go": {
                            "type": "added",
                            "totals": ReportTotals(
                                lines=3,
                                hits=3,
                                misses=0,
                                coverage=100.00,
                                branches=0,
                                methods=0,
                                messages=0,
                                sessions=0,
                                complexity=0,
                                complexity_total=0,
                                diff=0,
                            ),
                        },
                        "file_2.py": {
                            "type": "added",
                            "totals": ReportTotals(
                                lines=3,
                                hits=3,
                                misses=0,
                                partials=0,
                                coverage=-100.00,
                                branches=0,
                                methods=0,
                                messages=0,
                                sessions=0,
                                complexity=0,
                                complexity_total=0,
                                diff=0,
                            ),
                        },
                    }
                },
                changes,
                links={"pull": "pull.link"},
            )
        )
        assert lines == []

    def test_filesection_hide_project_cov_no_files_changed(
        self, sample_comparison, mocker
    ):
        section_writer = NewFilesSectionWriter(
            sample_comparison.head.commit.repository,
            "layout",
            show_complexity=False,
            settings={"hide_project_coverage": True},
            current_yaml={},
        )
        changes = [
            Change(
                path="unrelated.py",
                in_diff=False,
                totals=ReportTotals(
                    files=0,
                    lines=0,
                    hits=-3,
                    misses=2,
                    partials=0,
                    coverage=-43.333330000000004,
                    branches=0,
                    methods=0,
                    messages=0,
                    sessions=0,
                    complexity=0,
                    complexity_total=0,
                    diff=0,
                ),
            ),
            Change(path="added.py", new=True, in_diff=None, old_path=None, totals=None),
        ]
        lines = list(
            section_writer.write_section(
                sample_comparison,
                {"files": {}},
                changes,
                links={"pull": "pull.link"},
            )
        )
        assert lines == []


class TestNewHeaderSectionWriter:
    def test_new_header_section_writer(self, mocker, sample_comparison, snapshot):
        writer = HeaderSectionWriter(
            mocker.MagicMock(),
            mocker.MagicMock(),
            show_complexity=mocker.MagicMock(),
            settings={},
            current_yaml=mocker.MagicMock(),
        )
        mocker.patch(
            "services.notification.notifiers.mixins.message.sections.round_number",
            return_value=Decimal(0),
        )
        res = list(
            writer.write_section(
                sample_comparison,
                None,
                None,
                links={"pull": "urlurl", "base": "urlurl", "head": "headurl"},
            )
        )
        assert snapshot("json") == res

    def test_new_header_section_writer_with_behind_by(
        self, mocker, sample_comparison, snapshot
    ):
        writer = HeaderSectionWriter(
            mocker.MagicMock(),
            mocker.MagicMock(),
            show_complexity=mocker.MagicMock(),
            settings={},
            current_yaml=mocker.MagicMock(),
        )
        mocker.patch(
            "services.notification.notifiers.mixins.message.sections.round_number",
            return_value=Decimal(0),
        )
        res = list(
            writer.write_section(
                sample_comparison,
                None,
                None,
                links={"pull": "urlurl", "base": "urlurl", "head": "headurl"},
                behind_by=3,
            )
        )
        assert snapshot("json") == res

    def test_new_header_section_writer_test_results_setup(
        self, mocker, sample_comparison, snapshot
    ):
        sample_comparison.context = ComparisonContext(all_tests_passed=True)
        writer = HeaderSectionWriter(
            mocker.MagicMock(),
            mocker.MagicMock(),
            show_complexity=mocker.MagicMock(),
            settings={},
            current_yaml=mocker.MagicMock(),
        )
        mocker.patch(
            "services.notification.notifiers.mixins.message.sections.round_number",
            return_value=Decimal(0),
        )
        res = list(
            writer.write_section(
                sample_comparison,
                None,
                None,
                links={"pull": "urlurl", "base": "urlurl", "head": "headurl"},
            )
        )
        assert snapshot("json") == res

    def test_new_header_section_writer_test_results_error(
        self, mocker, sample_comparison, snapshot
    ):
        sample_comparison.context = ComparisonContext(
            all_tests_passed=False,
            test_results_error=":warning: We are unable to process any of the uploaded JUnit XML files. Please ensure your files are in the right format.",
        )
        writer = HeaderSectionWriter(
            mocker.MagicMock(),
            mocker.MagicMock(),
            show_complexity=mocker.MagicMock(),
            settings={},
            current_yaml=mocker.MagicMock(),
        )
        mocker.patch(
            "services.notification.notifiers.mixins.message.sections.round_number",
            return_value=Decimal(0),
        )
        res = list(
            writer.write_section(
                sample_comparison,
                None,
                None,
                links={"pull": "urlurl", "base": "urlurl", "head": "headurl"},
            )
        )
        assert snapshot("json") == res

    def test_new_header_section_writer_no_project_coverage(
        self, mocker, sample_comparison, snapshot
    ):
        writer = HeaderSectionWriter(
            mocker.MagicMock(),
            mocker.MagicMock(),
            show_complexity=mocker.MagicMock(),
            settings={"hide_project_coverage": True},
            current_yaml=mocker.MagicMock(),
        )
        mocker.patch(
            "services.notification.notifiers.mixins.message.sections.round_number",
            return_value=Decimal(0),
        )
        res = list(
            writer.write_section(
                sample_comparison,
                None,
                None,
                links={"pull": "urlurl", "base": "urlurl", "head": "headurl"},
            )
        )
        assert snapshot("json") == res

    def test_new_header_section_writer_no_project_coverage_test_results_setup(
        self, mocker, sample_comparison, snapshot
    ):
        sample_comparison.context = ComparisonContext(all_tests_passed=True)
        writer = HeaderSectionWriter(
            mocker.MagicMock(),
            mocker.MagicMock(),
            show_complexity=mocker.MagicMock(),
            settings={"hide_project_coverage": True},
            current_yaml=mocker.MagicMock(),
        )
        mocker.patch(
            "services.notification.notifiers.mixins.message.sections.round_number",
            return_value=Decimal(0),
        )
        res = list(
            writer.write_section(
                sample_comparison,
                None,
                None,
                links={"pull": "urlurl", "base": "urlurl", "head": "headurl"},
            )
        )
        assert snapshot("json") == res

    def test_new_header_section_writer_no_project_coverage_test_results_error(
        self, mocker, sample_comparison, snapshot
    ):
        sample_comparison.context = ComparisonContext(
            all_tests_passed=False,
            test_results_error=":warning: We are unable to process any of the uploaded JUnit XML files. Please ensure your files are in the right format.",
        )
        writer = HeaderSectionWriter(
            mocker.MagicMock(),
            mocker.MagicMock(),
            show_complexity=mocker.MagicMock(),
            settings={"hide_project_coverage": True},
            current_yaml=mocker.MagicMock(),
        )
        mocker.patch(
            "services.notification.notifiers.mixins.message.sections.round_number",
            return_value=Decimal(0),
        )
        res = list(
            writer.write_section(
                sample_comparison,
                None,
                None,
                links={"pull": "urlurl", "base": "urlurl", "head": "headurl"},
            )
        )
        assert snapshot("json") == res


class TestAnnouncementsSectionWriter:
    def test_announcement_section_writer(self, mocker):
        writer = AnnouncementSectionWriter(
            mocker.MagicMock(),
            mocker.MagicMock(),
            mocker.MagicMock(),
            mocker.MagicMock(),
            mocker.MagicMock(),
        )
        res = list(writer.write_section(mocker.MagicMock()))
        assert len(res) == 1
        line = res[0]
        assert line.startswith(":mega: ")
        message = line[7:]
        assert message in AnnouncementSectionWriter.current_active_messages


class TestNewFooterSectionWriter:
    def test_footer_section_writer_in_github(self, mocker):
        writer = NewFooterSectionWriter(
            mocker.MagicMock(),
            mocker.MagicMock(),
            mocker.MagicMock(),
            settings={},
            current_yaml=mocker.MagicMock(),
        )
        mock_comparison = mocker.MagicMock()
        mock_comparison.repository_service.service = "github"
        res = list(
            writer.write_section(mock_comparison, {}, [], links={"pull": "pull.link"})
        )
        assert res == [
            "",
            "[:umbrella: View full report in Codecov by Sentry](pull.link?dropdown=coverage&src=pr&el=continue).   ",
            ":loudspeaker: Have feedback on the report? [Share it here](https://about.codecov.io/codecov-pr-comment-feedback/).",
        ]

    def test_footer_section_writer_in_gitlab(self, mocker):
        writer = NewFooterSectionWriter(
            mocker.MagicMock(),
            mocker.MagicMock(),
            mocker.MagicMock(),
            settings={},
            current_yaml=mocker.MagicMock(),
        )
        mock_comparison = mocker.MagicMock()
        mock_comparison.repository_service.service = "gitlab"
        res = list(
            writer.write_section(mock_comparison, {}, [], links={"pull": "pull.link"})
        )
        assert res == [
            "",
            "[:umbrella: View full report in Codecov by Sentry](pull.link?dropdown=coverage&src=pr&el=continue).   ",
            ":loudspeaker: Have feedback on the report? [Share it here](https://gitlab.com/codecov-open-source/codecov-user-feedback/-/issues/4).",
        ]

    def test_footer_section_writer_in_bitbucket(self, mocker):
        writer = NewFooterSectionWriter(
            mocker.MagicMock(),
            mocker.MagicMock(),
            mocker.MagicMock(),
            settings={},
            current_yaml=mocker.MagicMock(),
        )
        mock_comparison = mocker.MagicMock()
        mock_comparison.repository_service.service = "bitbucket"
        res = list(
            writer.write_section(mock_comparison, {}, [], links={"pull": "pull.link"})
        )
        assert res == [
            "",
            "[:umbrella: View full report in Codecov by Sentry](pull.link?dropdown=coverage&src=pr&el=continue).   ",
            ":loudspeaker: Have feedback on the report? [Share it here](https://gitlab.com/codecov-open-source/codecov-user-feedback/-/issues/4).",
        ]

    def test_footer_section_writer_with_project_cov_hidden(self, mocker):
        writer = NewFooterSectionWriter(
            mocker.MagicMock(),
            mocker.MagicMock(),
            mocker.MagicMock(),
            settings={
                "layout": "newheader, files, newfooter",
                "hide_project_coverage": True,
            },
            current_yaml={},
        )
        mock_comparison = mocker.MagicMock()
        mock_comparison.repository_service.service = "bitbucket"
        res = list(
            writer.write_section(mock_comparison, {}, [], links={"pull": "pull.link"})
        )
        assert res == [
            "",
            ":loudspeaker: Thoughts on this report? [Let us know!](https://about.codecov.io/pull-request-comment-report/)",
        ]


@pytest.mark.usefixtures("is_not_first_pull")
class TestCommentNotifierInNewLayout:
    @pytest.fixture(autouse=True)
    def mock_all_plans_and_tiers(self):
        mock_all_plans_and_tiers()

    @pytest.mark.django_db
    def test_build_message_no_base_commit_new_layout(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_comparison_without_base_with_pull,
        snapshot,
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison_without_base_with_pull
        comparison.repository_service.service = "github"
        pull = comparison.pull
        notifier = CommentNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "newheader, reach, diff, flags, files, newfooter"
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        repository = comparison.head.commit.repository
        result = notifier.build_message(comparison)
        assert snapshot("txt") == "\n".join(result)

    @pytest.mark.django_db
    def test_build_message_no_base_report_new_layout(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_comparison_without_base_report,
        snapshot,
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison_without_base_report
        comparison.repository_service.service = "github"
        pull = comparison.pull
        notifier = CommentNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "newheader, reach, diff, flags, files, newfooter",
                "hide_comment_details": True,
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        repository = comparison.head.commit.repository
        result = notifier.build_message(comparison)
        assert snapshot("txt") == "\n".join(result)

    @pytest.mark.django_db
    def test_build_message_no_project_coverage(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_comparison,
        snapshot,
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison
        comparison.repository_service.service = "github"
        pull = comparison.pull
        notifier = CommentNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "newheader, newfiles, newfooter",
                "hide_project_coverage": True,
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        repository = comparison.head.commit.repository
        result = notifier.build_message(comparison)
        pull_url = f"test.example.br/gh/{repository.slug}/pull/{pull.pullid}"
        assert snapshot("txt") == "\n".join(result)

    @pytest.mark.django_db
    def test_build_message_no_project_coverage_files(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_comparison,
        snapshot,
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison
        comparison.repository_service.service = "github"
        pull = comparison.pull
        notifier = CommentNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "newheader, files, newfooter",
                "hide_project_coverage": True,
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        repository = comparison.head.commit.repository
        result = notifier.build_message(comparison)
        pull_url = f"test.example.br/gh/{repository.slug}/pull/{pull.pullid}"
        assert snapshot("txt") == "\n".join(result)

    @pytest.mark.django_db
    def test_build_message_no_project_coverage_condensed_yaml_configs(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_comparison,
        snapshot,
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison
        comparison.repository_service.service = "github"
        pull = comparison.pull
        notifier = CommentNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "condensed_header, condensed_files, condensed_footer",
                "hide_project_coverage": True,
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        repository = comparison.head.commit.repository
        result = notifier.build_message(comparison)
        pull_url = f"test.example.br/gh/{repository.slug}/pull/{pull.pullid}"
        assert snapshot("txt") == "\n".join(result)

    @pytest.mark.django_db
    def test_build_message_head_and_pull_head_differ_new_layout(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_comparison_head_and_pull_head_differ,
        snapshot,
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison_head_and_pull_head_differ
        comparison.repository_service.service = "github"
        pull = comparison.pull
        notifier = CommentNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "newheader, reach, diff, flags, newfooter",
                "hide_comment_details": True,
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        repository = comparison.head.commit.repository
        result = notifier.build_message(comparison)

        assert snapshot("txt") == "\n".join(result)

    @pytest.mark.django_db
    def test_build_message_head_and_pull_head_differ_with_components(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_comparison_head_and_pull_head_differ,
        snapshot,
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison_head_and_pull_head_differ
        comparison.repository_service.service = "github"
        pull = comparison.pull
        notifier = CommentNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "newheader, reach, diff, flags, components, newfooter",
                "hide_comment_details": True,
            },
            notifier_site_settings=True,
            current_yaml={
                "component_management": {
                    "individual_components": [
                        {"component_id": "go_files", "paths": [r".*\.go"]},
                        {"component_id": "unit_flags", "flag_regexes": [r"unit.*"]},
                    ]
                }
            },
            repository_service=mock_repo_provider,
        )
        repository = comparison.head.commit.repository
        result = notifier.build_message(comparison)

        assert snapshot("txt") == "\n".join(result)

    @pytest.mark.django_db
    def test_build_message_team_plan_customer_missing_lines(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_comparison_head_and_pull_head_differ,
        snapshot,
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison_head_and_pull_head_differ
        comparison.repository_service.service = "github"
        # relevant part of this test
        comparison.head.commit.repository.owner.plan = "users-teamm"
        notifier = CommentNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                # Irrelevant but they don't overwrite Owner's plan
                "layout": "newheader, reach, diff, flags, components, newfooter",
                "hide_project_coverage": True,
            },
            notifier_site_settings=True,
            current_yaml={
                # Irrelevant but here to ensure they don't overwrite Owner's plan
                "component_management": {
                    "individual_components": [
                        {"component_id": "go_files", "paths": [r".*\.go"]},
                        {"component_id": "unit_flags", "flag_regexes": [r"unit.*"]},
                    ]
                }
            },
            repository_service=mock_repo_provider,
        )

        pull = comparison.pull
        repository = sample_comparison_head_and_pull_head_differ.head.commit.repository
        result = notifier.build_message(comparison)

        assert snapshot("txt") == "\n".join(result)

    @pytest.mark.django_db
    def test_build_message_team_plan_customer_all_lines_covered(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_comparison_coverage_carriedforward,
        snapshot,
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        sample_comparison_coverage_carriedforward.context = ComparisonContext(
            all_tests_passed=True
        )
        comparison = sample_comparison_coverage_carriedforward
        comparison.repository_service.service = "github"
        # relevant part of this test
        comparison.head.commit.repository.owner.plan = "users-teamm"
        notifier = CommentNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                # Irrelevant but they don't overwrite Owner's plan
                "layout": "newheader, reach, diff, flags, components, newfooter",
                "hide_project_coverage": True,
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        pull = comparison.pull
        repository = sample_comparison_coverage_carriedforward.head.commit.repository
        result = notifier.build_message(comparison)

        assert snapshot("txt") == "\n".join(result)

    @pytest.mark.django_db
    def test_build_message_team_plan_customer_all_lines_covered_test_results_error(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_comparison_coverage_carriedforward,
        snapshot,
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        sample_comparison_coverage_carriedforward.context = ComparisonContext(
            all_tests_passed=False,
            test_results_error=":warning: We are unable to process any of the uploaded JUnit XML files. Please ensure your files are in the right format.",
        )
        comparison = sample_comparison_coverage_carriedforward
        comparison.repository_service.service = "github"
        # relevant part of this test
        comparison.head.commit.repository.owner.plan = "users-teamm"
        notifier = CommentNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                # Irrelevant but they don't overwrite Owner's plan
                "layout": "newheader, reach, diff, flags, components, newfooter",
                "hide_project_coverage": True,
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        pull = comparison.pull
        repository = sample_comparison_coverage_carriedforward.head.commit.repository
        result = notifier.build_message(comparison)

        assert snapshot("txt") == "\n".join(result)

    @pytest.mark.django_db
    def test_build_message_team_plan_customer_all_lines_covered_no_third_line(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_comparison_coverage_carriedforward,
        snapshot,
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        sample_comparison_coverage_carriedforward.context = ComparisonContext(
            all_tests_passed=False,
            test_results_error=None,
        )
        comparison = sample_comparison_coverage_carriedforward
        comparison.repository_service.service = "github"
        # relevant part of this test
        comparison.head.commit.repository.owner.plan = "users-teamm"
        notifier = CommentNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                # Irrelevant but they don't overwrite Owner's plan
                "layout": "newheader, reach, diff, flags, components, newfooter",
                "hide_project_coverage": True,
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        pull = comparison.pull
        repository = sample_comparison_coverage_carriedforward.head.commit.repository
        result = notifier.build_message(comparison)

        assert snapshot("txt") == "\n".join(result)

    @pytest.mark.django_db
    def test_build_message_no_patch_or_proj_change(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_comparison_coverage_carriedforward,
        snapshot,
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"
        comparison = sample_comparison_coverage_carriedforward
        pull = comparison.pull
        notifier = CommentNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "newheader, reach, diff, flags, files, footer",
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        repository = comparison.head.commit.repository
        result = notifier.build_message(comparison)
        assert snapshot("txt") == "\n".join(result)


class TestComponentWriterSection:
    def test_write_message_component_section_empty(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_comparison,
    ):
        comparison = sample_comparison
        section_writer = ComponentsSectionWriter(
            repository=comparison.head.commit.repository,
            layout="layout",
            show_complexity=False,
            settings={},
            current_yaml={},
        )
        message = section_writer.write_section(
            comparison=comparison, diff=None, changes=None, links={"pull": "urlurl"}
        )
        expected = []
        assert message == expected

    def test_get_component_data_for_table(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_comparison,
    ):
        comparison = sample_comparison
        section_writer = ComponentsSectionWriter(
            repository=comparison.head.commit.repository,
            layout="layout",
            show_complexity=False,
            settings={},
            current_yaml={
                "component_management": {
                    "individual_components": [
                        {"component_id": "go_files", "paths": [r".*\.go"]},
                        {"component_id": "py_files", "paths": [r".*\.py"]},
                    ]
                }
            },
        )
        all_components = get_components_from_yaml(section_writer.current_yaml)
        comparison = sample_comparison
        component_data = section_writer._get_table_data_for_components(
            all_components, comparison
        )
        expected_result = [
            {
                "name": "go_files",
                "before": ReportTotals(
                    files=1,
                    lines=4,
                    hits=2,
                    misses=2,
                    partials=0,
                    coverage="50.00000",
                    branches=0,
                    methods=0,
                    messages=0,
                    sessions=1,
                    complexity=11,
                    complexity_total=20,
                    diff=0,
                ),
                "after": ReportTotals(
                    files=1,
                    lines=8,
                    hits=5,
                    misses=3,
                    partials=0,
                    coverage="62.50000",
                    branches=0,
                    methods=0,
                    messages=0,
                    sessions=1,
                    complexity=10,
                    complexity_total=2,
                    diff=0,
                ),
                "diff": ReportTotals(
                    files=1,
                    lines=3,
                    hits=2,
                    misses=1,
                    partials=0,
                    coverage="66.66667",
                    branches=0,
                    methods=0,
                    messages=0,
                    sessions=0,
                    complexity=0,
                    complexity_total=0,
                    diff=0,
                ),
            },
            {
                "name": "py_files",
                "before": ReportTotals(
                    files=1,
                    lines=2,
                    hits=1,
                    misses=1,
                    partials=0,
                    coverage="50.00000",
                    branches=0,
                    methods=0,
                    messages=0,
                    sessions=1,
                    complexity=0,
                    complexity_total=0,
                    diff=0,
                ),
                "after": ReportTotals(
                    files=1,
                    lines=2,
                    hits=1,
                    misses=0,
                    partials=1,
                    coverage="50.00000",
                    branches=1,
                    methods=0,
                    messages=0,
                    sessions=1,
                    complexity=0,
                    complexity_total=0,
                    diff=0,
                ),
                "diff": ReportTotals(
                    files=0,
                    lines=0,
                    hits=0,
                    misses=0,
                    partials=0,
                    coverage=None,
                    branches=0,
                    methods=0,
                    messages=0,
                    sessions=0,
                    complexity=None,
                    complexity_total=None,
                    diff=0,
                ),
            },
        ]
        assert component_data == expected_result

    def test_get_component_data_for_table_no_base(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_comparison,
    ):
        comparison = sample_comparison
        comparison.project_coverage_base.report = None
        comparison.project_coverage_base.commit = None
        section_writer = ComponentsSectionWriter(
            repository=comparison.head.commit.repository,
            layout="layout",
            show_complexity=False,
            settings={},
            current_yaml={
                "component_management": {
                    "individual_components": [
                        {"component_id": "go_files", "paths": [r".*\.go"]},
                        {"component_id": "py_files", "paths": [r".*\.py"]},
                    ]
                }
            },
        )
        all_components = get_components_from_yaml(section_writer.current_yaml)
        comparison = sample_comparison
        component_data = section_writer._get_table_data_for_components(
            all_components, comparison
        )
        expected_result = [
            {
                "name": "go_files",
                "before": None,
                "after": ReportTotals(
                    files=1,
                    lines=8,
                    hits=5,
                    misses=3,
                    partials=0,
                    coverage="62.50000",
                    branches=0,
                    methods=0,
                    messages=0,
                    sessions=1,
                    complexity=10,
                    complexity_total=2,
                    diff=0,
                ),
                "diff": None,
            },
            {
                "name": "py_files",
                "before": None,
                "after": ReportTotals(
                    files=1,
                    lines=2,
                    hits=1,
                    misses=0,
                    partials=1,
                    coverage="50.00000",
                    branches=1,
                    methods=0,
                    messages=0,
                    sessions=1,
                    complexity=0,
                    complexity_total=0,
                    diff=0,
                ),
                "diff": None,
            },
        ]
        assert component_data == expected_result

    def test_write_message_component_section(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_comparison,
        snapshot,
    ):
        comparison = sample_comparison
        section_writer = ComponentsSectionWriter(
            repository=comparison.head.commit.repository,
            layout="layout",
            show_complexity=False,
            settings={},
            current_yaml={
                "component_management": {
                    "individual_components": [
                        {"component_id": "go_files", "paths": [r".*\.go"]},
                        {"component_id": "py_files", "paths": [r".*\.py"]},
                    ]
                }
            },
        )
        message = section_writer.write_section(
            comparison=comparison, diff=None, changes=None, links={"pull": "urlurl"}
        )
        assert snapshot("txt") == "\n".join(message)

    def test_write_message_component_section_no_base(
        self,
        dbsession,
        mock_configuration,
        mock_repo_provider,
        sample_comparison,
        snapshot,
    ):
        comparison = sample_comparison
        comparison.project_coverage_base.report = None
        comparison.project_coverage_base.commit = None
        section_writer = ComponentsSectionWriter(
            repository=comparison.head.commit.repository,
            layout="layout",
            show_complexity=False,
            settings={},
            current_yaml={
                "component_management": {
                    "individual_components": [
                        {"component_id": "go_files", "paths": [r".*\.go"]},
                        {"component_id": "py_files", "paths": [r".*\.py"]},
                    ]
                }
            },
        )
        message = section_writer.write_section(
            comparison=comparison, diff=None, changes=None, links={"pull": "urlurl"}
        )

        assert snapshot("txt") == "\n".join(message)


PROJECT_COVERAGE_CTA = ":information_source: You can also turn on [project coverage checks](https://docs.codecov.com/docs/common-recipe-list#set-project-coverage-checks-on-a-pull-request) and [project coverage reporting on Pull Request comment](https://docs.codecov.com/docs/common-recipe-list#show-project-coverage-changes-on-the-pull-request-comment)"


class TestCommentNotifierWelcome:
    def test_build_message(
        self, dbsession, mock_configuration, mock_repo_provider, sample_comparison
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"

        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"layout": "reach, diff, flags, files, footer"},
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        result = notifier.build_message(sample_comparison)
        expected_result = [
            "## Welcome to [Codecov](https://codecov.io) :tada:",
            "",
            "Once you merge this PR into your default branch, you're all set! Codecov will compare coverage reports and display results in all future pull requests.",
            "",
            "Thanks for integrating Codecov - We've got you covered :open_umbrella:",
        ]
        for exp, res in zip(expected_result, result):
            assert exp == res
        assert result == expected_result

    def test_build_message_with_preexisting_bundle_pulls(
        self, dbsession, mock_configuration, mock_repo_provider
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"

        owner = OwnerFactory.create(
            service="github",
            # Setting the time to _before_ patch centric default YAMLs start date of 2024-04-30
            createstamp=datetime(2023, 1, 1, tzinfo=UTC),
        )
        repository = RepositoryFactory.create(owner=owner)
        branch = "new_branch"
        # artificially create multiple pull entries with BA comments only
        ba_pull_one = PullFactory.create(
            repository=repository,
            base=CommitFactory.create(repository=repository).commitid,
            head=CommitFactory.create(repository=repository, branch=branch).commitid,
            commentid=None,
            bundle_analysis_commentid="98123978",
        )
        ba_pull_two = PullFactory.create(
            repository=repository,
            base=CommitFactory.create(repository=repository).commitid,
            head=CommitFactory.create(repository=repository, branch=branch).commitid,
            commentid=None,
            bundle_analysis_commentid="23982347",
        )
        # Add these entries first so they are created before the pull with commentid only
        dbsession.add_all([ba_pull_one, ba_pull_two])
        dbsession.flush()

        # Create new coverage pull
        base_commit = CommitFactory.create(repository=repository)
        head_commit = CommitFactory.create(repository=repository, branch=branch)
        pull = PullFactory.create(
            repository=repository,
            base=base_commit.commitid,
            head=head_commit.commitid,
        )

        head_report = Report()
        head_file = ReportFile("file_1.go")
        head_file.append(1, ReportLine.create(1, sessions=[[0, 1]], complexity=(10, 2)))
        head_report.append(head_file)

        base_report = Report()
        base_file = ReportFile("file_1.go")
        base_file.append(1, ReportLine.create(0, sessions=[[0, 1]], complexity=(10, 2)))
        base_report.append(base_file)

        head_full_commit = FullCommit(
            commit=head_commit, report=ReadOnlyReport.create_from_report(head_report)
        )
        base_full_commit = FullCommit(
            commit=base_commit, report=ReadOnlyReport.create_from_report(base_report)
        )
        comparison = ComparisonProxy(
            Comparison(
                head=head_full_commit,
                project_coverage_base=base_full_commit,
                patch_coverage_base_commitid=base_commit.commitid,
                enriched_pull=EnrichedPull(
                    database_pull=pull,
                    provider_pull={
                        "author": {"id": "12345", "username": "codecov-test-user"},
                        "base": {"branch": "master", "commitid": base_commit.commitid},
                        "head": {
                            "branch": "reason/some-testing",
                            "commitid": head_commit.commitid,
                        },
                        "number": str(pull.pullid),
                        "id": str(pull.pullid),
                        "state": "open",
                        "title": "Creating new code for reasons no one knows",
                    },
                ),
            )
        )
        dbsession.add_all([repository, base_commit, head_commit, pull])
        dbsession.flush()

        notifier = CommentNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"layout": "reach, diff, flags, files, footer"},
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        result = notifier.build_message(comparison)

        expected_result = [
            "## Welcome to [Codecov](https://codecov.io) :tada:",
            "",
            "Once you merge this PR into your default branch, you're all set! Codecov will compare coverage reports and display results in all future pull requests.",
            "",
            "Thanks for integrating Codecov - We've got you covered :open_umbrella:",
        ]
        for exp, res in zip(expected_result, result):
            assert exp == res
        assert result == expected_result

        pulls_in_db = dbsession.query(Pull).all()
        assert len(pulls_in_db) == 3

    def test_should_see_project_coverage_cta_public_repo(
        self, dbsession, mock_configuration, mock_repo_provider, sample_comparison
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"

        sample_comparison.head.commit.repository.private = False

        before_introduction_date = datetime(2024, 4, 1, 0, 0, 0).replace(tzinfo=UTC)
        sample_comparison.head.commit.repository.owner.createstamp = (
            before_introduction_date
        )

        dbsession.add_all(
            [
                sample_comparison.head.commit.repository,
                sample_comparison.head.commit.repository.owner,
            ]
        )
        dbsession.flush()

        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"layout": "reach, diff, flags, files, footer"},
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        result = notifier.build_message(sample_comparison)
        assert PROJECT_COVERAGE_CTA not in result

        after_introduction_date = datetime(2024, 6, 1, 0, 0, 0).replace(tzinfo=UTC)
        sample_comparison.head.commit.repository.owner.createstamp = (
            after_introduction_date
        )

        dbsession.add(sample_comparison.head.commit.repository.owner)
        dbsession.flush()

        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"layout": "reach, diff, flags, files, footer"},
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        result = notifier.build_message(sample_comparison)
        assert PROJECT_COVERAGE_CTA in result

    def test_should_see_project_coverage_cta_introduction_date(
        self, dbsession, mock_configuration, mock_repo_provider, sample_comparison
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"

        sample_comparison.head.commit.repository.private = True

        before_introduction_date = datetime(2024, 4, 1, 0, 0, 0).replace(tzinfo=UTC)
        sample_comparison.head.commit.repository.owner.createstamp = (
            before_introduction_date
        )

        dbsession.add_all(
            [
                sample_comparison.head.commit.repository,
                sample_comparison.head.commit.repository.owner,
            ]
        )
        dbsession.flush()

        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"layout": "reach, diff, flags, files, footer"},
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        result = notifier.build_message(sample_comparison)
        assert PROJECT_COVERAGE_CTA not in result

        after_introduction_date = datetime(2024, 6, 1, 0, 0, 0).replace(tzinfo=UTC)
        sample_comparison.head.commit.repository.owner.createstamp = (
            after_introduction_date
        )

        dbsession.add(sample_comparison.head.commit.repository.owner)
        dbsession.flush()

        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"layout": "reach, diff, flags, files, footer"},
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        result = notifier.build_message(sample_comparison)
        assert PROJECT_COVERAGE_CTA in result

    def test_should_see_project_coverage_cta_team_plan(
        self, dbsession, mock_configuration, mock_repo_provider, sample_comparison
    ):
        mock_configuration.params["setup"]["codecov_dashboard_url"] = "test.example.br"

        sample_comparison.head.commit.repository.private = True

        after_introduction_date = datetime(2024, 6, 1, 0, 0, 0).replace(tzinfo=UTC)
        sample_comparison.head.commit.repository.owner.createstamp = (
            after_introduction_date
        )

        sample_comparison.head.commit.repository.owner.plan = PlanName.TEAM_YEARLY.value

        dbsession.add_all(
            [
                sample_comparison.head.commit.repository,
                sample_comparison.head.commit.repository.owner,
            ]
        )
        dbsession.flush()

        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"layout": "reach, diff, flags, files, footer"},
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        result = notifier.build_message(sample_comparison)
        assert PROJECT_COVERAGE_CTA not in result

        sample_comparison.head.commit.repository.owner.plan = DEFAULT_FREE_PLAN

        dbsession.add(sample_comparison.head.commit.repository.owner)
        dbsession.flush()

        notifier = CommentNotifier(
            repository=sample_comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"layout": "reach, diff, flags, files, footer"},
            notifier_site_settings=True,
            current_yaml={},
            repository_service=mock_repo_provider,
        )
        result = notifier.build_message(sample_comparison)
        assert PROJECT_COVERAGE_CTA in result


class TestMessagesToUserSection:
    @pytest.mark.parametrize(
        "repo, is_enterprise, owner_has_apps, expected",
        [
            pytest.param(
                RepositoryFactory(owner__service="github", owner__integration_id=None),
                False,
                False,
                ":exclamation: Your organization needs to install the [Codecov GitHub app](https://github.com/apps/codecov/installations/select_target) to enable full functionality.",
                id="owner_not_using_app_should_emit_warning",
            ),
            pytest.param(
                RepositoryFactory(owner__service="github", owner__integration_id=None),
                True,
                False,
                "",
                id="is_enterprise_should_not_emit_warning",
            ),
            pytest.param(
                RepositoryFactory(
                    owner__service="github", owner__integration_id="integration_id"
                ),
                False,
                False,
                "",
                id="owner_using_app_legacy_should_not_emit_warning",
            ),
            pytest.param(
                RepositoryFactory(owner__service="github", owner__integration_id=None),
                False,
                True,
                "",
                id="owner_using_app_should_not_emit_warning",
            ),
        ],
    )
    def test_install_github_app_warning(
        self,
        mocker,
        repo: Repository,
        is_enterprise: bool,
        owner_has_apps: bool,
        expected: str,
    ):
        if owner_has_apps:
            repo.owner.github_app_installations = [
                GithubAppInstallationFactory(
                    owner=repo.owner, app_id=10, installation_id=1000
                )
            ]
        commit = CommitFactory(repository=repo)
        mock_head = mocker.MagicMock(commit=commit)
        mocker.patch(
            "services.notification.notifiers.mixins.message.sections.is_enterprise",
            return_value=is_enterprise,
        )
        fake_comparison = mocker.MagicMock(head=mock_head)
        assert (
            MessagesToUserSectionWriter(
                repo, mocker.MagicMock(), False, mocker.MagicMock(), {}
            )._write_install_github_app_warning(fake_comparison)
            == expected
        )

    @pytest.mark.parametrize(
        "commit, upload_diff, has_project_status, is_coverage_drop_significant, expected",
        [
            pytest.param(
                CommitFactory(state="pending"),
                [ReportUploadedCount(flag="unit", base_count=4, head_count=3)],
                True,
                True,
                "",
                id="commit_not_ready_should_not_send_warning",
            ),
            pytest.param(
                CommitFactory(state="complete"),
                [ReportUploadedCount(flag="unit", base_count=4, head_count=3)],
                False,
                True,
                "",
                id="no_project_status_should_not_send_warning",
            ),
            pytest.param(
                CommitFactory(state="complete"),
                [ReportUploadedCount(flag="unit", base_count=4, head_count=3)],
                True,
                False,
                "",
                id="no_significant_drop_in_coverage_should_not_send_warning",
            ),
            pytest.param(
                CommitFactory(state="complete"),
                [],
                True,
                True,
                "",
                id="no_upload_diff_should_not_send_warning",
            ),
            pytest.param(
                CommitFactory(
                    state="complete",
                    commitid="cf59ea49c149c8ef5d7303834362a4d27bbd4a28",
                ),
                [
                    ReportUploadedCount(flag="unit", base_count=4, head_count=3),
                    ReportUploadedCount(flag="local", base_count=2, head_count=1),
                ],
                True,
                True,
                (
                    "> :exclamation:  There is a different number of reports uploaded between BASE (bbd4a28) and HEAD (cf59ea4). Click for more details."
                    + "\n> "
                    + "\n> <details><summary>HEAD has 2 uploads less than BASE</summary>"
                    + "\n>"
                    + "\n>| Flag | BASE (bbd4a28) | HEAD (cf59ea4) |"
                    + "\n>|------|------|------|"
                    + "\n>|unit|4|3|"
                    + "\n>|local|2|1|"
                    + "\n></details>"
                ),
                id="should_send_warning_2_uploads_less",
            ),
            pytest.param(
                CommitFactory(
                    state="complete",
                    commitid="cf59ea49c149c8ef5d7303834362a4d27bbd4a28",
                ),
                [
                    ReportUploadedCount(flag="unit", base_count=4, head_count=3),
                ],
                True,
                True,
                (
                    "> :exclamation:  There is a different number of reports uploaded between BASE (bbd4a28) and HEAD (cf59ea4). Click for more details."
                    + "\n> "
                    + "\n> <details><summary>HEAD has 1 upload less than BASE</summary>"
                    + "\n>"
                    + "\n>| Flag | BASE (bbd4a28) | HEAD (cf59ea4) |"
                    + "\n>|------|------|------|"
                    + "\n>|unit|4|3|"
                    + "\n></details>"
                ),
                id="should_send_warning_1_upload_less",
            ),
        ],
    )
    def test_different_upload_count_warning(
        self,
        mocker,
        commit: Commit,
        upload_diff: list[ReportUploadedCount],
        has_project_status: bool,
        is_coverage_drop_significant: bool,
        expected: str,
    ):
        yaml = {"coverage": {"status": {"project": has_project_status}}}
        mocker.patch(
            "services.notification.notifiers.mixins.message.sections.is_coverage_drop_significant",
            return_value=is_coverage_drop_significant,
        )
        mock_head = mocker.MagicMock(commit=commit)
        mock_project_coverage_base = mocker.MagicMock(
            commit=CommitFactory(
                repository=commit.repository,
                commitid="bbd4a28cf59ea49c149c8ef5d7303834362a4d27",
            )
        )
        fake_comparison = mocker.MagicMock(
            head=mock_head, project_coverage_base=mock_project_coverage_base
        )
        fake_comparison.get_reports_uploaded_count_per_flag_diff.return_value = (
            upload_diff
        )
        assert (
            MessagesToUserSectionWriter(
                commit.repository, mocker.MagicMock(), False, mocker.MagicMock(), yaml
            )._write_different_upload_count_warning(fake_comparison)
            == expected
        )
