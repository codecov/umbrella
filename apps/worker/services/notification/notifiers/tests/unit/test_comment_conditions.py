from unittest.mock import MagicMock

import pytest

from shared.django_apps.enums import Decoration
from database.models.core import Repository
from services.comparison import ComparisonProxy
from services.notification.notifiers.base import AbstractBaseNotifier
from services.notification.notifiers.comment import CommentNotifier
from services.notification.notifiers.comment.conditions import (
    ComparisonHasPull,
    HasEnoughBuilds,
    HasEnoughRequiredChanges,
    NoAutoActivateMessageIfAutoActivateIsOff,
    NotifyCondition,
    PullHeadMatchesComparisonHead,
    PullRequestInProvider,
    PullRequestOpen,
)
from shared.validation.types import (
    CoverageCommentRequiredChanges,
    CoverageCommentRequiredChangesANDGroup,
)
from shared.yaml import UserYaml


def _get_notifier(
    repository: Repository,
    required_changes: CoverageCommentRequiredChangesANDGroup,
    repo_provider,
):
    return CommentNotifier(
        repository=repository,
        title="title",
        notifier_yaml_settings={"require_changes": required_changes},
        notifier_site_settings=True,
        current_yaml={},
        repository_service=repo_provider,
    )


def _get_mock_compare_result(file_affected: str, lines_affected: list[str]) -> dict:
    return {
        "diff": {
            "files": {
                file_affected: {
                    "type": "modified",
                    "before": None,
                    "segments": [
                        {
                            "header": lines_affected,
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
                    ],
                    "stats": {"added": 3, "removed": 3},
                }
            }
        }
    }


@pytest.mark.parametrize(
    "comparison_name, condition, expected",
    [
        pytest.param(
            "sample_comparison",
            [CoverageCommentRequiredChanges.any_change.value],
            True,
            id="any_change_comparison_with_changes",
        ),
        pytest.param(
            "sample_comparison_without_base_report",
            [CoverageCommentRequiredChanges.any_change.value],
            False,
            id="any_change_comparison_without_base",
        ),
        pytest.param(
            "sample_comparison_no_change",
            [CoverageCommentRequiredChanges.any_change.value],
            False,
            id="any_change_sample_comparison_no_change",
        ),
        pytest.param(
            "sample_comparison",
            [CoverageCommentRequiredChanges.coverage_drop.value],
            False,
            id="coverage_drop_comparison_with_positive_changes",
        ),
        pytest.param(
            "sample_comparison_no_change",
            [CoverageCommentRequiredChanges.coverage_drop.value],
            False,
            id="coverage_drop_sample_comparison_no_change",
        ),
        pytest.param(
            "sample_comparison_without_base_report",
            [CoverageCommentRequiredChanges.coverage_drop.value],
            True,
            id="coverage_drop_comparison_without_base",
        ),
    ],
)
def test_condition_different_comparisons_no_diff(
    comparison_name, condition, expected, mock_repo_provider, request
):
    comparison = request.getfixturevalue(comparison_name)
    # There's no diff between HEAD and BASE so we can't calculate unexpected coverage.
    # Any change then needs to be a coverage change
    mock_repo_provider.get_compare.return_value = {"diff": {"files": {}, "commits": []}}
    notifier = _get_notifier(
        comparison.head.commit.repository, condition, mock_repo_provider
    )
    assert HasEnoughRequiredChanges.check_condition(notifier, comparison) == expected


@pytest.mark.parametrize(
    "condition, expected",
    [
        pytest.param(
            [CoverageCommentRequiredChanges.any_change.value], False, id="any_change"
        ),
        pytest.param(
            [CoverageCommentRequiredChanges.coverage_drop.value],
            False,
            id="coverage_drop",
        ),
        pytest.param(
            [CoverageCommentRequiredChanges.uncovered_patch.value],
            False,
            id="uncovered_patch",
        ),
        pytest.param(
            [CoverageCommentRequiredChanges.no_requirements.value],
            True,
            id="no_requirements",
        ),
    ],
)
def test_condition_exact_same_report_coverage_not_affected_by_diff(
    sample_comparison_no_change, mock_repo_provider, condition, expected
):
    mock_repo_provider.get_compare.return_value = _get_mock_compare_result(
        "README.md", ["5", "8", "5", "9"]
    )
    notifier = _get_notifier(
        sample_comparison_no_change.head.commit.repository,
        condition,
        mock_repo_provider,
    )
    assert (
        HasEnoughRequiredChanges.check_condition(notifier, sample_comparison_no_change)
        == expected
    )


@pytest.mark.parametrize(
    "condition, expected",
    [
        pytest.param(
            [CoverageCommentRequiredChanges.any_change.value], True, id="any_change"
        ),
        pytest.param(
            [CoverageCommentRequiredChanges.coverage_drop.value],
            False,
            id="coverage_drop",
        ),
        pytest.param(
            [CoverageCommentRequiredChanges.uncovered_patch.value],
            False,
            id="uncovered_patch",
        ),
        pytest.param(
            [CoverageCommentRequiredChanges.no_requirements.value],
            True,
            id="no_requirements",
        ),
    ],
)
def test_condition_exact_same_report_coverage_affected_by_diff(
    sample_comparison_no_change, mock_repo_provider, condition, expected
):
    mock_repo_provider.get_compare.return_value = _get_mock_compare_result(
        "file_1.go", ["4", "8", "4", "8"]
    )
    notifier = _get_notifier(
        sample_comparison_no_change.head.commit.repository,
        condition,
        mock_repo_provider,
    )
    assert (
        HasEnoughRequiredChanges.check_condition(notifier, sample_comparison_no_change)
        == expected
    )


@pytest.mark.parametrize(
    "affected_lines, expected",
    [
        pytest.param(["4", "8", "4", "8"], False, id="patch_100%_covered"),
        pytest.param(["1", "8", "1", "8"], True, id="patch_NOT_100%_covered"),
    ],
)
def test_uncovered_patch(
    sample_comparison_no_change, mock_repo_provider, affected_lines, expected
):
    mock_repo_provider.get_compare.return_value = _get_mock_compare_result(
        "file_1.go", affected_lines
    )
    notifier = _get_notifier(
        sample_comparison_no_change.head.commit.repository,
        [CoverageCommentRequiredChanges.uncovered_patch.value],
        mock_repo_provider,
    )
    assert (
        HasEnoughRequiredChanges.check_condition(notifier, sample_comparison_no_change)
        == expected
    )


@pytest.mark.parametrize(
    "comparison_name, yaml, expected",
    [
        pytest.param("sample_comparison", {}, False, id="positive_change"),
        pytest.param(
            "sample_comparison_negative_change",
            {},
            True,
            id="negative_change_no_extra_config",
        ),
        pytest.param(
            "sample_comparison_negative_change",
            {"coverage": {"status": {"project": True}}},
            True,
            id="negative_change_bool_config",
        ),
        pytest.param(
            "sample_comparison_negative_change",
            {"coverage": {"status": {"project": {"threshold": 10}}}},
            False,
            id="negative_change_high_threshold",
        ),
    ],
)
def test_coverage_drop_with_different_project_configs(
    comparison_name, yaml, expected, request
):
    comparison: ComparisonProxy = request.getfixturevalue(comparison_name)
    comparison.comparison.current_yaml = UserYaml(yaml)
    notifier = _get_notifier(
        comparison.head.commit.repository,
        [CoverageCommentRequiredChanges.coverage_drop.value],
        None,
    )
    assert HasEnoughRequiredChanges.check_condition(notifier, comparison) == expected


@pytest.mark.parametrize(
    "decoration_type, plan_auto_activate, expected",
    [
        pytest.param(
            Decoration.upgrade, False, False, id="upgrade_no_auto_activate__dont_send"
        ),
        pytest.param(Decoration.upgrade, True, True, id="upgrade_auto_activate__send"),
        pytest.param(
            Decoration.upload_limit,
            False,
            True,
            id="other_decoration_no_auto_activate__send",
        ),
        pytest.param(
            Decoration.upload_limit,
            True,
            True,
            id="other_decoration_auto_activate__send",
        ),
    ],
)
def test_no_auto_activate_message_if_auto_activate_is_off(
    sample_comparison_no_change,
    mock_repo_provider,
    decoration_type,
    plan_auto_activate,
    expected,
):
    notifier = _get_notifier(
        sample_comparison_no_change.head.commit.repository,
        [CoverageCommentRequiredChanges.any_change.value],
        mock_repo_provider,
    )
    notifier.decoration_type = decoration_type
    notifier.repository.author.plan_auto_activate = plan_auto_activate
    assert (
        NoAutoActivateMessageIfAutoActivateIsOff.check_condition(
            notifier, sample_comparison_no_change
        )
        == expected
    )


class TestComparisonHasPull:
    def test_check_condition_with_pull(self, sample_comparison):
        notifier = _get_notifier(
            sample_comparison.head.commit.repository,
            [CoverageCommentRequiredChanges.any_change.value],
            None,
        )
        assert ComparisonHasPull.check_condition(notifier, sample_comparison) is True

    def test_check_condition_without_pull(self, sample_comparison_without_pull):
        notifier = _get_notifier(
            sample_comparison_without_pull.head.commit.repository,
            [CoverageCommentRequiredChanges.any_change.value],
            None,
        )
        assert (
            ComparisonHasPull.check_condition(notifier, sample_comparison_without_pull)
            is False
        )


class TestPullRequestInProvider:
    def test_check_condition_with_provider_pull(self, sample_comparison):
        notifier = _get_notifier(
            sample_comparison.head.commit.repository,
            [CoverageCommentRequiredChanges.any_change.value],
            None,
        )
        assert (
            PullRequestInProvider.check_condition(notifier, sample_comparison) is True
        )

    def test_check_condition_without_provider_pull(
        self, sample_comparison_database_pull_without_provider
    ):
        notifier = _get_notifier(
            sample_comparison_database_pull_without_provider.head.commit.repository,
            [CoverageCommentRequiredChanges.any_change.value],
            None,
        )
        assert (
            PullRequestInProvider.check_condition(
                notifier, sample_comparison_database_pull_without_provider
            )
            is False
        )

    def test_check_condition_without_enriched_pull(
        self, sample_comparison_without_pull
    ):
        notifier = _get_notifier(
            sample_comparison_without_pull.head.commit.repository,
            [CoverageCommentRequiredChanges.any_change.value],
            None,
        )
        assert (
            PullRequestInProvider.check_condition(
                notifier, sample_comparison_without_pull
            )
            is False
        )


class TestPullRequestOpen:
    def test_check_condition_with_open_pull(self, sample_comparison):
        notifier = _get_notifier(
            sample_comparison.head.commit.repository,
            [CoverageCommentRequiredChanges.any_change.value],
            None,
        )
        assert PullRequestOpen.check_condition(notifier, sample_comparison) is True

    def test_check_condition_with_closed_pull(self, sample_comparison):
        sample_comparison.pull.state = "closed"
        notifier = _get_notifier(
            sample_comparison.head.commit.repository,
            [CoverageCommentRequiredChanges.any_change.value],
            None,
        )
        assert PullRequestOpen.check_condition(notifier, sample_comparison) is False

    def test_check_condition_with_merged_pull(self, sample_comparison):
        sample_comparison.pull.state = "merged"
        notifier = _get_notifier(
            sample_comparison.head.commit.repository,
            [CoverageCommentRequiredChanges.any_change.value],
            None,
        )
        assert PullRequestOpen.check_condition(notifier, sample_comparison) is False


class TestPullHeadMatchesComparisonHead:
    def test_check_condition_with_matching_head(self, sample_comparison):
        notifier = _get_notifier(
            sample_comparison.head.commit.repository,
            [CoverageCommentRequiredChanges.any_change.value],
            None,
        )
        assert (
            PullHeadMatchesComparisonHead.check_condition(notifier, sample_comparison)
            is True
        )

    def test_check_condition_with_non_matching_head(self, sample_comparison):
        # Change the pull head to not match comparison head
        sample_comparison.pull.head = "different_commit_id"
        notifier = _get_notifier(
            sample_comparison.head.commit.repository,
            [CoverageCommentRequiredChanges.any_change.value],
            None,
        )
        assert (
            PullHeadMatchesComparisonHead.check_condition(notifier, sample_comparison)
            is False
        )


class TestHasEnoughBuilds:
    def test_check_condition_with_enough_builds(self, sample_comparison):
        notifier = _get_notifier(
            sample_comparison.head.commit.repository,
            [CoverageCommentRequiredChanges.any_change.value],
            None,
        )
        notifier.notifier_yaml_settings = {"after_n_builds": 1}
        # sample_comparison has at least 1 session/build
        assert HasEnoughBuilds.check_condition(notifier, sample_comparison) is True

    def test_check_condition_with_not_enough_builds(self, sample_comparison):
        notifier = _get_notifier(
            sample_comparison.head.commit.repository,
            [CoverageCommentRequiredChanges.any_change.value],
            None,
        )
        notifier.notifier_yaml_settings = {"after_n_builds": 10}
        # sample_comparison has fewer than 10 builds
        assert HasEnoughBuilds.check_condition(notifier, sample_comparison) is False

    def test_check_condition_with_zero_required_builds(self, sample_comparison):
        notifier = _get_notifier(
            sample_comparison.head.commit.repository,
            [CoverageCommentRequiredChanges.any_change.value],
            None,
        )
        notifier.notifier_yaml_settings = {"after_n_builds": 0}
        assert HasEnoughBuilds.check_condition(notifier, sample_comparison) is True

    def test_check_condition_with_no_head_report(self, sample_comparison):
        sample_comparison.head.report = None
        notifier = _get_notifier(
            sample_comparison.head.commit.repository,
            [CoverageCommentRequiredChanges.any_change.value],
            None,
        )
        notifier.notifier_yaml_settings = {"after_n_builds": 5}
        # Should pass when there's no head report
        assert HasEnoughBuilds.check_condition(notifier, sample_comparison) is True

    def test_check_condition_with_exact_build_count(self, sample_comparison):
        notifier = _get_notifier(
            sample_comparison.head.commit.repository,
            [CoverageCommentRequiredChanges.any_change.value],
            None,
        )
        # Set required builds to match the number of sessions in the report
        build_count = len(sample_comparison.head.report.sessions)
        notifier.notifier_yaml_settings = {"after_n_builds": build_count}
        assert HasEnoughBuilds.check_condition(notifier, sample_comparison) is True


class TestHasEnoughRequiredChangesAdditional:
    def test_check_condition_with_existing_commentid(self, sample_comparison):
        sample_comparison.pull.commentid = "12345"
        notifier = _get_notifier(
            sample_comparison.head.commit.repository,
            [CoverageCommentRequiredChanges.coverage_drop.value],
            None,
        )
        # Should return True when commentid exists, skipping verification
        assert (
            HasEnoughRequiredChanges.check_condition(notifier, sample_comparison)
            is True
        )

    def test_check_condition_with_backwards_compatibility_bool_true(
        self, sample_comparison, mock_repo_provider
    ):
        mock_repo_provider.get_compare.return_value = _get_mock_compare_result(
            "file_1.go", ["4", "8", "4", "8"]
        )
        notifier = _get_notifier(
            sample_comparison.head.commit.repository,
            [CoverageCommentRequiredChanges.any_change.value],
            mock_repo_provider,
        )
        notifier.notifier_yaml_settings = {"require_changes": True}
        # True should be converted to 1 (any_change)
        assert (
            HasEnoughRequiredChanges.check_condition(notifier, sample_comparison)
            is True
        )

    def test_check_condition_with_backwards_compatibility_bool_false(
        self, sample_comparison, mock_repo_provider
    ):
        mock_repo_provider.get_compare.return_value = _get_mock_compare_result(
            "README.md", ["5", "8", "5", "9"]
        )
        notifier = _get_notifier(
            sample_comparison.head.commit.repository,
            [CoverageCommentRequiredChanges.any_change.value],
            mock_repo_provider,
        )
        notifier.notifier_yaml_settings = {"require_changes": False}
        # False should be converted to 0 (no_requirements)
        assert (
            HasEnoughRequiredChanges.check_condition(notifier, sample_comparison)
            is True
        )

    def test_check_condition_with_multiple_or_groups(
        self, sample_comparison, mock_repo_provider
    ):
        mock_repo_provider.get_compare.return_value = _get_mock_compare_result(
            "file_1.go", ["4", "8", "4", "8"]
        )
        notifier = _get_notifier(
            sample_comparison.head.commit.repository,
            [
                CoverageCommentRequiredChanges.any_change.value,
                CoverageCommentRequiredChanges.coverage_drop.value,
            ],
            mock_repo_provider,
        )
        # Both conditions need to pass (AND logic)
        assert (
            HasEnoughRequiredChanges.check_condition(notifier, sample_comparison)
            is False
        )

    def test_check_unexpected_changes_with_changes(self, sample_comparison):
        # Mock get_changes to return a non-empty list (truthy)
        sample_comparison.get_changes = lambda: ["change1", "change2"]
        assert (
            HasEnoughRequiredChanges._check_unexpected_changes(sample_comparison)
            is True
        )

    def test_check_unexpected_changes_without_changes(self, sample_comparison):
        # Mock get_changes to return None or empty list (falsy)
        sample_comparison.get_changes = lambda: None
        assert (
            HasEnoughRequiredChanges._check_unexpected_changes(sample_comparison)
            is False
        )

    def test_check_unexpected_changes_with_empty_list(self, sample_comparison):
        # Mock get_changes to return empty list (falsy)
        sample_comparison.get_changes = lambda: []
        assert (
            HasEnoughRequiredChanges._check_unexpected_changes(sample_comparison)
            is False
        )

    def test_check_coverage_change_with_valid_diff(self, sample_comparison):
        # Mock a valid diff result
        mock_diff = {"files": {"file1.py": {}}}
        mock_diff_result = {"general": type("obj", (object,), {"lines": 10})()}
        sample_comparison.get_diff = lambda: mock_diff
        sample_comparison.head.report.calculate_diff = lambda diff: mock_diff_result
        assert (
            HasEnoughRequiredChanges._check_coverage_change(sample_comparison) is True
        )

    def test_check_coverage_change_with_zero_lines(self, sample_comparison):
        # Mock a diff result with zero lines
        mock_diff = {"files": {"file1.py": {}}}
        mock_diff_result = {"general": type("obj", (object,), {"lines": 0})()}
        sample_comparison.get_diff = lambda: mock_diff
        sample_comparison.head.report.calculate_diff = lambda diff: mock_diff_result
        assert (
            HasEnoughRequiredChanges._check_coverage_change(sample_comparison) is False
        )

    def test_check_coverage_change_with_no_head_report(self, sample_comparison):
        sample_comparison.head.report = None
        assert (
            HasEnoughRequiredChanges._check_coverage_change(sample_comparison) is False
        )

    def test_check_coverage_drop_with_no_head_coverage(self, sample_comparison):
        sample_comparison.head.report.totals.coverage = None
        # Should default to True when we can't determine coverage drop
        assert HasEnoughRequiredChanges._check_coverage_drop(sample_comparison) is True

    def test_check_coverage_drop_with_no_base_report(self, sample_comparison):
        sample_comparison.project_coverage_base.report = None
        # Should default to True when we can't determine coverage drop
        assert HasEnoughRequiredChanges._check_coverage_drop(sample_comparison) is True

    def test_check_coverage_drop_with_no_base_coverage(self, sample_comparison):
        sample_comparison.project_coverage_base.report.totals.coverage = None
        # Should default to True when we can't determine coverage drop
        assert HasEnoughRequiredChanges._check_coverage_drop(sample_comparison) is True

    def test_check_coverage_drop_with_positive_change(self, sample_comparison):
        # Set up comparison with positive coverage change
        sample_comparison.head.report.totals.coverage = 80.0
        sample_comparison.project_coverage_base.report.totals.coverage = 70.0
        sample_comparison.comparison.current_yaml = UserYaml({})
        assert HasEnoughRequiredChanges._check_coverage_drop(sample_comparison) is False

    def test_check_coverage_drop_with_small_drop_below_threshold(
        self, sample_comparison
    ):
        # Set up comparison with small drop below threshold
        sample_comparison.head.report.totals.coverage = 79.99
        sample_comparison.project_coverage_base.report.totals.coverage = 80.0
        sample_comparison.comparison.current_yaml = UserYaml({})
        # Drop of 0.01% is below the 0.01 threshold
        assert HasEnoughRequiredChanges._check_coverage_drop(sample_comparison) is False

    def test_check_coverage_drop_with_threshold(self, sample_comparison):
        # Set up comparison with drop that exceeds threshold
        sample_comparison.head.report.totals.coverage = 85.0
        sample_comparison.project_coverage_base.report.totals.coverage = 100.0
        sample_comparison.comparison.current_yaml = UserYaml(
            {"coverage": {"status": {"project": {"threshold": 10}}}}
        )
        # Drop of 15% exceeds threshold of 10%
        assert HasEnoughRequiredChanges._check_coverage_drop(sample_comparison) is True

    def test_check_uncovered_patch_with_no_head_report(self, sample_comparison):
        sample_comparison.head.report = None
        assert (
            HasEnoughRequiredChanges._check_uncovered_patch(sample_comparison) is False
        )

    def test_check_uncovered_patch_with_no_diff(self, sample_comparison):
        sample_comparison.get_diff = lambda use_original_base=False: None
        assert (
            HasEnoughRequiredChanges._check_uncovered_patch(sample_comparison) is False
        )

    def test_check_uncovered_patch_with_zero_lines(self, sample_comparison):
        # Mock apply_diff to return totals with zero lines
        mock_totals = type("obj", (object,), {"lines": 0, "coverage": 100.0})()
        sample_comparison.get_diff = lambda use_original_base=False: {"files": {}}
        sample_comparison.head.report.apply_diff = lambda diff: mock_totals
        assert (
            HasEnoughRequiredChanges._check_uncovered_patch(sample_comparison) is False
        )

    def test_check_uncovered_patch_with_full_coverage(self, sample_comparison):
        # Mock apply_diff to return 100% coverage
        mock_totals = type("obj", (object,), {"lines": 10, "coverage": 100.0})()
        sample_comparison.get_diff = lambda use_original_base=False: {"files": {}}
        sample_comparison.head.report.apply_diff = lambda diff: mock_totals
        assert (
            HasEnoughRequiredChanges._check_uncovered_patch(sample_comparison) is False
        )

    def test_check_uncovered_patch_with_uncovered_lines(self, sample_comparison):
        # Mock apply_diff to return less than 100% coverage
        mock_totals = type("obj", (object,), {"lines": 10, "coverage": 80.0})()
        sample_comparison.get_diff = lambda use_original_base=False: {"files": {}}
        sample_comparison.head.report.apply_diff = lambda diff: mock_totals
        assert (
            HasEnoughRequiredChanges._check_uncovered_patch(sample_comparison) is True
        )

    def test_check_condition_OR_group_no_requirements(self, sample_comparison):
        result = HasEnoughRequiredChanges.check_condition_OR_group(
            CoverageCommentRequiredChanges.no_requirements.value, sample_comparison
        )
        assert result is True

    def test_check_condition_OR_group_any_change(self, sample_comparison):
        # Mock get_changes and get_diff to avoid API calls
        sample_comparison.get_changes = lambda: ["change1"]
        sample_comparison.get_diff = lambda use_original_base=False: {"files": {}}
        # Mock calculate_diff to avoid issues in _check_coverage_change
        sample_comparison.head.report.calculate_diff = lambda diff: None
        result = HasEnoughRequiredChanges.check_condition_OR_group(
            CoverageCommentRequiredChanges.any_change.value, sample_comparison
        )
        assert result is True

    def test_check_condition_OR_group_combined_flags(self, sample_comparison):
        # Test with combined flags (OR group)
        combined_value = (
            CoverageCommentRequiredChanges.any_change.value
            | CoverageCommentRequiredChanges.coverage_drop.value
        )
        # Mock get_changes and get_diff to avoid API calls
        sample_comparison.get_changes = lambda: None
        sample_comparison.get_diff = lambda use_original_base=False: {"files": {}}
        # Mock calculate_diff to avoid issues in _check_coverage_change
        sample_comparison.head.report.calculate_diff = lambda diff: None
        sample_comparison.head.report.totals.coverage = 70.0
        sample_comparison.project_coverage_base.report.totals.coverage = 80.0
        sample_comparison.comparison.current_yaml = UserYaml({})
        result = HasEnoughRequiredChanges.check_condition_OR_group(
            combined_value, sample_comparison
        )
        # Should return True if any condition in the OR group passes (coverage_drop should pass)
        assert result is True


class TestNotifyCondition:
    def test_on_failure_side_effect_default(self):
        notifier = MagicMock(spec=AbstractBaseNotifier)
        comparison = MagicMock()
        result = NotifyCondition.on_failure_side_effect(notifier, comparison)
        assert result.notification_attempted is False
        assert result.notification_successful is False
        assert result.explanation is None
        assert result.data_sent is None
        assert result.data_received is None
