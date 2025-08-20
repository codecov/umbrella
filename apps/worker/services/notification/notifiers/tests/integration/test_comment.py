from unittest.mock import PropertyMock

import pytest

from database.tests.factories import CommitFactory, PullFactory, RepositoryFactory
from services.comparison import ComparisonContext, ComparisonProxy
from services.comparison.types import Comparison, EnrichedPull, FullCommit
from services.decoration import Decoration
from services.notification.notifiers.comment import CommentNotifier
from shared.reports.readonly import ReadOnlyReport
from tests.helpers import mock_all_plans_and_tiers


@pytest.fixture
def is_not_first_pull(mocker):
    mocker.patch(
        "database.models.core.Pull.is_first_coverage_pull",
        return_value=False,
        new_callable=PropertyMock,
    )


@pytest.fixture
def sample_comparison(dbsession, request, sample_report, small_report):
    repository = RepositoryFactory.create(
        author__service="github",
        author__username="joseph-sentry",
        name="codecov-demo",
        author__unencrypted_oauth_token="ghp_testmgzs9qm7r27wp376fzv10aobbpva7hd3",
        image_token="abcdefghij",
    )
    dbsession.add(repository)
    dbsession.flush()
    base_commit = CommitFactory.create(
        repository=repository, commitid="5b174c2b40d501a70c479e91025d5109b1ad5c1b"
    )
    head_commit = CommitFactory.create(
        repository=repository,
        branch="test",
        commitid="5601846871b8142ab0df1e0b8774756c658bcc7d",
    )
    pull = PullFactory.create(
        repository=repository,
        base=base_commit.commitid,
        head=head_commit.commitid,
        pullid=9,
    )
    dbsession.add(base_commit)
    dbsession.add(head_commit)
    dbsession.add(pull)
    dbsession.flush()
    repository = base_commit.repository
    base_full_commit = FullCommit(
        commit=base_commit, report=ReadOnlyReport.create_from_report(small_report)
    )
    head_full_commit = FullCommit(
        commit=head_commit, report=ReadOnlyReport.create_from_report(sample_report)
    )
    return ComparisonProxy(
        Comparison(
            head=head_full_commit,
            project_coverage_base=base_full_commit,
            patch_coverage_base_commitid=base_commit.commitid,
            enriched_pull=EnrichedPull(
                database_pull=pull,
                provider_pull={
                    "author": {"id": "12345", "username": "joseph-sentry"},
                    "base": {
                        "branch": "main",
                        "commitid": "5b174c2b40d501a70c479e91025d5109b1ad5c1b",
                    },
                    "head": {
                        "branch": "test",
                        "commitid": "5601846871b8142ab0df1e0b8774756c658bcc7d",
                    },
                    "state": "open",
                    "title": "make change",
                    "id": "9",
                    "number": "9",
                },
            ),
        )
    )


@pytest.fixture
def sample_comparison_gitlab(dbsession, request, sample_report, small_report):
    repository = RepositoryFactory.create(
        author__username="joseph-sentry",
        author__service="gitlab",
        author__unencrypted_oauth_token="test1nioqi3p3681oa43",
        service_id="47404140",
        name="example-python",
        image_token="abcdefghij",
    )
    dbsession.add(repository)
    dbsession.flush()
    base_commit = CommitFactory.create(
        repository=repository, commitid="0fc784af11c401449e56b24a174bae7b9af86c98"
    )
    head_commit = CommitFactory.create(
        repository=repository,
        branch="behind",
        commitid="0b6a213fc300cd328c0625f38f30432ee6e066e5",
    )
    pull = PullFactory.create(
        repository=repository,
        base=base_commit.commitid,
        head=head_commit.commitid,
        pullid=5,
    )
    dbsession.add(base_commit)
    dbsession.add(head_commit)
    dbsession.add(pull)
    dbsession.flush()
    repository = base_commit.repository
    base_full_commit = FullCommit(
        commit=base_commit, report=ReadOnlyReport.create_from_report(small_report)
    )
    head_full_commit = FullCommit(
        commit=head_commit, report=ReadOnlyReport.create_from_report(sample_report)
    )
    return ComparisonProxy(
        Comparison(
            head=head_full_commit,
            project_coverage_base=base_full_commit,
            patch_coverage_base_commitid=base_commit.commitid,
            enriched_pull=EnrichedPull(
                database_pull=pull,
                provider_pull={
                    "author": {"id": "15014576", "username": "joseph-sentry"},
                    "base": {
                        "branch": "main",
                        "commitid": "0fc784af11c401449e56b24a174bae7b9af86c98",
                    },
                    "head": {
                        "branch": "behind",
                        "commitid": "0b6a213fc300cd328c0625f38f30432ee6e066e5",
                    },
                    "state": "open",
                    "title": "Behind",
                    "id": "1",
                    "number": "1",
                },
            ),
        )
    )


@pytest.fixture
def sample_comparison_for_upgrade(dbsession, request, sample_report, small_report):
    repository = RepositoryFactory.create(
        author__service="github",
        author__username="codecove2e",
        name="example-python",
        author__unencrypted_oauth_token="ghp_testgkdo1u8jqexy9wabk1n0puoetf9ziam5",
        image_token="abcdefghij",
    )
    dbsession.add(repository)
    dbsession.flush()
    base_commit = CommitFactory.create(
        repository=repository, commitid="93189ce50f224296d6412e2884b93dcc3c7c8654"
    )
    head_commit = CommitFactory.create(
        repository=repository,
        branch="new_branch",
        commitid="8589c19ce95a2b13cf7b3272cbf275ca9651ae9c",
    )
    pull = PullFactory.create(
        repository=repository,
        base=base_commit.commitid,
        head=head_commit.commitid,
        pullid=2,
    )
    dbsession.add(base_commit)
    dbsession.add(head_commit)
    dbsession.add(pull)
    dbsession.flush()
    repository = base_commit.repository
    base_full_commit = FullCommit(
        commit=base_commit, report=ReadOnlyReport.create_from_report(small_report)
    )
    head_full_commit = FullCommit(
        commit=head_commit, report=ReadOnlyReport.create_from_report(sample_report)
    )
    return ComparisonProxy(
        Comparison(
            head=head_full_commit,
            project_coverage_base=base_full_commit,
            patch_coverage_base_commitid=base_commit.commitid,
            enriched_pull=EnrichedPull(
                database_pull=pull,
                provider_pull={
                    "author": {"id": "12345", "username": "codecove2e"},
                    "base": {
                        "branch": "master",
                        "commitid": "93189ce50f224296d6412e2884b93dcc3c7c8654",
                    },
                    "head": {
                        "branch": "codecove2e-patch-3",
                        "commitid": "8589c19ce95a2b13cf7b3272cbf275ca9651ae9c",
                    },
                    "state": "open",
                    "title": "Update __init__.py",
                    "id": "4",
                    "number": "4",
                },
            ),
        )
    )


@pytest.fixture
def sample_comparison_for_limited_upload(
    dbsession, request, sample_report, small_report
):
    repository = RepositoryFactory.create(
        author__username="test-acc9",
        author__service="github",
        name="priv_example",
        author__unencrypted_oauth_token="ghp_test1xwr5rxl12dbm97a7r4anr6h67uw0thf",
        image_token="abcdefghij",
    )
    dbsession.add(repository)
    dbsession.flush()
    base_commit = CommitFactory.create(
        repository=repository, commitid="ef6edf5ae6643d53a7971fb8823d3f7b2ac65619"
    )
    head_commit = CommitFactory.create(
        repository=repository,
        branch="featureA",
        commitid="610ada9fa2bbc49f1a08917da3f73bef2d03709c",
    )
    pull = PullFactory.create(
        repository=repository,
        base=base_commit.commitid,
        head=head_commit.commitid,
        pullid=3,
    )
    dbsession.add(base_commit)
    dbsession.add(head_commit)
    dbsession.add(pull)
    dbsession.flush()
    repository = base_commit.repository
    base_full_commit = FullCommit(commit=base_commit, report=small_report)
    head_full_commit = FullCommit(commit=head_commit, report=sample_report)
    return ComparisonProxy(
        Comparison(
            head=head_full_commit,
            project_coverage_base=base_full_commit,
            patch_coverage_base_commitid=base_commit.commitid,
            enriched_pull=EnrichedPull(
                database_pull=pull,
                provider_pull={
                    "author": {"id": "12345", "username": "dana-yaish"},
                    "base": {
                        "branch": "main",
                        "commitid": "ef6edf5ae6643d53a7971fb8823d3f7b2ac65619",
                    },
                    "head": {
                        "branch": "featureA",
                        "commitid": "610ada9fa2bbc49f1a08917da3f73bef2d03709c",
                    },
                    "state": "open",
                    "title": "Create randomcommit.me",
                    "id": "1",
                    "number": "1",
                },
            ),
        )
    )


@pytest.mark.usefixtures("is_not_first_pull")
class TestCommentNotifierIntegration:
    @pytest.fixture(autouse=True)
    def setup(self):
        mock_all_plans_and_tiers()

    @pytest.mark.django_db
    def test_notify(self, sample_comparison, codecov_vcr, mock_configuration, snapshot):
        sample_comparison.context = ComparisonContext(
            all_tests_passed=True, test_results_error=None
        )
        mock_configuration._params["setup"] = {
            "codecov_url": None,
            "codecov_dashboard_url": None,
        }
        comparison = sample_comparison
        notifier = CommentNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"layout": "reach, diff, flags, files, footer"},
            notifier_site_settings=True,
            current_yaml={},
            repository_service=comparison.repository_service,
        )
        result = notifier.notify(comparison)
        assert result.notification_attempted
        assert result.notification_successful
        assert result.explanation is None
        assert snapshot("txt") == "\n".join(result.data_sent["message"])
        assert result.data_sent["commentid"] is None
        assert result.data_sent["pullid"] == 9
        assert result.data_received == {"id": 1699669247}

    @pytest.mark.django_db
    def test_notify_test_results_error(
        self, sample_comparison, codecov_vcr, mock_configuration, snapshot
    ):
        sample_comparison.context = ComparisonContext(
            all_tests_passed=False,
            test_results_error=":x: We are unable to process any of the uploaded JUnit XML files. Please ensure your files are in the right format.",
        )
        mock_configuration._params["setup"] = {
            "codecov_url": None,
            "codecov_dashboard_url": None,
        }
        comparison = sample_comparison
        notifier = CommentNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"layout": "reach, diff, flags, files, footer"},
            notifier_site_settings=True,
            current_yaml={},
            repository_service=comparison.repository_service,
        )
        result = notifier.notify(comparison)
        assert result.notification_attempted
        assert result.notification_successful
        assert result.explanation is None

        assert snapshot("txt") == "\n".join(result.data_sent["message"])
        assert result.data_sent["commentid"] is None
        assert result.data_sent["pullid"] == 9
        assert result.data_received == {"id": 1699669247}

    @pytest.mark.django_db
    def test_notify_upgrade(
        self,
        dbsession,
        sample_comparison_for_upgrade,
        codecov_vcr,
        mock_configuration,
        snapshot,
    ):
        mock_configuration._params["setup"] = {"codecov_dashboard_url": None}
        comparison = sample_comparison_for_upgrade
        notifier = CommentNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"layout": "reach, diff, flags, files, footer"},
            notifier_site_settings=True,
            current_yaml={},
            repository_service=comparison.repository_service,
            decoration_type=Decoration.upgrade,
        )
        result = notifier.notify(comparison)
        assert result.notification_attempted
        assert result.notification_successful
        assert result.explanation is None
        assert snapshot("txt") == "\n".join(result.data_sent["message"])
        assert result.data_sent["commentid"] is None
        assert result.data_sent["pullid"] == 2
        assert result.data_received == {"id": 1361234119}

    @pytest.mark.django_db
    def test_notify_upload_limited(
        self,
        dbsession,
        sample_comparison_for_limited_upload,
        codecov_vcr,
        mock_configuration,
        snapshot,
    ):
        mock_configuration._params["setup"] = {
            "codecov_url": None,
            "codecov_dashboard_url": "https://app.codecov.io",
        }
        comparison = sample_comparison_for_limited_upload
        notifier = CommentNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"layout": "reach, diff, flags, files, footer"},
            notifier_site_settings=True,
            current_yaml={},
            repository_service=comparison.repository_service,
            decoration_type=Decoration.upload_limit,
        )
        result = notifier.notify(comparison)
        assert result.notification_attempted
        assert result.notification_successful
        assert result.explanation is None
        assert snapshot("txt") == "\n".join(result.data_sent["message"])
        assert result.data_sent["commentid"] is None
        assert result.data_sent["pullid"] == 3
        assert result.data_received == {"id": 1111984446}

    @pytest.mark.django_db
    def test_notify_gitlab(
        self, sample_comparison_gitlab, codecov_vcr, mock_configuration, snapshot
    ):
        mock_configuration._params["setup"] = {
            "codecov_url": None,
            "codecov_dashboard_url": None,
        }
        comparison = sample_comparison_gitlab
        notifier = CommentNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={"layout": "reach, diff, flags, files, footer"},
            notifier_site_settings=True,
            current_yaml={},
            repository_service=comparison.repository_service,
        )
        result = notifier.notify(comparison)
        assert result.notification_attempted
        assert result.notification_successful
        assert result.explanation is None
        assert snapshot("txt") == "\n".join(result.data_sent["message"])
        assert result.data_sent["commentid"] is None
        assert result.data_sent["pullid"] == 5
        assert result.data_received == {"id": 1457135397}

    @pytest.mark.django_db
    def test_notify_new_layout(
        self, sample_comparison, codecov_vcr, mock_configuration, snapshot
    ):
        mock_configuration._params["setup"] = {"codecov_dashboard_url": None}
        comparison = sample_comparison
        notifier = CommentNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "newheader, reach, diff, flags, files, newfooter",
                "hide_comment_details": True,
            },
            notifier_site_settings=True,
            current_yaml={},
            repository_service=comparison.repository_service,
        )
        result = notifier.notify(comparison)
        assert result.notification_attempted
        assert result.notification_successful
        assert result.explanation is None
        assert snapshot("txt") == "\n".join(result.data_sent["message"])
        assert result.data_sent["commentid"] is None
        assert result.data_sent["pullid"] == 9
        assert result.data_received == {"id": 1699669290}

    @pytest.mark.django_db
    def test_notify_with_components(
        self, sample_comparison, codecov_vcr, mock_configuration, snapshot
    ):
        mock_configuration._params["setup"] = {"codecov_dashboard_url": None}
        comparison = sample_comparison
        notifier = CommentNotifier(
            repository=comparison.head.commit.repository,
            title="title",
            notifier_yaml_settings={
                "layout": "newheader, reach, diff, flags, files, components, newfooter",
                "hide_comment_details": True,
            },
            notifier_site_settings=True,
            current_yaml={
                "component_management": {
                    "individual_components": [
                        {"component_id": "go_files", "paths": [r".*\.go"]}
                    ]
                }
            },
            repository_service=comparison.repository_service,
        )
        result = notifier.notify(comparison)
        assert result.notification_attempted
        assert result.notification_successful
        assert result.explanation is None
        assert snapshot("txt") == "\n".join(result.data_sent["message"])
        assert result.data_sent["commentid"] is None
        assert result.data_sent["pullid"] == 9
        assert result.data_received == {"id": 1699669323}
