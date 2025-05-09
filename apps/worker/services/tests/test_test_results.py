from unittest import mock

import pytest

from database.tests.factories import (
    CommitFactory,
    OwnerFactory,
    RepositoryFactory,
)
from helpers.notifier import NotifierResult
from services.test_results import (
    ErrorPayload,
    FlakeInfo,
    TACommentInDepthInfo,
    TestResultsNotificationFailure,
    TestResultsNotificationPayload,
    TestResultsNotifier,
    generate_failure_info,
    generate_flags_hash,
    generate_test_id,
    should_do_flaky_detection,
)
from services.yaml import UserYaml
from shared.plan.constants import DEFAULT_FREE_PLAN
from shared.torngit.exceptions import TorngitClientError
from tests.helpers import mock_all_plans_and_tiers


def mock_repo_service():
    repo_service = mock.Mock(
        post_comment=mock.AsyncMock(),
        edit_comment=mock.AsyncMock(),
    )
    return repo_service


def test_send_to_provider():
    tn = TestResultsNotifier(CommitFactory(), None)
    tn._pull = mock.Mock()
    tn._pull.database_pull.commentid = None
    tn._repo_service = mock_repo_service()
    m = {"id": 1}
    tn._repo_service.post_comment.return_value = m

    res = tn.send_to_provider(tn._pull, "hello world")

    assert res == True

    tn._repo_service.post_comment.assert_called_with(
        tn._pull.database_pull.pullid, "hello world"
    )
    assert tn._pull.database_pull.commentid == 1


def test_send_to_provider_edit():
    tn = TestResultsNotifier(CommitFactory(), None)
    tn._pull = mock.Mock()
    tn._pull.database_pull.commentid = 1
    tn._repo_service = mock_repo_service()
    m = {"id": 1}
    tn._repo_service.edit_comment.return_value = m

    res = tn.send_to_provider(tn._pull, "hello world")

    assert res == True
    tn._repo_service.edit_comment.assert_called_with(
        tn._pull.database_pull.pullid, 1, "hello world"
    )


def test_send_to_provider_fail():
    tn = TestResultsNotifier(CommitFactory(), None)
    tn._pull = mock.Mock()
    tn._pull.database_pull.commentid = 1
    tn._repo_service = mock_repo_service()
    tn._repo_service.edit_comment.side_effect = TorngitClientError

    res = tn.send_to_provider(tn._pull, "hello world")

    assert res == False


def test_generate_failure_info(snapshot):
    flags_hash = generate_flags_hash([])
    test_id = generate_test_id(1, "testsuite", "testname", flags_hash)
    fail = TestResultsNotificationFailure(
        "hello world",
        "testname",
        [],
        test_id,
        1.0,
        "https://example.com/build_url",
    )

    res = generate_failure_info(fail)

    assert snapshot("txt") == res


def test_build_message(snapshot):
    flags_hash = generate_flags_hash([])
    test_id = generate_test_id(1, "testsuite", "testname", flags_hash)
    fail = TestResultsNotificationFailure(
        "hello world",
        "testname",
        [],
        test_id,
        1.0,
        "https://example.com/build_url",
    )
    info = TACommentInDepthInfo(failures=[fail], flaky_tests={})
    payload = TestResultsNotificationPayload(1, 2, 3, info)
    commit = CommitFactory(
        branch="thing/thing",
        repository__owner__username="username",
        repository__owner__service="github",
        repository__name="name",
    )
    tn = TestResultsNotifier(commit, None, None, None, payload)
    res = tn.build_message()

    assert snapshot("txt") == res


def test_build_message_with_flake(snapshot):
    flags_hash = generate_flags_hash([])
    test_id = generate_test_id(1, "testsuite", "testname", flags_hash)
    fail = TestResultsNotificationFailure(
        "hello world",
        "testname",
        [],
        test_id,
        1.0,
        "https://example.com/build_url",
    )
    flaky_test = FlakeInfo(1, 3)
    info = TACommentInDepthInfo(failures=[fail], flaky_tests={test_id: flaky_test})
    payload = TestResultsNotificationPayload(1, 2, 3, info)
    commit = CommitFactory(
        branch="test_branch",
        repository__owner__username="username",
        repository__owner__service="github",
        repository__name="name",
    )
    tn = TestResultsNotifier(commit, None, None, None, payload)
    res = tn.build_message()

    assert snapshot("txt") == res


def test_notify(mocker):
    mocker.patch("helpers.notifier.get_repo_provider_service", return_value=mock.Mock())
    mocker.patch(
        "helpers.notifier.fetch_and_update_pull_request_information_from_commit",
        return_value=mock.Mock(),
    )
    tn = TestResultsNotifier(CommitFactory(), None, _pull=mock.Mock())
    tn.build_message = mock.Mock()
    tn.send_to_provider = mock.Mock()

    notification_result = tn.notify()

    assert notification_result == NotifierResult.COMMENT_POSTED


def test_notify_fail_torngit_error(
    mocker,
):
    mocker.patch("helpers.notifier.get_repo_provider_service", return_value=mock.Mock())
    mocker.patch(
        "helpers.notifier.fetch_and_update_pull_request_information_from_commit",
        return_value=mock.Mock(),
    )
    tn = TestResultsNotifier(CommitFactory(), None, _pull=mock.Mock())
    tn.build_message = mock.Mock()
    tn.send_to_provider = mock.Mock(return_value=False)

    notification_result = tn.notify()

    assert notification_result == NotifierResult.TORNGIT_ERROR


@pytest.mark.django_db
@pytest.mark.parametrize(
    "config,private,plan,ex_result",
    [
        (False, False, "users-inappm", False),
        (True, True, DEFAULT_FREE_PLAN, False),
        (True, False, DEFAULT_FREE_PLAN, True),
        (True, False, "users-inappm", True),
        (True, True, "users-inappm", True),
    ],
)
def test_should_do_flake_detection(dbsession, mocker, config, private, plan, ex_result):
    mock_all_plans_and_tiers()
    owner = OwnerFactory(plan=plan)
    repo = RepositoryFactory(private=private, owner=owner)
    dbsession.add(repo)
    dbsession.flush()

    yaml = {"test_analytics": {"flake_detection": config}}

    result = should_do_flaky_detection(repo, UserYaml.from_dict(yaml))

    assert result == ex_result


def test_specific_error_message(mocker, snapshot):
    mock_repo_service = mock.AsyncMock()
    mocker.patch(
        "helpers.notifier.get_repo_provider_service", return_value=mock_repo_service
    )
    mocker.patch(
        "helpers.notifier.fetch_and_update_pull_request_information_from_commit",
        return_value=mock.AsyncMock(),
    )

    error = ErrorPayload(
        "unsupported_file_format",
        "Error parsing JUnit XML in test.xml at 4:32: ParserError: No name found",
    )
    tn = TestResultsNotifier(CommitFactory(), None, error=error)
    result = tn.error_comment()

    assert result == (True, "comment_posted")

    args = mock_repo_service.edit_comment.call_args[0]
    db_pull = tn._pull.database_pull
    assert args[0] == db_pull.pullid
    assert args[1] == db_pull.commentid
    assert snapshot("txt") == args[2]


def test_specific_error_message_no_error(mocker, snapshot):
    mock_repo_service = mock.AsyncMock()
    mocker.patch(
        "helpers.notifier.get_repo_provider_service", return_value=mock_repo_service
    )
    mocker.patch(
        "helpers.notifier.fetch_and_update_pull_request_information_from_commit",
        return_value=mock.AsyncMock(),
    )

    tn = TestResultsNotifier(CommitFactory(), None)
    result = tn.error_comment()

    assert result == (True, "comment_posted")

    args = mock_repo_service.edit_comment.call_args[0]
    db_pull = tn._pull.database_pull
    assert args[0] == db_pull.pullid
    assert args[1] == db_pull.commentid
    assert snapshot("txt") == args[2]
