from unittest.mock import Mock

import pytest

from helpers.notifier import NotifierResult
from services.comment import post_comment
from shared.torngit.exceptions import TorngitClientError


@pytest.fixture
def mock_commit(mocker):
    mock_commit = mocker.AsyncMock()
    mock_commit.commitid = "abc123"
    mock_commit.repository = Mock()
    return mock_commit


@pytest.fixture
def mock_fetch_pull(mocker):
    mock_fetch_pull = mocker.AsyncMock()
    mock_fetch_pull.database_pull.pullid = 42
    mock_fetch_pull.database_pull.commentid = None
    mocker.patch(
        "services.comment.fetch_and_update_pull_request_information_from_commit",
        return_value=mock_fetch_pull,
    )
    return mock_fetch_pull


@pytest.fixture
def mock_repo_service(mocker):
    mock_post_comment = mocker.AsyncMock(return_value={"id": 123})
    mock_edit_comment = mocker.AsyncMock()
    mock_repo_service = Mock(
        post_comment=mock_post_comment, edit_comment=mock_edit_comment
    )
    mocker.patch(
        "services.comment.get_repo_provider_service", return_value=mock_repo_service
    )
    return mock_repo_service


def test_post_comment_success_new_comment(
    mocker, mock_repo_service, mock_fetch_pull, mock_commit
):
    result = post_comment(mock_commit, "Test message")

    assert result == NotifierResult.COMMENT_POSTED
    mock_repo_service.post_comment.assert_called_once_with(42, "Test message")
    assert mock_fetch_pull.database_pull.commentid == 123


def test_post_comment_success_edit_comment(
    mocker, mock_repo_service, mock_fetch_pull, mock_commit
):
    mock_fetch_pull.database_pull.commentid = 456

    result = post_comment(mock_commit, "Updated message")

    assert result == NotifierResult.COMMENT_POSTED
    mock_repo_service.edit_comment.assert_called_once_with(42, 456, "Updated message")


def test_post_comment_no_pull(mocker, mock_repo_service, mock_commit):
    mocker.patch(
        "services.comment.fetch_and_update_pull_request_information_from_commit",
        return_value=None,
    )

    result = post_comment(mock_commit, "Test message")

    assert result == NotifierResult.NO_PULL


def test_post_comment_torngit_error_new_comment(
    mocker, mock_repo_service, mock_fetch_pull, mock_commit
):
    mock_repo_service.post_comment.side_effect = TorngitClientError()

    result = post_comment(mock_commit, "Test message")

    assert result == NotifierResult.TORNGIT_ERROR


def test_post_comment_torngit_error_edit_comment(
    mocker, mock_repo_service, mock_fetch_pull, mock_commit
):
    mock_fetch_pull.database_pull.commentid = 456

    mock_repo_service.edit_comment.side_effect = TorngitClientError()

    result = post_comment(mock_commit, "Updated message")

    assert result == NotifierResult.TORNGIT_ERROR
