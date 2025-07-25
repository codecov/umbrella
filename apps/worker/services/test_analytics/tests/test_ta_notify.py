from database.tests.factories.core import CommitFactory
from services.test_analytics.ta_notify import (
    build_test_results_message,
)
from services.test_analytics.ta_types import (
    ErrorPayload,
    FlakeInfo,
    TACommentInDepthInfo,
    TestResultsNotificationFailure,
    TestResultsNotificationPayload,
)


def test_build_message_with_error_only(snapshot):
    mock_commit = CommitFactory()
    payload = TestResultsNotificationPayload(failed=0, passed=5, skipped=2, info=None)
    error = ErrorPayload(error_code="file_not_in_storage", error_message=None)

    result = build_test_results_message(payload, error, mock_commit)

    assert snapshot("txt") == result


def test_build_message_with_failures(snapshot, mocker):
    mock_commit = CommitFactory()

    failure = TestResultsNotificationFailure(
        failure_message="Test failed with assertion error",
        display_name="test_example",
        envs=["python3.9"],
        test_id="test123",
        duration_seconds=1.5,
        build_url="https://example.com/build/123",
    )

    info = TACommentInDepthInfo(failures=[failure], flaky_tests={})

    payload = TestResultsNotificationPayload(failed=1, passed=4, skipped=0, info=info)

    mock_url = mocker.patch("services.test_analytics.ta_notify.get_test_analytics_url")
    mock_url.return_value = "https://codecov.io/test-analytics"

    result = build_test_results_message(payload, None, mock_commit)

    assert snapshot("txt") == result


def test_build_message_with_flaky_tests(snapshot, mocker):
    mock_commit = CommitFactory()

    failure = TestResultsNotificationFailure(
        failure_message="Flaky test failure",
        display_name="test_flaky",
        envs=["python3.9"],
        test_id="flaky123",
        duration_seconds=2.0,
        build_url=None,
    )

    flake_info = FlakeInfo(failed=2, count=10)

    info = TACommentInDepthInfo(
        failures=[failure], flaky_tests={"flaky123": flake_info}
    )

    payload = TestResultsNotificationPayload(failed=1, passed=9, skipped=0, info=info)

    mock_url = mocker.patch("services.test_analytics.ta_notify.get_test_analytics_url")
    mock_url.return_value = "https://codecov.io/test-analytics"

    result = build_test_results_message(payload, None, mock_commit)

    assert snapshot("txt") == result


def test_build_message_with_error_and_payload(snapshot, mocker):
    mock_commit = CommitFactory()

    failure = TestResultsNotificationFailure(
        failure_message="Test failed",
        display_name="test_with_error",
        envs=["python3.9"],
        test_id="test456",
        duration_seconds=0.5,
        build_url=None,
    )

    info = TACommentInDepthInfo(failures=[failure], flaky_tests={})

    payload = TestResultsNotificationPayload(failed=1, passed=0, skipped=0, info=info)

    error = ErrorPayload(
        error_code="warning", error_message="Parser warning: some issue"
    )

    mock_url = mocker.patch("services.test_analytics.ta_notify.get_test_analytics_url")
    mock_url.return_value = "https://codecov.io/test-analytics"

    result = build_test_results_message(payload, error, mock_commit)

    assert snapshot("txt") == result
