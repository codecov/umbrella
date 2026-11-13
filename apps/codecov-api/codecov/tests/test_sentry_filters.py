from graphql import GraphQLError

from codecov.sentry_filters import before_send


def _hint(exc):
    return {"exc_info": (type(exc), exc, None)}


def test_drops_schema_mismatch_validation_error():
    exc = GraphQLError("Cannot query field 'project' on type 'Query'.")
    assert before_send({"event_id": "1"}, _hint(exc)) is None


def test_keeps_graphql_error_wrapping_real_exception():
    exc = GraphQLError(
        "Cannot query field 'project' on type 'Query'.",
        original_error=ValueError("boom"),
    )
    event = {"event_id": "1"}
    assert before_send(event, _hint(exc)) is event


def test_keeps_graphql_error_raised_during_execution():
    exc = GraphQLError("Cannot query field 'x' on type 'Query'.", path=["x"])
    event = {"event_id": "1"}
    assert before_send(event, _hint(exc)) is event


def test_keeps_other_graphql_errors():
    exc = GraphQLError("Something else went wrong")
    event = {"event_id": "1"}
    assert before_send(event, _hint(exc)) is event


def test_keeps_non_graphql_exceptions():
    exc = ValueError("Cannot query field")
    event = {"event_id": "1"}
    assert before_send(event, _hint(exc)) is event


def test_keeps_events_without_exc_info():
    event = {"event_id": "1"}
    assert before_send(event, {}) is event
    assert before_send(event, None) is event
