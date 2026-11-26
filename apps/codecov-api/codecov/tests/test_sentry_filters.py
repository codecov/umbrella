from graphql import GraphQLError

from codecov.sentry_filters import before_send, is_client_graphql_validation_error


def _hint(exc):
    return {"exc_info": (type(exc), exc, None)}


def test_drops_cannot_query_field_validation_error():
    exc = GraphQLError("Cannot query field 'events' on type 'Query'.")
    assert is_client_graphql_validation_error(exc)
    assert before_send({"event_id": "1"}, _hint(exc)) is None


def test_keeps_graphql_error_wrapping_original_error():
    exc = GraphQLError(
        "Cannot query field 'events' on type 'Query'.",
        original_error=ValueError("boom"),
    )
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
