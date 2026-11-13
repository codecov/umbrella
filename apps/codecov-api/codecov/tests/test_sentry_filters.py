from graphql import GraphQLError

from codecov.sentry_filters import before_send, is_graphql_invalid_field_error


def _hint(exc):
    return {"exc_info": (type(exc), exc, None)}


def test_drops_invalid_field_validation_error():
    exc = GraphQLError("Cannot query field 'certificates' on type 'Query'.")
    assert is_graphql_invalid_field_error(exc)
    assert before_send({"event_id": "1"}, _hint(exc)) is None


def test_keeps_graphql_error_wrapping_resolver_exception():
    original = ValueError("boom")
    exc = GraphQLError(
        "Cannot query field 'x' on type 'Query'.", original_error=original
    )
    event = {"event_id": "1"}
    assert before_send(event, _hint(exc)) is event


def test_keeps_other_graphql_errors():
    exc = GraphQLError("Variable '$id' of required type 'ID!' was not provided.")
    event = {"event_id": "1"}
    assert before_send(event, _hint(exc)) is event


def test_keeps_non_graphql_errors_and_events_without_exc_info():
    event = {"event_id": "1"}
    assert before_send(event, _hint(RuntimeError("Cannot query field"))) is event
    assert before_send(event, {}) is event
    assert before_send(event, None) is event
