from graphql import GraphQLError

from codecov.sentry_filters import before_send


def _hint(exc):
    return {"exc_info": (type(exc), exc, None)}


def test_drops_graphql_bad_query_validation_error():
    exc = GraphQLError("Cannot query field 'messages' on type 'Query'.")
    assert before_send({"event_id": "1"}, _hint(exc)) is None


def test_keeps_graphql_error_with_original_error():
    original = ValueError("Cannot query field boom")
    exc = GraphQLError("Cannot query field boom", original_error=original)
    event = {"event_id": "1"}
    assert before_send(event, _hint(exc)) is event


def test_keeps_other_graphql_errors():
    exc = GraphQLError("Variable '$x' of required type 'String!' was not provided.")
    event = {"event_id": "1"}
    assert before_send(event, _hint(exc)) is event


def test_keeps_non_graphql_exceptions_and_messages():
    event = {"event_id": "1"}
    assert before_send(event, _hint(ValueError("Cannot query field"))) is event
    assert before_send(event, {}) is event
