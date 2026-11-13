from graphql import GraphQLError

from codecov.sentry_filters import before_send, is_graphql_schema_validation_error


def _hint(exc):
    return {"exc_info": (type(exc), exc, None)}


def test_drops_graphql_schema_validation_error():
    exc = GraphQLError("Cannot query field 'token' on type 'Query'.")
    assert is_graphql_schema_validation_error(exc)
    assert before_send({"event_id": "abc"}, _hint(exc)) is None


def test_keeps_graphql_error_with_original_error():
    exc = GraphQLError(
        "Cannot query field 'token' on type 'Query'.",
        original_error=ValueError("boom"),
    )
    event = {"event_id": "abc"}
    assert before_send(event, _hint(exc)) is event


def test_keeps_other_graphql_errors():
    exc = GraphQLError("Syntax Error: Unexpected Name 'foo'.")
    event = {"event_id": "abc"}
    assert before_send(event, _hint(exc)) is event


def test_keeps_non_graphql_exceptions_and_messages():
    event = {"event_id": "abc"}
    assert before_send(event, _hint(ValueError("boom"))) is event
    assert before_send(event, {}) is event
    assert before_send(event, None) is event
