from graphql import GraphQLError

from codecov.sentry_filters import before_send


def test_drops_schema_validation_error():
    exc = GraphQLError("Cannot query field 'session' on type 'Query'.")
    assert before_send({"a": 1}, {"exc_info": (type(exc), exc, None)}) is None


def test_keeps_graphql_error_with_original_error():
    exc = GraphQLError(
        "Cannot query field 'x' on type 'Query'.", original_error=ValueError("boom")
    )
    event = {"a": 1}
    assert before_send(event, {"exc_info": (type(exc), exc, None)}) is event


def test_keeps_other_graphql_errors():
    exc = GraphQLError("Something else")
    event = {"a": 1}
    assert before_send(event, {"exc_info": (type(exc), exc, None)}) is event


def test_keeps_events_without_exc_info():
    event = {"a": 1}
    assert before_send(event, {}) is event
