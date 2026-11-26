from graphql import GraphQLError

from codecov.sentry_filters import before_send


def test_drops_graphql_validation_errors():
    exc = GraphQLError("Cannot query field 'sessions' on type 'Query'.")
    assert before_send({"id": 1}, {"exc_info": (type(exc), exc, None)}) is None


def test_keeps_graphql_resolver_errors():
    original = ValueError("boom")
    exc = GraphQLError("boom", original_error=original)
    event = {"id": 1}
    assert before_send(event, {"exc_info": (type(exc), exc, None)}) is event


def test_keeps_other_exceptions_and_messages():
    exc = ValueError("boom")
    event = {"id": 1}
    assert before_send(event, {"exc_info": (type(exc), exc, None)}) is event
    assert before_send(event, {}) is event
