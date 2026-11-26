from graphql import GraphQLError

from codecov.sentry_filters import before_send, is_graphql_client_validation_error


def test_drops_cannot_query_field_validation_error():
    exc = GraphQLError("Cannot query field 'file' on type 'Query'.")
    assert is_graphql_client_validation_error(exc)
    assert before_send({"event": 1}, {"exc_info": (type(exc), exc, None)}) is None


def test_keeps_resolver_graphql_error():
    original = ValueError("boom")
    exc = GraphQLError("Cannot query field 'x'", original_error=original)
    event = {"event": 1}
    assert not is_graphql_client_validation_error(exc)
    assert before_send(event, {"exc_info": (type(exc), exc, None)}) is event


def test_keeps_other_graphql_errors_and_exceptions():
    event = {"event": 1}
    exc = GraphQLError("Some other validation error")
    assert before_send(event, {"exc_info": (type(exc), exc, None)}) is event
    err = ValueError("x")
    assert before_send(event, {"exc_info": (type(err), err, None)}) is event
    assert before_send(event, {}) is event
