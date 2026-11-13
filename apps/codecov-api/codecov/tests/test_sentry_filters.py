from graphql import GraphQLError

from codecov.sentry_filters import before_send


def test_drops_graphql_validation_errors():
    exc = GraphQLError("Cannot query field 'report' on type 'Query'.")
    event = {"event_id": "x"}
    assert before_send(event, {"exc_info": (type(exc), exc, None)}) is None


def test_keeps_graphql_errors_with_original_error():
    exc = GraphQLError("boom", original_error=ValueError("boom"))
    event = {"event_id": "x"}
    assert before_send(event, {"exc_info": (type(exc), exc, None)}) is event


def test_keeps_other_events():
    exc = ValueError("boom")
    event = {"event_id": "x"}
    assert before_send(event, {"exc_info": (type(exc), exc, None)}) is event
    assert before_send(event, {}) is event
