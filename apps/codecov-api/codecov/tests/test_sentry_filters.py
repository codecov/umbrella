from graphql import GraphQLError

from codecov.sentry_filters import before_send


def _hint(exc):
    return {"exc_info": (type(exc), exc, None)}


def test_drops_graphql_validation_errors():
    event = {"event_id": "abc"}
    exc = GraphQLError("Cannot query field 'tasks' on type 'Query'.")
    assert before_send(event, _hint(exc)) is None


def test_keeps_graphql_errors_wrapping_resolver_exceptions():
    event = {"event_id": "abc"}
    exc = GraphQLError("boom", original_error=ValueError("boom"))
    assert before_send(event, _hint(exc)) is event


def test_keeps_other_exceptions():
    event = {"event_id": "abc"}
    assert before_send(event, _hint(ValueError("boom"))) is event


def test_keeps_events_without_exc_info():
    event = {"event_id": "abc"}
    assert before_send(event, {}) is event
    assert before_send(event, None) is event
