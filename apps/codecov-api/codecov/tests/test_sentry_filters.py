from graphql import GraphQLError

from codecov.sentry_filters import before_send


def _hint(exc):
    return {"exc_info": (type(exc), exc, None)}


def test_drops_graphql_validation_error():
    exc = GraphQLError("Cannot query field 'profiles' on type 'Query'.")
    assert before_send({"event_id": "1"}, _hint(exc)) is None


def test_keeps_graphql_error_from_resolver():
    exc = GraphQLError("boom", original_error=ValueError("boom"))
    event = {"event_id": "1"}
    assert before_send(event, _hint(exc)) is event


def test_keeps_other_exceptions():
    event = {"event_id": "1"}
    assert before_send(event, _hint(ValueError("x"))) is event


def test_keeps_events_without_exception():
    event = {"event_id": "1"}
    assert before_send(event, {}) is event
