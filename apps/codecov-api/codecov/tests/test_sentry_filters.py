from graphql import GraphQLError

from codecov.sentry_filters import before_send


def _event(mechanism="ariadne"):
    return {"exception": {"values": [{"mechanism": {"type": mechanism}}]}}


def _hint(exc):
    return {"exc_info": (type(exc), exc, None)}


def test_drops_ariadne_validation_error():
    exc = GraphQLError("Cannot query field 'filesChanged' on type 'Pull'.")
    assert before_send(_event(), _hint(exc)) is None


def test_keeps_non_ariadne_mechanism():
    exc = GraphQLError("Cannot query field 'filesChanged' on type 'Pull'.")
    event = _event("django")
    assert before_send(event, _hint(exc)) is event


def test_keeps_resolver_errors():
    exc = GraphQLError("Cannot query field 'x'", original_error=ValueError("boom"))
    event = _event()
    assert before_send(event, _hint(exc)) is event


def test_keeps_other_graphql_errors():
    exc = GraphQLError("Something else went wrong")
    event = _event()
    assert before_send(event, _hint(exc)) is event


def test_keeps_events_without_exc_info():
    event = {"message": "hi"}
    assert before_send(event, {}) is event
