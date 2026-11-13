from graphql import GraphQLError

from codecov.sentry_filters import before_send, is_client_query_error


def _event(mechanism="ariadne"):
    return {"exception": {"values": [{"mechanism": {"type": mechanism}}]}}


def _hint(exc):
    return {"exc_info": (type(exc), exc, None)}


def test_is_client_query_error():
    assert is_client_query_error(GraphQLError("Cannot query field 'x' on type 'Y'."))
    assert not is_client_query_error(
        GraphQLError("boom", original_error=ValueError("boom"))
    )
    assert not is_client_query_error(ValueError("boom"))


def test_before_send_drops_ariadne_validation_errors():
    exc = GraphQLError("Cannot query field 'files' on type 'Comparison'.")
    assert before_send(_event(), _hint(exc)) is None


def test_before_send_keeps_resolver_errors():
    exc = GraphQLError("boom", original_error=ValueError("boom"))
    event = _event()
    assert before_send(event, _hint(exc)) is event


def test_before_send_keeps_other_mechanisms():
    exc = GraphQLError("Cannot query field 'x' on type 'Y'.")
    event = _event("django")
    assert before_send(event, _hint(exc)) is event


def test_before_send_keeps_events_without_exc_info():
    event = {"message": "hello"}
    assert before_send(event, {}) is event
