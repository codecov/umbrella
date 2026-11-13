from graphql import GraphQLError

from codecov.sentry_filters import before_send


def _event(mechanism_type="ariadne"):
    return {"exception": {"values": [{"mechanism": {"type": mechanism_type}}]}}


def _hint(exc):
    return {"exc_info": (type(exc), exc, None)}


def test_drops_ariadne_client_request_error():
    exc = GraphQLError("Field 'x' is not defined by type 'PathContentsFilters'.")
    assert before_send(_event(), _hint(exc)) is None


def test_drops_ariadne_variable_coercion_error():
    inner = GraphQLError("Field 'x' is not defined by type 'PathContentsFilters'.")
    exc = GraphQLError("Variable '$filters' got invalid value", original_error=inner)
    assert before_send(_event(), _hint(exc)) is None


def test_keeps_graphql_error_wrapping_real_exception():
    exc = GraphQLError("boom", original_error=ValueError("boom"))
    event = _event()
    assert before_send(event, _hint(exc)) is event


def test_keeps_non_ariadne_graphql_error():
    exc = GraphQLError("bad")
    event = _event("generic")
    assert before_send(event, _hint(exc)) is event


def test_keeps_other_exceptions_and_events_without_hint():
    event = _event()
    assert before_send(event, _hint(ValueError("x"))) is event
    assert before_send(event, {}) is event
