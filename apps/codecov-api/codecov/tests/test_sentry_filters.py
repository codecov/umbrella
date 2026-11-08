from graphql import GraphQLError

from codecov.sentry_filters import before_send


def _event(mechanism_type="ariadne"):
    return {
        "exception": {
            "values": [{"type": "GraphQLError", "mechanism": {"type": mechanism_type}}]
        }
    }


def _hint(exc):
    return {"exc_info": (type(exc), exc, None)}


def test_drops_ariadne_validation_error():
    exc = GraphQLError("Cannot query field 'post' on type 'Query'.")
    assert before_send(_event(), _hint(exc)) is None


def test_keeps_ariadne_error_with_original_error():
    exc = GraphQLError("boom", original_error=ValueError("boom"))
    event = _event()
    assert before_send(event, _hint(exc)) is event


def test_keeps_non_ariadne_graphql_error():
    exc = GraphQLError("Cannot query field 'post' on type 'Query'.")
    event = _event(mechanism_type="django")
    assert before_send(event, _hint(exc)) is event


def test_keeps_other_exceptions():
    event = _event()
    assert before_send(event, _hint(ValueError("x"))) is event


def test_keeps_events_without_exc_info():
    event = {"message": "hi"}
    assert before_send(event, {}) is event
