from graphql import GraphQLError

from codecov.sentry_filters import before_send


def _event(mechanism):
    return {"exception": {"values": [{"mechanism": {"type": mechanism}}]}}


def test_drops_ariadne_validation_error():
    exc = GraphQLError("Cannot query field 'person' on type 'Query'.")
    hint = {"exc_info": (GraphQLError, exc, None)}
    assert before_send(_event("ariadne"), hint) is None


def test_keeps_ariadne_error_with_original_error():
    exc = GraphQLError("boom", original_error=ValueError("boom"))
    hint = {"exc_info": (GraphQLError, exc, None)}
    event = _event("ariadne")
    assert before_send(event, hint) is event


def test_keeps_execution_error_with_path():
    exc = GraphQLError("boom", path=["failing"])
    hint = {"exc_info": (GraphQLError, exc, None)}
    event = _event("ariadne")
    assert before_send(event, hint) is event


def test_keeps_non_ariadne_events():
    exc = GraphQLError("Cannot query field 'person' on type 'Query'.")
    hint = {"exc_info": (GraphQLError, exc, None)}
    event = _event("django")
    assert before_send(event, hint) is event


def test_keeps_events_without_exc_info():
    event = {"message": "hi"}
    assert before_send(event, {}) is event
