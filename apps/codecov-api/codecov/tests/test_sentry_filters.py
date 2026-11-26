from graphql import GraphQLError

from codecov.sentry_filters import before_send


def _event(mechanism_type="ariadne"):
    return {
        "exception": {
            "values": [
                {
                    "type": "GraphQLError",
                    "mechanism": {"type": mechanism_type, "handled": False},
                }
            ]
        }
    }


def test_drops_ariadne_validation_error():
    exc = GraphQLError("Cannot query field 'item' on type 'Query'.")
    hint = {"exc_info": (GraphQLError, exc, None)}
    assert before_send(_event(), hint) is None


def test_keeps_ariadne_resolver_error():
    exc = GraphQLError("boom", original_error=ValueError("boom"))
    hint = {"exc_info": (GraphQLError, exc, None)}
    event = _event()
    assert before_send(event, hint) is event


def test_keeps_non_ariadne_graphql_error():
    exc = GraphQLError("Cannot query field 'item' on type 'Query'.")
    hint = {"exc_info": (GraphQLError, exc, None)}
    event = _event(mechanism_type="django")
    assert before_send(event, hint) is event


def test_keeps_other_exceptions():
    exc = ValueError("boom")
    hint = {"exc_info": (ValueError, exc, None)}
    event = _event()
    assert before_send(event, hint) is event


def test_keeps_events_without_exc_info():
    event = {"message": "hello"}
    assert before_send(event, {}) is event
