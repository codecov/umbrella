from graphql import GraphQLError

from codecov.sentry_filters import before_send, is_graphql_bad_query_event


def _event(mechanism_type="ariadne"):
    return {
        "exception": {
            "values": [
                {
                    "type": "GraphQLError",
                    "value": "Cannot query field 'uploads' on type 'Query'.",
                    "mechanism": {"type": mechanism_type, "handled": False},
                }
            ]
        }
    }


def _hint(exc):
    return {"exc_info": (type(exc), exc, None)}


def test_drops_ariadne_bad_query_event():
    exc = GraphQLError("Cannot query field 'uploads' on type 'Query'.")
    assert is_graphql_bad_query_event(_event(), _hint(exc))
    assert before_send(_event(), _hint(exc)) is None


def test_keeps_resolver_errors():
    original = ValueError("boom")
    exc = GraphQLError("boom", original_error=original)
    event = _event()
    assert before_send(event, _hint(exc)) is event


def test_keeps_other_graphql_errors():
    exc = GraphQLError("Variable '$x' of required type 'Int!' was not provided.")
    event = _event()
    assert before_send(event, _hint(exc)) is event


def test_keeps_bad_query_not_from_ariadne_integration():
    exc = GraphQLError("Cannot query field 'uploads' on type 'Query'.")
    event = _event(mechanism_type="generic")
    assert before_send(event, _hint(exc)) is event


def test_keeps_non_graphql_events():
    event = {"message": "hello"}
    assert before_send(event, {}) is event
    exc = ValueError("Cannot query field")
    assert before_send(_event(), _hint(exc)) is not None
