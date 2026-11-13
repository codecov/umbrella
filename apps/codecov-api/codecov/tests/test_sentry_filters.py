from graphql import GraphQLError

from codecov.sentry_filters import before_send


def _hint(exc):
    return {"exc_info": (type(exc), exc, None)}


def test_drops_unknown_field_validation_error():
    exc = GraphQLError("Cannot query field 'people' on type 'Query'.")
    assert before_send({"id": 1}, _hint(exc)) is None


def test_keeps_resolver_errors():
    exc = GraphQLError("Cannot query field 'x'", original_error=ValueError("boom"))
    event = {"id": 1}
    assert before_send(event, _hint(exc)) is event


def test_keeps_other_graphql_errors():
    exc = GraphQLError("Something else")
    event = {"id": 1}
    assert before_send(event, _hint(exc)) is event


def test_keeps_non_graphql_errors():
    event = {"id": 1}
    assert before_send(event, _hint(ValueError("x"))) is event
    assert before_send(event, {}) is event
