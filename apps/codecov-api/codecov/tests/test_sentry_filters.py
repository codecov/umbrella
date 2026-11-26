import sys

from graphql import GraphQLError, build_schema, parse, validate

from codecov.sentry_filters import before_send, is_graphql_client_query_error_event


def _hint_for(exc):
    return {"exc_info": (type(exc), exc, None)}


def _validation_error():
    schema = build_schema("type Query { stuff: String }")
    errors = validate(schema, parse("{ roles { id } }"))
    assert errors
    return errors[0]


def test_drops_graphql_validation_error():
    event = {"message": "x"}
    assert before_send(event, _hint_for(_validation_error())) is None


def test_keeps_graphql_error_wrapping_exception():
    original = ValueError("boom")
    err = GraphQLError("boom", original_error=original, path=["failing"])
    event = {"message": "x"}
    assert before_send(event, _hint_for(err)) is event


def test_keeps_graphql_error_with_execution_path():
    err = GraphQLError("resolver failed", path=["failing"])
    assert not is_graphql_client_query_error_event(_hint_for(err))


def test_keeps_non_graphql_events():
    event = {"message": "x"}
    assert before_send(event, _hint_for(ValueError("x"))) is event
    assert before_send(event, {}) is event
    assert before_send(event, {"exc_info": sys.exc_info()}) is event
