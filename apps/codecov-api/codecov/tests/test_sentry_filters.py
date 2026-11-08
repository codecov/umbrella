from graphql import GraphQLError

from codecov.sentry_filters import before_send, is_unknown_field_query_error


def _hint(exc):
    return {"exc_info": (type(exc), exc, None)}


def test_drops_unknown_field_validation_error():
    err = GraphQLError("Cannot query field 'reports' on type 'Query'.")
    assert is_unknown_field_query_error(err)
    assert before_send({"event_id": "x"}, _hint(err)) is None


def test_keeps_resolver_errors():
    err = GraphQLError("Cannot query field oops", original_error=ValueError("boom"))
    event = {"event_id": "x"}
    assert before_send(event, _hint(err)) is event


def test_keeps_other_graphql_errors():
    err = GraphQLError("Query Cost Exceeded")
    event = {"event_id": "x"}
    assert before_send(event, _hint(err)) is event


def test_keeps_non_graphql_and_hintless_events():
    event = {"event_id": "x"}
    assert before_send(event, _hint(ValueError("x"))) is event
    assert before_send(event, {}) is event
