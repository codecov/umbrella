from graphql import GraphQLError

from codecov.sentry_filters import before_send


def _hint(exc):
    return {"exc_info": (type(exc), exc, None)}


def test_drops_bad_query_validation_error():
    exc = GraphQLError("Cannot query field 'member' on type 'Query'.")
    assert before_send({"a": 1}, _hint(exc)) is None


def test_keeps_graphql_error_with_original_error():
    exc = GraphQLError(
        "Cannot query field 'x' on type 'Query'.", original_error=ValueError("boom")
    )
    event = {"a": 1}
    assert before_send(event, _hint(exc)) is event


def test_keeps_other_graphql_errors():
    exc = GraphQLError("Something else went wrong")
    event = {"a": 1}
    assert before_send(event, _hint(exc)) is event


def test_keeps_non_graphql_errors_and_events_without_exc_info():
    event = {"a": 1}
    assert before_send(event, _hint(ValueError("Cannot query field"))) is event
    assert before_send(event, {}) is event
