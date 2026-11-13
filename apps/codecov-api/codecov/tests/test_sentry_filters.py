from graphql import GraphQLError

from codecov.sentry_filters import before_send


def _hint(exc):
    return {"exc_info": (type(exc), exc, None)}


def test_drops_unknown_field_validation_error():
    exc = GraphQLError("Cannot query field 'role' on type 'Query'.")
    assert before_send({"event_id": "x"}, _hint(exc)) is None


def test_keeps_graphql_error_with_original_error():
    exc = GraphQLError(
        "Cannot query field 'role' on type 'Query'.", original_error=ValueError()
    )
    event = {"event_id": "x"}
    assert before_send(event, _hint(exc)) is event


def test_keeps_other_graphql_errors():
    exc = GraphQLError("Something else broke")
    event = {"event_id": "x"}
    assert before_send(event, _hint(exc)) is event


def test_keeps_non_graphql_errors_and_events_without_hint():
    event = {"event_id": "x"}
    assert before_send(event, _hint(ValueError("boom"))) is event
    assert before_send(event, {}) is event
