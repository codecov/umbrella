from graphql import GraphQLError

from codecov.sentry_filters import sentry_before_send


def make_event(mechanism_type: str = "ariadne") -> dict:
    return {
        "exception": {
            "values": [{"type": "GraphQLError", "mechanism": {"type": mechanism_type}}]
        }
    }


def hint_for(error: BaseException) -> dict:
    return {"exc_info": (type(error), error, None)}


def test_drops_ariadne_schema_validation_error():
    error = GraphQLError("Cannot query field 'comment' on type 'Query'.")
    assert sentry_before_send(make_event(), hint_for(error)) is None


def test_keeps_ariadne_error_with_original_error():
    error = GraphQLError(
        "Cannot query field 'comment' on type 'Query'.",
        original_error=ValueError("boom"),
    )
    event = make_event()
    assert sentry_before_send(event, hint_for(error)) is event


def test_keeps_other_ariadne_graphql_errors():
    error = GraphQLError("Syntax Error: Unexpected Name 'foo'.")
    event = make_event()
    assert sentry_before_send(event, hint_for(error)) is event


def test_keeps_non_ariadne_events():
    error = GraphQLError("Cannot query field 'comment' on type 'Query'.")
    event = make_event(mechanism_type="django")
    assert sentry_before_send(event, hint_for(error)) is event


def test_keeps_events_without_hint():
    event = make_event()
    assert sentry_before_send(event, {}) is event
    assert sentry_before_send({"message": "hi"}, {}) == {"message": "hi"}
