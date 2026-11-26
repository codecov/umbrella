import sys

from graphql import GraphQLError

from codecov.sentry_filters import before_send


def _hint_for(exc):
    try:
        raise exc
    except Exception:
        return {"exc_info": sys.exc_info()}


def test_drops_graphql_validation_errors():
    event = {"message": "x"}
    hint = _hint_for(GraphQLError("Cannot query field 'keys' on type 'Query'."))
    assert before_send(event, hint) is None


def test_keeps_graphql_errors_wrapping_resolver_exceptions():
    event = {"message": "x"}
    hint = _hint_for(GraphQLError("boom", original_error=ValueError("boom")))
    assert before_send(event, hint) is event


def test_keeps_other_exceptions_and_messages():
    event = {"message": "x"}
    assert before_send(event, _hint_for(ValueError("boom"))) is event
    assert before_send(event, {}) is event
