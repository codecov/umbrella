from graphql import GraphQLError, build_schema, parse, validate

from codecov.sentry_filters import before_send, is_graphql_client_error

SCHEMA = build_schema("type Query { failing: Boolean }")


def _validation_error() -> GraphQLError:
    return validate(SCHEMA, parse("{ usersByIds { id } }"))[0]


def _event(mechanism_type: str) -> dict:
    return {
        "exception": {
            "values": [{"type": "GraphQLError", "mechanism": {"type": mechanism_type}}]
        }
    }


def test_validation_error_is_client_error():
    assert is_graphql_client_error(_validation_error())


def test_resolver_errors_are_not_client_errors():
    wrapped = GraphQLError("boom", path=["failing"], original_error=ValueError("x"))
    raised_in_resolver = GraphQLError("nope", path=["failing"])
    assert not is_graphql_client_error(wrapped)
    assert not is_graphql_client_error(raised_in_resolver)
    assert not is_graphql_client_error(ValueError("x"))


def test_before_send_drops_ariadne_validation_errors():
    error = _validation_error()
    hint = {"exc_info": (type(error), error, None)}
    assert before_send(_event("ariadne"), hint) is None


def test_before_send_keeps_ariadne_resolver_errors():
    error = GraphQLError("boom", path=["failing"], original_error=ValueError("x"))
    event = _event("ariadne")
    assert before_send(event, {"exc_info": (type(error), error, None)}) is event


def test_before_send_keeps_non_ariadne_events():
    error = _validation_error()
    event = _event("django")
    assert before_send(event, {"exc_info": (type(error), error, None)}) is event
    assert before_send(event, {}) is event
