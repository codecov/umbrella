from graphql import GraphQLError, build_schema, parse, validate

from codecov.sentry_filters import before_send, is_client_document_error


def _validation_error() -> GraphQLError:
    schema = build_schema("type Query { failing: Boolean }")
    errors = validate(schema, parse("{ members { id } }"))
    assert errors
    return errors[0]


def _execution_error() -> GraphQLError:
    original = ValueError("boom")
    return GraphQLError("boom", path=["failing"], original_error=original)


def _event(mechanism: str = "ariadne") -> dict:
    return {
        "exception": {
            "values": [{"type": "GraphQLError", "mechanism": {"type": mechanism}}]
        }
    }


def _hint(exc: BaseException) -> dict:
    return {"exc_info": (type(exc), exc, None)}


def test_is_client_document_error():
    assert is_client_document_error(_validation_error())
    assert not is_client_document_error(_execution_error())
    assert not is_client_document_error(ValueError("not graphql"))


def test_drops_ariadne_validation_errors():
    assert before_send(_event(), _hint(_validation_error())) is None


def test_keeps_ariadne_execution_errors():
    event = _event()
    assert before_send(event, _hint(_execution_error())) is event


def test_keeps_validation_errors_from_other_mechanisms():
    event = _event(mechanism="generic")
    assert before_send(event, _hint(_validation_error())) is event


def test_keeps_events_without_exc_info():
    event = {"message": "hello"}
    assert before_send(event, {}) is event
