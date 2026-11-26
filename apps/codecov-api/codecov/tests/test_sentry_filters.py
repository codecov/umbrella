from codecov.sentry_filters import before_send


def _event(mechanism="ariadne", exc_type="GraphQLError", value=None):
    return {
        "exception": {
            "values": [
                {
                    "type": exc_type,
                    "value": value or "Cannot query field 'teams' on type 'Query'.",
                    "mechanism": {"type": mechanism, "handled": False},
                }
            ]
        }
    }


def test_drops_unknown_field_validation_errors():
    assert before_send(_event(), {}) is None


def test_keeps_other_ariadne_errors():
    event = _event(value="Something else broke")
    assert before_send(event, {}) is event


def test_keeps_non_ariadne_errors():
    event = _event(mechanism="django")
    assert before_send(event, {}) is event


def test_keeps_other_exception_types():
    event = _event(exc_type="ValueError")
    assert before_send(event, {}) is event


def test_keeps_events_without_exceptions():
    event = {"message": "hello"}
    assert before_send(event, {}) is event
