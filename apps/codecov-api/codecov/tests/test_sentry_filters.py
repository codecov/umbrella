from codecov.sentry_filters import before_send


def _event(value, mechanism="ariadne", exc_type="GraphQLError"):
    return {
        "exception": {
            "values": [
                {
                    "type": exc_type,
                    "value": value,
                    "mechanism": {"type": mechanism, "handled": False},
                }
            ]
        }
    }


def test_drops_ariadne_cannot_query_field_errors():
    event = _event("Cannot query field 'tokens' on type 'Query'.")
    assert before_send(event, {}) is None


def test_keeps_other_ariadne_errors():
    event = _event("Something went wrong")
    assert before_send(event, {}) is event


def test_keeps_non_ariadne_errors():
    event = _event("Cannot query field 'tokens' on type 'Query'.", mechanism="django")
    assert before_send(event, {}) is event


def test_keeps_non_graphql_error_types():
    event = _event("Cannot query field 'x'", exc_type="ValueError")
    assert before_send(event, {}) is event


def test_keeps_events_without_exceptions():
    event = {"message": "hello"}
    assert before_send(event, {}) is event
