from graphql import GraphQLError

from codecov.sentry_filters import before_send, is_graphql_client_query_error


def ariadne_event() -> dict:
    return {
        "exception": {
            "values": [
                {
                    "type": "GraphQLError",
                    "mechanism": {"type": "ariadne", "handled": False},
                }
            ]
        }
    }


def hint_for(error: BaseException) -> dict:
    return {"exc_info": (type(error), error, None)}


class TestIsGraphqlClientQueryError:
    def test_validation_error(self):
        error = GraphQLError("Cannot query field 'customers' on type 'Query'.")
        assert is_graphql_client_query_error(error)

    def test_execution_error_with_path(self):
        error = GraphQLError("boom", path=["owner"])
        assert not is_graphql_client_query_error(error)

    def test_execution_error_with_original_error(self):
        error = GraphQLError("boom", original_error=ValueError("boom"))
        assert not is_graphql_client_query_error(error)

    def test_non_graphql_error(self):
        assert not is_graphql_client_query_error(ValueError("boom"))
        assert not is_graphql_client_query_error(None)


class TestBeforeSend:
    def test_drops_ariadne_validation_error(self):
        error = GraphQLError("Cannot query field 'customers' on type 'Query'.")
        assert before_send(ariadne_event(), hint_for(error)) is None

    def test_keeps_ariadne_execution_error(self):
        error = GraphQLError("boom", path=["owner"], original_error=ValueError())
        event = ariadne_event()
        assert before_send(event, hint_for(error)) is event

    def test_keeps_validation_error_from_other_mechanisms(self):
        error = GraphQLError("Cannot query field 'customers' on type 'Query'.")
        event = {"exception": {"values": [{"mechanism": {"type": "generic"}}]}}
        assert before_send(event, hint_for(error)) is event

    def test_keeps_other_events(self):
        event = {"message": "hello"}
        assert before_send(event, {}) is event
        assert before_send(event, hint_for(ValueError("boom"))) is event
