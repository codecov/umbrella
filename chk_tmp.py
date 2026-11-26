
import logging
from ariadne import make_executable_schema, ObjectType, graphql_sync, format_error
from graphql import GraphQLError
s = make_executable_schema("""input O { direction: String } input F { ordering: O } type Query { stuff(filters: F!): String failing: String }""", ObjectType("Query"))
calls=[]
def fmt(error, debug=False):
    is_variable_error = error.formatted["message"].startswith("Variable '$")
    oe = error.original_error
    if is_variable_error and (oe is None or isinstance(oe, GraphQLError)):
        return {"message": error.message, "type": "ValidationError"}
    calls.append(error.message)
    return {"message": "INTERNAL SERVER ERROR"}
print(graphql_sync(s, {"query": "query Q($filters: F!) { stuff(filters: $filters) }", "variables": {"filters": {"ordering": "NAME"}}}, error_formatter=fmt)[1], calls)
