from graphql_api.helpers.ariadne import ariadne_load_local_graphql

from .regenerate_support_pin import (
    error_regenerate_support_pin,
    resolve_regenerate_support_pin,
)

gql_regenerate_support_pin = ariadne_load_local_graphql(
    __file__, "regenerate_support_pin.graphql"
)

__all__ = ["error_regenerate_support_pin", "resolve_regenerate_support_pin"]
