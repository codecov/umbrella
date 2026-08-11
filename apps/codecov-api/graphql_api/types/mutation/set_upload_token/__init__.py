from graphql_api.helpers.ariadne import ariadne_load_local_graphql

from .set_upload_token import (
    error_set_upload_token,
    resolve_set_upload_token,
)

gql_set_upload_token = ariadne_load_local_graphql(__file__, "set_upload_token.graphql")

__all__ = ["error_set_upload_token", "resolve_set_upload_token", "gql_set_upload_token"]