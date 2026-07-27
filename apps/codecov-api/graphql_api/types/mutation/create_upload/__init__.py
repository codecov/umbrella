from graphql_api.helpers.ariadne import ariadne_load_local_graphql

from .create_upload import error_create_upload, resolve_create_upload

gql_create_upload = ariadne_load_local_graphql(__file__, "create_upload.graphql")

__all__ = ["error_create_upload", "resolve_create_upload"]