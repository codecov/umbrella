from graphql_api.helpers.ariadne import ariadne_load_local_graphql

from .delete_owner import error_delete_owner, resolve_delete_owner

gql_delete_owner = ariadne_load_local_graphql(__file__, "delete_owner.graphql")


__all__ = ["error_delete_owner", "resolve_delete_owner"]