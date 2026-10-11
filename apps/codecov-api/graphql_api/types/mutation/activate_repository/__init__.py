from graphql_api.helpers.ariadne import ariadne_load_local_graphql

from .activate_repository import error_activate_repository, resolve_activate_repository

gql_activate_repository = ariadne_load_local_graphql(
    __file__, "activate_repository.graphql"
)


__all__ = ["error_activate_repository", "resolve_activate_repository"]