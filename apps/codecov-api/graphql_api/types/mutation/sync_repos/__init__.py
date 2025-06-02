from graphql_api.helpers.ariadne import ariadne_load_local_graphql

from .sync_repos import (
    error_sync_repos,
    resolve_sync_repos,
)

gql_sync_repos = ariadne_load_local_graphql(__file__, "sync_repos.graphql")


__all__ = ["resolve_sync_repos", "error_sync_repos"]
