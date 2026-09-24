from graphql_api.helpers.ariadne import ariadne_load_local_graphql

from .version import version_bindable

version = ariadne_load_local_graphql(__file__, "version.graphql")

__all__ = ["version", "version_bindable"]
