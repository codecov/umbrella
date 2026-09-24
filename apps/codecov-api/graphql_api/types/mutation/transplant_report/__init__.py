from graphql_api.helpers.ariadne import ariadne_load_local_graphql

from .transplant_report import error_transplant_report, resolve_transplant_report

gql_transplant_report = ariadne_load_local_graphql(__file__, "transplant_report.graphql")


__all__ = ["error_transplant_report", "resolve_transplant_report"]
