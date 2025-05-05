import logging

from django.db.models.query import QuerySet

from services.cleanup.models import MANUAL_CLEANUP
from services.cleanup.relations import build_relation_graph
from services.cleanup.utils import CleanupContext

log = logging.getLogger(__name__)


def cleanup_queryset(query: QuerySet, context: CleanupContext):
    """
    Cleans up all the models and storage files reachable from the given `QuerySet`.

    This deletes all database models in topological sort order, and also removes
    all the files in storage for any of the models in the relationship graph.
    """
    models_to_cleanup = build_relation_graph(query)

    for relation in models_to_cleanup:
        model = relation.model
        context.set_current_model(model)

        for query in relation.querysets:
            # This is needed so that the correct connection is chosen for the
            # `_raw_delete` queries, as otherwise it might chose a readonly connection.
            query._for_write = True

            manual_cleanup = MANUAL_CLEANUP.get(model)
            if manual_cleanup is not None:
                manual_cleanup(context, query)
            else:
                cleaned_models = query._raw_delete(query.db)
                context.add_progress(cleaned_models)
