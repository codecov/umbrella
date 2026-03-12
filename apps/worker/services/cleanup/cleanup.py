import logging

from celery.exceptions import SoftTimeLimitExceeded
from django.db import OperationalError
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
                log.info(
                    "cleanup_queryset: deleting %s with query %.500s",
                    model.__name__,
                    query.query,
                )
                try:
                    cleaned_models = query._raw_delete(query.db)
                except SoftTimeLimitExceeded:
                    log.warning(
                        "cleanup_queryset: soft time limit hit, rolling back delete of %s",
                        model.__name__,
                        exc_info=True,
                    )
                    raise
                except OperationalError:
                    log.warning(
                        "cleanup_queryset: db connection dropped, rolling back delete of %s",
                        model.__name__,
                        exc_info=True,
                    )
                    raise
                log.info(
                    "cleanup_queryset: deleted %d rows from %s",
                    cleaned_models,
                    model.__name__,
                )
                context.add_progress(cleaned_models)
