from django.db import transaction
from django.db.models import Min

from shared.config import get_config


def backfill_app_id(model):
    """
    Making this field non-nullable. It was nullable so that we could live load the app_id from
    the value set in configs.
    We no longer want to do that, so any GithubAppInstallations with None will be backfilled
    with the default app_id from configs.

    If there is no default app_id in configs, we assume there are no nulls in the db.
    """
    installation_default_app_id = get_config("github", "integration", "id")
    if installation_default_app_id is not None:
        with transaction.atomic():
            model.objects.filter(app_id__isnull=True).update(
                app_id=installation_default_app_id
            )


def eliminate_dupes(model):
    """
    There are no unique constraints on this model so duplicates exist.
    Eliminate them so we can add a unique_together on installation_id and app_id.
    """
    with transaction.atomic():
        # group by insallation_id + app_id, getting the first object created from each unique group
        # this will include objects with no duplicate AND the first created object in the set of duplicates
        to_keep = (
            model.objects.values("installation_id", "app_id")
            .annotate(min_id=Min("id"))
            .values_list("min_id", flat=True)
        )

        model.objects.exclude(id__in=to_keep).delete()
