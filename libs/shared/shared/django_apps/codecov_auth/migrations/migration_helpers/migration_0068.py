from django.db.models import Min


def eliminate_dupes(model):
    """
    There are no unique constraints on this model so duplicates exist.
    Eliminate them so we can add a unique_together on installation_id and app_id.
    """
    # group by insallation_id + app_id, getting the first object created from each unique group
    # this will include objects with no duplicate AND the first created object in the set of duplicates
    to_keep = (
        model.objects.values("installation_id", "app_id")
        .annotate(min_id=Min("id"))
        .values_list("min_id", flat=True)
    )

    model.objects.exclude(id__in=to_keep).delete()
