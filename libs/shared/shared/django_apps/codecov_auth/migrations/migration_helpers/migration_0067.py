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
        model.objects.filter(app_id__isnull=True).update(
            app_id=installation_default_app_id
        )
