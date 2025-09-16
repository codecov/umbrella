from django.conf import settings

from shared.config import get_config


def default_installation_app_ids() -> set[str]:
    installation_default_app_id = get_config("github", "integration", "id")
    sentry_app_id = getattr(settings, "GITHUB_SENTRY_APP_ID", None)

    return set(
        map(
            str,
            filter(
                lambda x: x is not None,
                [installation_default_app_id, sentry_app_id],
            ),
        )
    )


def is_configured(app_id: int, pem_path: str | None = None) -> bool:
    if str(app_id) in default_installation_app_ids():
        return True

    return app_id is not None and pem_path is not None
