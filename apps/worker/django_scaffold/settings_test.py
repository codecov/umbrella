from pathlib import Path

from shared.django_apps.settings_test import *

BASE_DIR = Path(__file__).resolve().parent.parent

# Append the Celery DB result backend app so chord results are stored in
# PostgreSQL rather than Redis during tests.
INSTALLED_APPS = INSTALLED_APPS + [  # noqa: F405
    "django_celery_results",
]

BUNDLE_ANALYSIS_NOTIFY_MESSAGE_TEMPLATES = (
    BASE_DIR / "services" / "bundle_analysis" / "notify" / "messages" / "templates"
)
TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BUNDLE_ANALYSIS_NOTIFY_MESSAGE_TEMPLATES],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.contrib.auth.context_processors.auth",
                "django.template.context_processors.request",
                "django.contrib.messages.context_processors.messages",
            ]
        },
    }
]

SECRET_KEY = "*"

GCS_BUCKET_NAME = "archive"

IS_ENTERPRISE = False
