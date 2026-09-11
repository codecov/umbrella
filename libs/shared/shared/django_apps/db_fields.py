from django.db import models

CASE_INSENSITIVE_COLLATION = "codecov_case_insensitive"


class CaseInsensitiveTextField(models.TextField):
    """TextField with a case-insensitive collation (Django 5+ replacement for CITextField)."""

    def __init__(self, *args, **kwargs):
        kwargs.setdefault("db_collation", CASE_INSENSITIVE_COLLATION)
        super().__init__(*args, **kwargs)

    def deconstruct(self):
        name, path, args, kwargs = super().deconstruct()
        return (
            name,
            "shared.django_apps.db_fields.CaseInsensitiveTextField",
            args,
            kwargs,
        )
