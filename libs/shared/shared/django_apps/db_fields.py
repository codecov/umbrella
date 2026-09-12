from django.db import models


class CaseInsensitiveTextField(models.TextField):
    """Django 5+ TextField that keeps existing PostgreSQL citext columns."""

    def db_type(self, connection):
        if connection.vendor == "postgresql":
            return "citext"
        return super().db_type(connection)

    def deconstruct(self):
        name, path, args, kwargs = super().deconstruct()
        return (
            name,
            "shared.django_apps.db_fields.CaseInsensitiveTextField",
            args,
            kwargs,
        )
