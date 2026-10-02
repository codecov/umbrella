from django.db import models
from django.db.models import lookups


class _CITextLookupMixin:
    def process_lhs(self, compiler, connection, lhs=None):
        sql, params = super().process_lhs(compiler, connection, lhs)
        if connection.vendor == "postgresql":
            return f"({sql})::citext", params
        return sql, params


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


@CaseInsensitiveTextField.register_lookup
class CITextContains(_CITextLookupMixin, lookups.Contains):
    pass


@CaseInsensitiveTextField.register_lookup
class CITextStartsWith(_CITextLookupMixin, lookups.StartsWith):
    pass


@CaseInsensitiveTextField.register_lookup
class CITextEndsWith(_CITextLookupMixin, lookups.EndsWith):
    pass


@CaseInsensitiveTextField.register_lookup
class CITextRegex(_CITextLookupMixin, lookups.Regex):
    pass
