from types import SimpleNamespace

from shared.django_apps.db_fields import CaseInsensitiveTextField


def test_case_insensitive_text_field_uses_citext_on_postgres():
    field = CaseInsensitiveTextField()
    assert field.db_type(SimpleNamespace(vendor="postgresql")) == "citext"
    assert field.db_type(SimpleNamespace(vendor="sqlite")) == "text"


def test_case_insensitive_text_field_deconstruct_path():
    name, path, args, kwargs = CaseInsensitiveTextField(null=True).deconstruct()
    assert path == "shared.django_apps.db_fields.CaseInsensitiveTextField"
    assert kwargs.get("null") is True
