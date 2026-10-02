import pytest

from shared.django_apps.codecov_auth.models import Owner
from shared.django_apps.codecov_auth.tests.factories import OwnerFactory


@pytest.mark.parametrize("lookup", ["contains", "startswith", "endswith", "regex"])
def test_case_insensitive_pattern_lookups_cast_to_citext(lookup):
    queryset = Owner.objects.filter(**{f"username__{lookup}": "TeSt"})
    assert '("owners"."username"::text)::citext' in str(queryset.query)


@pytest.mark.django_db
@pytest.mark.parametrize(
    ("lookup", "value"),
    [
        ("contains", "foobar"),
        ("startswith", "foo"),
        ("endswith", "bar"),
        ("regex", "^foo"),
    ],
)
def test_case_insensitive_pattern_lookups_preserve_citext_semantics(lookup, value):
    owner = OwnerFactory(username="FooBar")
    assert Owner.objects.filter(**{f"username__{lookup}": value}).get() == owner
