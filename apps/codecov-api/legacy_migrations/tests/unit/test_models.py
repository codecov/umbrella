from django.test import TestCase

from legacy_migrations.models import YamlHistory
from legacy_migrations.tests.factories import YamlHistoryFactory
from shared.django_apps.core.tests.factories import OwnerFactory


class TestYamlHistory(TestCase):
    def test_get_pieces_of_model(self):
        owner = OwnerFactory()
        author = OwnerFactory()
        yaml = YamlHistoryFactory(author=author, ownerid=owner, message="some_message")

        assert yaml.ownerid == owner
        assert yaml.author == author
        assert yaml.message == "some_message"

        assert YamlHistory.objects.count() == 1
