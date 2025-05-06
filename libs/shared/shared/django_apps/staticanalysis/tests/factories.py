from uuid import uuid4

import factory
from factory.django import DjangoModelFactory

from shared.django_apps.core.tests.factories import RepositoryFactory
from shared.django_apps.staticanalysis.models import StaticAnalysisSingleFileSnapshot


class StaticAnalysisSingleFileSnapshotFactory(DjangoModelFactory):
    class Meta:
        model = StaticAnalysisSingleFileSnapshot

    repository = factory.SubFactory(RepositoryFactory)
    file_hash = factory.LazyFunction(lambda: uuid4().hex)
    content_location = "a/b/c.txt"
    state_id = 1
