from uuid import uuid4

import factory
from factory.django import DjangoModelFactory

from shared.django_apps.upload_breadcrumbs.models import (
    Endpoints,
    Errors,
    Milestones,
    UploadBreadcrumb,
)


class UploadBreadcrumbFactory(DjangoModelFactory):
    class Meta:
        model = UploadBreadcrumb

    commit_sha = factory.LazyFunction(lambda: uuid4().hex)
    repo_id = factory.Sequence(lambda n: n)
    upload_ids = factory.List([factory.Sequence(lambda n: n) for _ in range(3)])
    sentry_trace_id = factory.LazyFunction(lambda: uuid4().hex)
    breadcrumb_data = factory.Dict(
        {
            "milestone": factory.LazyFunction(
                lambda: Milestones.FETCHING_COMMIT_DETAILS
            ),
            "endpoint": factory.LazyFunction(lambda: Endpoints.CREATE_COMMIT),
            "error": factory.LazyFunction(lambda: Errors.UNKNOWN),
            "error_text": "An unknown error occurred.",
        }
    )
