import django_filters

from codecov_auth.models import Owner


class BranchFilters(django_filters.FilterSet):
    author = django_filters.CharFilter(method="filter_author")

    def filter_author(self, queryset, name, value):
        # `authors` is an array of integer owner IDs; accept either an ownerid
        # or a username (resolved within the repo's service).
        value = value.strip()
        if value.isdigit():
            return queryset.filter(authors__contains=[int(value)])

        owner_ids = list(
            Owner.objects.filter(
                username=value,
                service__in=queryset.values("repository__author__service"),
            ).values_list("ownerid", flat=True)
        )
        if not owner_ids:
            return queryset.none()
        return queryset.filter(authors__overlap=owner_ids)
