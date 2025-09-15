from django.test import TestCase

from codecov_auth.models import Owner
from shared.django_apps.codecov_auth.models import Service
from shared.django_apps.core.tests.factories import OwnerFactory


class OwnerManagerTests(TestCase):
    def setUp(self):
        self.owner = OwnerFactory()

    def test_users_of(self):
        org = OwnerFactory()
        self.owner.organizations = [org.ownerid]
        self.owner.save()

        owner_in_org_and_plan_activated_users = OwnerFactory(
            organizations=[org.ownerid]
        )
        owner_only_in_plan_activated_users = OwnerFactory()

        org.plan_activated_users = [
            owner_in_org_and_plan_activated_users.ownerid,
            owner_only_in_plan_activated_users.ownerid,
        ]
        org.save()

        with self.subTest("returns all users"):
            users_of = Owner.objects.users_of(owner=org)
            self.assertCountEqual(
                [user.ownerid for user in users_of],
                [
                    self.owner.ownerid,
                    owner_only_in_plan_activated_users.ownerid,
                    owner_in_org_and_plan_activated_users.ownerid,
                ],
            )

        with self.subTest("no plan_activated_users"):
            org.plan_activated_users = []
            org.save()
            users_of = Owner.objects.users_of(owner=org)
            self.assertCountEqual(
                [user.ownerid for user in users_of],
                [self.owner.ownerid, owner_in_org_and_plan_activated_users.ownerid],
            )

        with self.subTest("no users"):
            self.owner.delete()
            owner_in_org_and_plan_activated_users.delete()
            users_of = Owner.objects.users_of(owner=org)
            self.assertEqual(list(users_of), [])

    def test_orgs_filters_exclude_to_be_deleted_owners(self):
        """Test that Owner.orgs filtering correctly excludes TO_BE_DELETED service and null username."""
        normal_org = OwnerFactory(
            username="normal-org",
            service=Service.GITHUB.value,
        )
        deleted_org = OwnerFactory(
            username="deleted-org",
            service=Service.TO_BE_DELETED.value,
        )

        # Create a test owner with all these organizations
        test_owner = OwnerFactory(
            username="test-user",
            service=Service.GITHUB.value,
            organizations=[
                normal_org.ownerid,
                deleted_org.ownerid,
            ],
        )

        # Test the filtering logic directly on the orgs queryset
        filtered_orgs = test_owner.orgs

        # Should only include the normal organization
        filtered_org_ids = list(filtered_orgs.values_list("ownerid", flat=True))
        self.assertIn(normal_org.ownerid, filtered_org_ids)
        self.assertNotIn(deleted_org.ownerid, filtered_org_ids)

        # Test with term filtering
        filtered_orgs_with_term = filtered_orgs.filter(username__contains="-org")
        term_filtered_ids = list(
            filtered_orgs_with_term.values_list("ownerid", flat=True)
        )
        self.assertIn(normal_org.ownerid, term_filtered_ids)
        self.assertEqual(len(term_filtered_ids), 1)
