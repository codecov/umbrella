import json
import logging

import sentry_sdk
from django.conf import settings
from rest_framework import serializers, status
from rest_framework.decorators import (
    api_view,
    authentication_classes,
    permission_classes,
)
from rest_framework.response import Response

from api.public.v2.test_results.serializers import TestrunSerializer
from codecov_auth.models import Account
from codecov_auth.permissions import JWTAuthenticationPermission
from shared.django_apps.codecov_auth.models import (
    GithubAppInstallation,
    Owner,
    Plan,
    Service,
)
from shared.django_apps.core.models import Repository
from shared.django_apps.ta_timeseries.models import Testrun
from shared.plan.constants import PlanName

log = logging.getLogger(__name__)


class OrganizationIntegrationSerializer(serializers.Serializer):
    installation_id = serializers.CharField(
        help_text="Installation ID for the integration",
        required=True,
    )
    service_id = serializers.CharField(
        help_text="Service ID for the provider org.",
        required=True,
    )
    slug = serializers.CharField(
        help_text="Slug for the organization",
        required=True,
    )
    provider = serializers.ChoiceField(
        choices=Service.values,
        help_text="Provider for the integration (e.g., github, gitlab, etc.)",
        required=True,
    )


class SentryAccountLinkSerializer(serializers.Serializer):
    """Serializer for linking Sentry organizations to Codecov organizations"""

    sentry_org_id = serializers.CharField(help_text="The Sentry organization ID")
    sentry_org_name = serializers.CharField(
        help_text="Sentry organization name",
    )

    organizations = OrganizationIntegrationSerializer(
        many=True,
        help_text="List of organizations/integrations tied to the account.",
        required=True,
    )


class SentryAccountUnlinkSerializer(serializers.Serializer):
    """Serializer for unlinking Sentry organizations from Codecov organizations."""

    sentry_org_ids = serializers.ListField(
        child=serializers.CharField(),
        help_text="List of Sentry organization IDs to unlink",
        min_length=1,
    )


@api_view(["POST"])
@authentication_classes([])
@permission_classes([JWTAuthenticationPermission])
def account_link(request, *args, **kwargs):
    serializer = SentryAccountLinkSerializer(data=request.data)
    serializer.is_valid(raise_exception=True)

    sentry_org_id = serializer.validated_data["sentry_org_id"]
    sentry_org_name = serializer.validated_data["sentry_org_name"]

    account_to_reactivate = None
    github_orgs = []  # list of organizations to link to the account. Only GitHub organizations are allowed.

    # First pass: Check for conflicts and non-github organizations and check for inactive account to reactivate.
    for org_data in serializer.validated_data["organizations"]:
        if org_data["provider"] != Service.GITHUB.value:
            log.warning(
                f"Skipping linking org: {org_data['slug']} because it is not a GitHub organization"
            )
            continue

        github_orgs.append(org_data)

        try:
            existing_owner = Owner.objects.get(
                service_id=org_data["service_id"], service=org_data["provider"]
            )

            # Check if the organization has an existing paid plan that should
            # be preserved
            # FIXME: This is temporary (famous last words) and will be changed
            # once you can actually purchase prevent plan in Sentry.
            # At that point the plan is to _cancel_ existing codecov plans and
            # just use the Sentry one.
            if existing_owner.plan:
                try:
                    plan_obj = Plan.objects.get(name=existing_owner.plan)
                    if plan_obj.paid_plan:
                        # This owner has a paid plan, we should not override
                        # it with Sentry merge plan
                        return Response(
                            {
                                "message": (
                                    f"Organization {org_data['slug']} already has an "
                                    f"active paid plan ({existing_owner.plan}). "
                                    f"Cannot link to Sentry account as it would "
                                    f"override existing billing."
                                )
                            },
                            status=status.HTTP_400_BAD_REQUEST,
                        )
                except Plan.DoesNotExist:
                    sentry_sdk.capture_message(
                        f"Owner {existing_owner.ownerid} has a plan {existing_owner.plan} that does not exist in Plan model",
                        level="warning",
                    )
                    log.warning(
                        f"Owner {existing_owner.ownerid} has a plan {existing_owner.plan} that does not exist in Plan model"
                    )

            # If the organization is already linked to an active Sentry account,
            # return an error. If the organization is linked to an inactive
            # Sentry account, set it to reactivate later
            if (
                existing_owner.account
                and existing_owner.account.plan == PlanName.SENTRY_MERGE_PLAN.value
            ):
                if existing_owner.account.is_active:
                    return Response(
                        {
                            "message": (
                                f"Organization {org_data['slug']} is already linked to "
                                f"an active Sentry account"
                            )
                        },
                        status=status.HTTP_400_BAD_REQUEST,
                    )
                elif account_to_reactivate is None:
                    account_to_reactivate = existing_owner.account
        except Owner.DoesNotExist:
            pass

    if not github_orgs:
        return Response(
            {"message": "No GitHub organizations found to link"},
            status=status.HTTP_400_BAD_REQUEST,
        )

    # Second pass: Account linking step, either reactivate or create a new
    # account if there is no inactive account to reactivate
    if account_to_reactivate:
        account = account_to_reactivate
        account.sentry_org_id = sentry_org_id
        account.name = sentry_org_name
        account.plan = PlanName.SENTRY_MERGE_PLAN.value
        account.is_active = True
        account.save()
    else:
        account, created = Account.objects.get_or_create(
            sentry_org_id=sentry_org_id,
            defaults={
                "name": sentry_org_name,
                "plan": PlanName.SENTRY_MERGE_PLAN.value,
                "is_active": True,
            },
        )

        if not created:
            account.name = sentry_org_name
            account.plan = PlanName.SENTRY_MERGE_PLAN.value
            account.is_active = True
            account.save()

    for org_data in github_orgs:
        owner, _owner_created = Owner.objects.get_or_create(
            service_id=org_data["service_id"],
            service=org_data["provider"],
            defaults={
                "account": account,
                "name": org_data["slug"],
                "username": org_data["slug"],
            },
        )

        owner.account = account
        owner.save()

        installation_id = org_data["installation_id"]
        GithubAppInstallation.objects.get_or_create(
            installation_id=installation_id,
            defaults={
                "owner": owner,
                "installation_id": installation_id,
                "name": settings.GITHUB_SENTRY_APP_NAME,
                "app_id": settings.GITHUB_SENTRY_APP_ID,
            },
        )

    return Response(
        {
            "message": "Account linked successfully",
        }
    )


@api_view(["POST"])
@authentication_classes([])
@permission_classes([JWTAuthenticationPermission])
def account_unlink(request, *args, **kwargs):
    serializer = SentryAccountUnlinkSerializer(data=request.data)
    serializer.is_valid(raise_exception=True)

    sentry_org_ids = serializer.validated_data["sentry_org_ids"]
    successfully_unlinked = 0
    total_requested = len(sentry_org_ids)

    for sentry_org_id in sentry_org_ids:
        try:
            account = Account.objects.get(sentry_org_id=sentry_org_id)
            account.is_active = False
            account.save()
            successfully_unlinked += 1
        except Account.DoesNotExist:
            log.warning(
                f"Account with Sentry organization ID {sentry_org_id} not found"
            )
            pass

    return Response(
        {
            "message": f"Unlinked {successfully_unlinked} of {total_requested} accounts",
            "successfully_unlinked": successfully_unlinked,
            "total_requested": total_requested,
        }
    )


class SentryTestAnalyticsEuSerializer(serializers.Serializer):
    """Serializer for test analytics EU endpoint"""

    integration_names = serializers.ListField(
        child=serializers.CharField(),
        help_text="The Sentry integration names",
        min_length=1,
    )


@api_view(["POST"])
@authentication_classes([])
@permission_classes([JWTAuthenticationPermission])
def test_analytics_eu(request, *args, **kwargs):
    serializer = SentryTestAnalyticsEuSerializer(data=request.data)
    serializer.is_valid(raise_exception=True)

    integration_names = serializer.validated_data["integration_names"]

    # For every integration name, determine if an Owner record exist by filtering by name and service=github
    test_runs_per_integration = {}
    for name in integration_names:
        try:
            owner = Owner.objects.get(name=name, service=Service.GITHUB)
        except Owner.DoesNotExist:
            log.warning(
                f"Owner with name {name} and service {Service.GITHUB} not found"
            )
            continue

        repositories = Repository.objects.filter(
            author=owner, test_analytics_enabled=True
        )

        # For each repository, get the list of test runs
        test_runs_per_repository = {}
        for repository in repositories:
            test_runs = Testrun.objects.filter(repo_id=repository.repoid)
            test_runs_json = json.dumps(TestrunSerializer(test_runs, many=True).data)
            test_runs_per_repository[repository.name] = test_runs_json

        # Store each test_runs_per_repository in a dictionary
        test_runs_per_integration[name] = test_runs_per_repository

    return Response(
        {
            "test_runs_per_integration": test_runs_per_integration,
        }
    )
