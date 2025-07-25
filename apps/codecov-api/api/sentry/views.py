import logging

from django.conf import settings
from rest_framework import serializers
from rest_framework.decorators import api_view, permission_classes
from rest_framework.response import Response

from codecov_auth.models import Account
from codecov_auth.permissions import JWTAuthenticationPermission
from shared.django_apps.codecov_auth.models import GithubAppInstallation, Owner, Service

log = logging.getLogger(__name__)


class OrganizationIntegrationSerializer(serializers.Serializer):
    installation_id = serializers.CharField(
        help_text="Installation ID for the integration",
        required=True,
    )
    external_id = serializers.CharField(
        help_text="External ID for the integration. This is the ID of the organization in the provider.",
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
    """Serializer for linking Sentry accounts to Codecov users."""

    sentry_org_id = serializers.CharField(help_text="The Sentry organization ID")
    sentry_org_name = serializers.CharField(
        help_text="Sentry organization name",
    )

    organizations = OrganizationIntegrationSerializer(
        many=True,
        help_text="List of organizations/integrations tied to the account.",
        required=True,
    )


@api_view(["POST"])
@permission_classes([JWTAuthenticationPermission])
def account_link(request, *args, **kwargs):
    serializer = SentryAccountLinkSerializer(data=request.data)
    serializer.is_valid(raise_exception=True)

    sentry_org_id = serializer.validated_data["sentry_org_id"]
    sentry_org_name = serializer.validated_data["sentry_org_name"]

    account, _created = Account.objects.get_or_create(
        sentry_org_id=sentry_org_id, defaults={"name": sentry_org_name}
    )

    for org_data in serializer.validated_data["organizations"]:
        if org_data["provider"] != Service.GITHUB.value:
            log.warning(
                f"Skipping linking org: {org_data['slug']} because it is not a GitHub organization"
            )
            continue

        owner, _owner_created = Owner.objects.get_or_create(
            service_id=org_data["external_id"],
            service=org_data["provider"],
            defaults={
                "account": account,
                "name": org_data["slug"],
                "username": org_data["slug"],
            },
        )

        # Update the owner to link to the account.
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
