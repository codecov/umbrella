import binascii
import logging
import os
import uuid
from datetime import datetime
from hashlib import md5
from typing import Optional, Self

from django.contrib.postgres.fields import ArrayField, CITextField
from django.contrib.sessions.models import Session as DjangoSession
from django.db import models
from django.db.models import Case, QuerySet, Sum, When
from django.db.models.fields import AutoField, BooleanField, IntegerField
from django.db.models.manager import BaseManager
from django.forms import ValidationError
from django.utils import timezone
from django_prometheus.models import ExportModelOperationsMixin
from model_utils import FieldTracker

from shared.config import get_config
from shared.django_apps.codecov.models import BaseCodecovModel, BaseModel
from shared.django_apps.codecov_auth.constants import (
    AVATAR_GITHUB_BASE_URL,
    AVATARIO_BASE_URL,
    BITBUCKET_BASE_URL,
    GRAVATAR_BASE_URL,
)
from shared.django_apps.codecov_auth.helpers import get_gitlab_url
from shared.django_apps.codecov_auth.managers import OwnerManager
from shared.django_apps.core.managers import RepositoryManager
from shared.django_apps.core.models import DateTimeWithoutTZField, Repository
from shared.plan.constants import DEFAULT_FREE_PLAN, PlanName, TierName, TrialDaysAmount

# Added to avoid 'doesn't declare an explicit app_label and isn't in an application in INSTALLED_APPS' error\
# Needs to be called the same as the API app
CODECOV_AUTH_APP_LABEL = "codecov_auth"

# Large number to represent Infinity as float('int') is not JSON serializable
INFINITY = 99999999

SERVICE_GITHUB = "github"
SERVICE_GITHUB_ENTERPRISE = "github_enterprise"
SERVICE_BITBUCKET = "bitbucket"
SERVICE_BITBUCKET_SERVER = "bitbucket_server"
SERVICE_GITLAB = "gitlab"
SERVICE_CODECOV_ENTERPRISE = "enterprise"


DEFAULT_AVATAR_SIZE = 55


log = logging.getLogger(__name__)


# TODO use this to refactor avatar_url
class Service(models.TextChoices):
    GITHUB = "github"
    GITLAB = "gitlab"
    BITBUCKET = "bitbucket"
    GITHUB_ENTERPRISE = "github_enterprise"
    GITLAB_ENTERPRISE = "gitlab_enterprise"
    BITBUCKET_SERVER = "bitbucket_server"


class PlanProviders(models.TextChoices):
    GITHUB = "github"


# Follow the shape of TrialStatus in plan folder
class TrialStatus(models.TextChoices):
    NOT_STARTED = "not_started"
    ONGOING = "ongoing"
    EXPIRED = "expired"
    CANNOT_TRIAL = "cannot_trial"


class User(ExportModelOperationsMixin("codecov_auth.user"), BaseCodecovModel):
    class CustomerIntent(models.TextChoices):
        BUSINESS = "BUSINESS"
        PERSONAL = "PERSONAL"

    email = CITextField(null=True)
    name = models.TextField(null=True)
    is_staff = models.BooleanField(null=True, default=False)
    is_superuser = models.BooleanField(null=True, default=False)
    external_id = models.UUIDField(default=uuid.uuid4, editable=False, unique=True)
    terms_agreement = models.BooleanField(null=True, default=False, blank=True)
    terms_agreement_at = DateTimeWithoutTZField(null=True, blank=True)
    customer_intent = models.TextField(choices=CustomerIntent.choices, null=True)
    email_opt_in = models.BooleanField(default=False)

    REQUIRED_FIELDS = []
    USERNAME_FIELD = "external_id"

    _okta_loggedin_accounts: list["Account"] = []

    class Meta:
        db_table = "users"
        app_label = CODECOV_AUTH_APP_LABEL

    @property
    def is_active(self):
        # Required to implement django's user-model interface
        return True

    @property
    def is_anonymous(self):
        # Required to implement django's user-model interface
        return False

    @property
    def is_authenticated(self):
        # Required to implement django's user-model interface
        return True

    @property
    def is_github_student(self) -> bool:
        try:
            github_owner: Owner = self.owners.get(service=Service.GITHUB)
        except Owner.DoesNotExist:
            return False
        return github_owner.student

    @property
    def default_org(self):
        try:
            return self.owners.first().default_org.username
        except AttributeError:
            return None

    def has_perm(self, perm, obj=None):
        # Required to implement django's user-model interface
        return self.is_staff

    def has_perms(self, *args, **kwargs):
        # Required to implement django's user-model interface
        return self.is_staff

    def has_module_perms(self, package_name):
        # Required to implement django's user-model interface
        return self.is_staff

    def get_username(self):
        # Required to implement django's user-model interface
        return self.external_id


class Account(BaseModel):
    name = models.CharField(max_length=100, null=False, blank=False, unique=True)
    is_active = models.BooleanField(default=True, null=False, blank=True)
    plan = models.CharField(
        max_length=50,
        choices=PlanName.choices(),
        null=False,
        default=DEFAULT_FREE_PLAN,
    )
    plan_seat_count = models.SmallIntegerField(default=1, null=False, blank=True)
    free_seat_count = models.SmallIntegerField(default=0, null=False, blank=True)
    plan_auto_activate = models.BooleanField(default=True, null=False, blank=True)
    is_delinquent = models.BooleanField(default=False, null=False, blank=True)
    sentry_org_id = models.BigIntegerField(
        null=True, blank=True
    )  # Sentry Organization IDs are 64-bit integers
    users = models.ManyToManyField(
        User, through="AccountsUsers", related_name="accounts"
    )

    class Meta:
        ordering = ["-updated_at"]
        app_label = CODECOV_AUTH_APP_LABEL

    def __str__(self):
        str_representation_of_is_active = "Active" if self.is_active else "Inactive"
        return f"{str_representation_of_is_active} Account: {self.name}"

    def _student_count_helper(self) -> QuerySet:
        # This method creates the query to annotate a user as a student.
        # To be used in conjunction with filter or exclude to count students and non-students
        return (
            self.users.values("id")
            .annotate(  # count the number of student owners aggregated on users.id
                owner_student_count=Sum(
                    Case(
                        When(owners__student=True, then=1),
                        default=0,
                        output_field=IntegerField(),
                    )
                )
            )
            .annotate(  # if there are any associated student owner to this user, then it is marked as a student
                is_student=Case(
                    When(owner_student_count__gt=0, then=True),
                    default=False,
                    output_field=BooleanField(),
                )
            )
        )

    @property
    def activated_user_count(self) -> int:
        """
        Return the number of activated users. An activated user is one that does not have any student associations.
        """
        return self._student_count_helper().filter(is_student=False).count()

    @property
    def activated_student_count(self) -> int:
        return self._student_count_helper().filter(is_student=True).count()

    @property
    def all_user_count(self) -> int:
        return self.users.count()

    @property
    def organizations_count(self) -> int:
        return self.organizations.all().count()

    @property
    def total_seat_count(self) -> int:
        return self.plan_seat_count + self.free_seat_count

    @property
    def available_seat_count(self) -> int:
        count = self.total_seat_count - self.activated_user_count
        return count if count > 0 else 0

    @property
    def pretty_plan(self) -> dict | None:
        """
        This is how we represent the details of a plan to a user, see plan.constants.py
        We inject quantity to make plan management easier on api, see PlanSerializer
        """
        plan_details = Plan.objects.select_related("tier").get(name=self.plan)
        if plan_details:
            return {
                "marketing_name": plan_details.marketing_name,
                "value": plan_details.name,
                "billing_rate": plan_details.billing_rate,
                "base_unit_price": plan_details.base_unit_price,
                "benefits": plan_details.benefits,
                "tier_name": plan_details.tier.tier_name,
                "monthly_uploads_limit": plan_details.monthly_uploads_limit,
                "trial_days": TrialDaysAmount.CODECOV_SENTRY.value
                if plan_details.name == PlanName.TRIAL_PLAN_NAME.value
                else None,
                "quantity": self.plan_seat_count,
            }
        return None

    def can_activate_user(self, user: User | None = None) -> bool:
        """
        Check if account can activate a user. If no user is passed,
        then only check for available seats. Otherwise, we can activate
        a user if they're a student and if they haven't already been added.
        """
        # User is already activated, meaning their occupancy is already counted in the plan seat count.
        # Return True since activating them again costs 0 seats, so they will always fit.
        if user and user in self.users.all():
            return True

        # User is a student, return True
        if user and user.is_github_student:
            return True

        total_seats_for_account = self.plan_seat_count + self.free_seat_count
        return self.activated_user_count < total_seats_for_account

    def activate_user_onto_account(self, user: User) -> None:
        self.users.add(user)

    def activate_owner_user_onto_account(self, owner_user: "Owner") -> None:
        user: User = owner_user.user
        if not user:
            user = User.objects.create(name=owner_user.name, email=owner_user.email)
            owner_user.user = user
            owner_user.save()
        self.activate_user_onto_account(user)

    def deactivate_owner_user_from_account(self, owner_user: "Owner") -> None:
        if owner_user.user is None:
            log.warning(
                "Attempting to deactivate an owner without associated user. Skipping deactivation."
            )
            return

        organizations_in_account: list[Owner] = self.organizations.all()
        all_owner_users_for_account: set[AutoField] = {
            ownerid
            for org in organizations_in_account
            for ownerid in org.plan_activated_users
        }
        if owner_user.ownerid not in all_owner_users_for_account:
            self.users.remove(owner_user.user)
        else:
            log.info(
                "User was not removed from account because they currently are "
                "activated on another organization.",
                extra={"owner_id": owner_user.ownerid, "account_id": self.id},
            )
        return


class Owner(ExportModelOperationsMixin("codecov_auth.owner"), models.Model):
    class Meta:
        db_table = "owners"
        app_label = CODECOV_AUTH_APP_LABEL
        ordering = ["ownerid"]
        constraints = [
            models.UniqueConstraint(
                fields=["service", "username"], name="owner_service_username"
            ),
            models.UniqueConstraint(
                fields=["service", "service_id"], name="owner_service_ids"
            ),
        ]

    REQUIRED_FIELDS = []
    USERNAME_FIELD = "username"

    ownerid = models.AutoField(primary_key=True)
    service = models.TextField(choices=Service.choices)  # Really an ENUM in db
    username = CITextField(
        unique=True, null=True
    )  # No actual unique constraint on this in the DB
    email = models.TextField(null=True)
    business_email = models.TextField(null=True)
    name = models.TextField(null=True)
    oauth_token = models.TextField(null=True)
    stripe_customer_id = models.TextField(null=True, blank=True)
    stripe_subscription_id = models.TextField(null=True, blank=True)
    stripe_coupon_id = models.TextField(null=True, blank=True)
    createstamp = models.DateTimeField(null=True)
    service_id = models.TextField(null=False)
    parent_service_id = models.TextField(null=True)
    root_parent_service_id = models.TextField(null=True)
    private_access = models.BooleanField(null=True)
    staff = models.BooleanField(null=True, default=False)
    cache = models.JSONField(null=True)
    # Really an ENUM in db
    plan = models.TextField(null=True, default=DEFAULT_FREE_PLAN, blank=True)
    plan_provider = models.TextField(
        null=True, choices=PlanProviders.choices, blank=True
    )  # postgres enum containing only "github"
    plan_user_count = models.SmallIntegerField(null=True, default=1, blank=True)
    plan_auto_activate = models.BooleanField(null=True, default=True)
    plan_activated_users = ArrayField(
        models.IntegerField(null=True), null=True, blank=True
    )
    did_trial = models.BooleanField(null=True)
    trial_start_date = DateTimeWithoutTZField(null=True)
    trial_end_date = DateTimeWithoutTZField(null=True)
    trial_status = models.CharField(
        max_length=50,
        choices=TrialStatus.choices,
        null=True,
        default=TrialStatus.NOT_STARTED.value,
    )
    trial_fired_by = models.IntegerField(null=True)
    pretrial_users_count = models.SmallIntegerField(null=True, blank=True)
    free = models.SmallIntegerField(default=0)
    invoice_details = models.TextField(null=True)
    uses_invoice = models.BooleanField(default=False, null=False)
    delinquent = models.BooleanField(null=True)
    yaml = models.JSONField(null=True)
    updatestamp = DateTimeWithoutTZField(default=datetime.now)
    organizations = ArrayField(models.IntegerField(null=True), null=True, blank=True)
    admins = ArrayField(models.IntegerField(null=True), null=True, blank=True)

    # DEPRECATED - replaced by GithubAppInstallation model
    integration_id = models.IntegerField(null=True, blank=True)

    permission = ArrayField(models.IntegerField(null=True), null=True)
    bot = models.ForeignKey(
        "Owner", db_column="bot", null=True, on_delete=models.SET_NULL, blank=True
    )
    student = models.BooleanField(default=False)
    student_created_at = DateTimeWithoutTZField(null=True)
    student_updated_at = DateTimeWithoutTZField(null=True)
    onboarding_completed = models.BooleanField(default=False)
    is_superuser = models.BooleanField(null=True, default=False)
    max_upload_limit = models.IntegerField(null=True, default=150, blank=True)
    upload_token_required_for_public_repos = models.BooleanField(default=False)

    sentry_user_id = models.TextField(null=True, blank=True, unique=True)
    sentry_user_data = models.JSONField(null=True)

    user = models.ForeignKey(
        User,
        null=True,
        on_delete=models.SET_NULL,
        blank=True,
        related_name="owners",
    )

    account = models.ForeignKey(
        Account,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="organizations",
    )

    objects = OwnerManager()
    tracker = FieldTracker(
        fields=["username", "service", "upload_token_required_for_public_repos"]
    )
    repository_set = RepositoryManager()

    def __str__(self):
        return f"Owner<{self.service}/{self.username}>"

    def save(self, *args, **kwargs):
        self.updatestamp = timezone.now()
        super().save(*args, **kwargs)

    @property
    def has_yaml(self):
        return self.yaml is not None

    @property
    def default_org(self):
        try:
            if self.profile:
                return self.profile.default_org
        except OwnerProfile.DoesNotExist:
            return None

    @property
    def has_legacy_plan(self):
        return self.plan is None or not self.plan.startswith("users")

    @property
    def repo_total_credits(self):
        # Returns the number of private repo credits remaining
        # Only meaningful for legacy plans
        V4_PLAN_PREFIX = "v4-"
        if not self.has_legacy_plan:
            return INFINITY
        if self.plan is None:
            return int(1 + self.free or 0)
        elif self.plan.startswith(V4_PLAN_PREFIX):
            return int(self.plan[3:-1])
        else:
            return int(self.plan[:-1])

    @property
    def root_organization(self: "Owner") -> Optional["Owner"]:
        """
        Find the root organization of Gitlab, by using the root_parent_service_id
        if it exists, otherwise iterating through the parents and caches it in root_parent_service_id
        """
        if self.root_parent_service_id:
            return Owner.objects.get(
                service_id=self.root_parent_service_id, service=self.service
            )

        root = None
        if self.service == "gitlab" and self.parent_service_id:
            root = self
            while root.parent_service_id is not None:
                root = Owner.objects.get(
                    service_id=root.parent_service_id, service=root.service
                )
            self.root_parent_service_id = root.service_id
            self.save()
        return root

    @property
    def nb_active_private_repos(self):
        return self.repository_set.filter(active=True, private=True).count()

    @property
    def has_public_repos(self):
        return self.repository_set.filter(private=False).exists()

    @property
    def has_private_repos(self):
        return self.repository_set.filter(private=True).exists()

    @property
    def has_active_repos(self):
        return self.repository_set.filter(active=True).exists()

    @property
    def repo_credits(self):
        # Returns the number of private repo credits remaining
        # Only meaningful for legacy plans
        if not self.has_legacy_plan:
            return INFINITY
        return self.repo_total_credits - self.nb_active_private_repos

    @property
    def orgs(self):
        if self.organizations:
            return Owner.objects.filter(ownerid__in=self.organizations)
        return Owner.objects.none()

    @property
    def active_repos(self):
        return Repository.objects.filter(active=True, author=self.ownerid).order_by(
            "-updatestamp"
        )

    @property
    def activated_user_count(self):
        if not self.plan_activated_users:
            return 0
        return Owner.objects.filter(
            ownerid__in=self.plan_activated_users, student=False
        ).count()

    @property
    def activated_student_count(self):
        if not self.plan_activated_users:
            return 0
        return Owner.objects.filter(
            ownerid__in=self.plan_activated_users, student=True
        ).count()

    @property
    def student_count(self):
        return Owner.objects.users_of(self).filter(student=True).count()

    @property
    def inactive_user_count(self):
        return (
            Owner.objects.users_of(self).filter(student=False).count()
            - self.activated_user_count
        )

    @property
    def total_seat_count(self) -> int:
        if self.plan_user_count is None:
            return self.free
        return self.plan_user_count + self.free

    def is_admin(self, owner):
        return self.ownerid == owner.ownerid or (
            bool(self.admins) and owner.ownerid in self.admins
        )

    @property
    def is_authenticated(self):
        # NOTE: this is here to support `UserTokenAuthentication` which still returns
        # an `Owner` as the authenticatable record.  Since there is code that calls
        # `request.user.is_authenticated` we need to support that here.
        return True

    def clean(self):
        if self.staff:
            domain = self.email.split("@")[1] if self.email else ""
            if domain not in ["codecov.io", "sentry.io"]:
                raise ValidationError(
                    "User not part of Codecov or Sentry cannot be a staff member"
                )
        if not self.plan:
            self.plan = None
        if not self.stripe_customer_id:
            self.stripe_customer_id = None
        if not self.stripe_subscription_id:
            self.stripe_subscription_id = None

    @property
    def avatar_url(self, size=DEFAULT_AVATAR_SIZE):
        if self.service == SERVICE_GITHUB and self.service_id:
            return f"{AVATAR_GITHUB_BASE_URL}/u/{self.service_id}?v=3&s={size}"

        elif self.service == SERVICE_GITHUB_ENTERPRISE and self.service_id:
            return "{}/avatars/u/{}?v=3&s={}".format(
                get_config("github_enterprise", "url"), self.service_id, size
            )

        # Bitbucket
        elif self.service == SERVICE_BITBUCKET and self.username:
            return f"{BITBUCKET_BASE_URL}/account/{self.username}/avatar/{size}"

        elif (
            self.service == SERVICE_BITBUCKET_SERVER
            and self.service_id
            and self.username
        ):
            if "U" in self.service_id:
                return "{}/users/{}/avatar.png?s={}".format(
                    get_config("bitbucket_server", "url"), self.username, size
                )
            else:
                return "{}/projects/{}/avatar.png?s={}".format(
                    get_config("bitbucket_server", "url"), self.username, size
                )

        # Gitlab
        elif self.service == SERVICE_GITLAB and self.email:
            return get_gitlab_url(self.email, size)

        # Codecov config
        elif get_config("services", "gravatar") and self.email:
            return f"{GRAVATAR_BASE_URL}/avatar/{md5(self.email.lower().encode()).hexdigest()}?s={size}"

        elif get_config("services", "avatars.io") and self.email:
            return f"{AVATARIO_BASE_URL}/avatar/{md5(self.email.lower().encode()).hexdigest()}/{size}"

        elif self.ownerid:
            return "{}/users/{}.png?size={}".format(
                get_config("setup", "codecov_url"), self.ownerid, size
            )

        elif os.getenv("APP_ENV") == SERVICE_CODECOV_ENTERPRISE:
            return "{}/media/images/gafsi/avatar.svg".format(
                get_config("setup", "codecov_url")
            )

        else:
            return "{}/media/images/gafsi/avatar.svg".format(
                get_config("setup", "media", "assets")
            )

    @property
    def pretty_plan(self):
        if self.account:
            return self.account.pretty_plan

        plan_details = Plan.objects.select_related("tier").get(name=self.plan)
        if plan_details:
            return {
                "marketing_name": plan_details.marketing_name,
                "value": plan_details.name,
                "billing_rate": plan_details.billing_rate,
                "base_unit_price": plan_details.base_unit_price,
                "benefits": plan_details.benefits,
                "tier_name": plan_details.tier.tier_name,
                "monthly_uploads_limit": plan_details.monthly_uploads_limit,
                "trial_days": TrialDaysAmount.CODECOV_SENTRY.value
                if plan_details.name == PlanName.TRIAL_PLAN_NAME.value
                else None,
                "quantity": self.plan_user_count,
            }
        return None

    def can_activate_user(self, owner_user: Self) -> bool:
        owner_org = self
        if owner_user.student:
            return True
        if owner_org.account:
            return owner_org.account.can_activate_user(owner_user.user)
        return (
            owner_org.activated_user_count < owner_org.plan_user_count + owner_org.free
        )

    def activate_user(self, owner_user: Self) -> None:
        owner_org = self
        log.info(f"Activating user {owner_user.ownerid} in ownerid {owner_org.ownerid}")
        if isinstance(owner_org.plan_activated_users, list):
            if owner_user.ownerid not in owner_org.plan_activated_users:
                owner_org.plan_activated_users.append(owner_user.ownerid)
        else:
            owner_org.plan_activated_users = [owner_user.ownerid]
        owner_org.save()

        if owner_org.account:
            owner_org.account.activate_owner_user_onto_account(owner_user)

    def deactivate_user(self, owner_user: Self) -> None:
        owner_org = self
        log.info(
            f"Deactivating user {owner_user.ownerid} in ownerid {owner_org.ownerid}"
        )
        if isinstance(owner_org.plan_activated_users, list):
            try:
                owner_org.plan_activated_users.remove(owner_user.ownerid)
            except ValueError:
                pass
        owner_org.save()

        if owner_org.account and owner_user.user:
            owner_org.account.deactivate_owner_user_from_account(owner_user)

    def add_admin(self, user):
        log.info(
            f"Granting admin permissions to user {user.ownerid} within owner {self.ownerid}"
        )
        if isinstance(self.admins, list):
            if user.ownerid not in self.admins:
                self.admins.append(user.ownerid)
        else:
            self.admins = [user.ownerid]
        self.save()

    def remove_admin(self, user):
        log.info(
            f"Revoking admin permissions for user {user.ownerid} within owner {self.ownerid}"
        )
        if isinstance(self.admins, list):
            try:
                self.admins.remove(user.ownerid)
            except ValueError:
                pass
        self.save()


GITHUB_APP_INSTALLATION_DEFAULT_NAME = "codecov_app_installation"


class GithubAppInstallation(
    ExportModelOperationsMixin("codecov_auth.github_app_installation"), BaseCodecovModel
):
    # replacement for owner.integration_id
    # installation id GitHub sends us in the installation-related webhook events
    installation_id = models.IntegerField(null=False)
    name = models.TextField(default=GITHUB_APP_INSTALLATION_DEFAULT_NAME)
    # if null, all repos are covered by this installation
    # otherwise, it's a list of repo.id values
    repository_service_ids = ArrayField(models.TextField(null=False), null=True)

    # Needed to get a JWT for the app
    # app_id and pem_path for default apps are configured in the install YAML
    app_id = models.IntegerField(null=False)
    pem_path = models.TextField(null=True)

    is_suspended = models.BooleanField(null=False, default=False)

    owner = models.ForeignKey(
        Owner,
        null=False,
        on_delete=models.CASCADE,
        related_name="github_app_installations",
    )

    class Meta:
        app_label = CODECOV_AUTH_APP_LABEL
        unique_together = ("app_id", "installation_id")

    def is_configured(self) -> bool:
        """Returns whether this installation is properly configured and can be used"""
        if self.app_id is not None and self.pem_path is not None:
            return True
        if self.name == "unconfigured_app":
            return False
        # The default app is configured in the installation YAML
        installation_default_app_id = get_config("github", "integration", "id")
        if installation_default_app_id is None:
            log.error(
                "Can't find default app ID in the YAML. Assuming installation is configured to prevent the app from breaking itself.",
                extra={"installation_id": self.id, "installation_name": self.name},
            )
            return True
        return str(self.app_id) == str(installation_default_app_id)

    def repository_queryset(self) -> BaseManager[Repository]:
        """Returns a QuerySet of repositories covered by this installation"""
        if self.repository_service_ids is None:
            # All repos covered
            return Repository.objects.filter(author=self.owner)
        # Some repos covered
        return Repository.objects.filter(
            service_id__in=self.repository_service_ids, author=self.owner
        )

    def covers_all_repos(self) -> bool:
        return self.repository_service_ids is None

    def is_repo_covered_by_integration(self, repo: Repository) -> bool:
        if self.covers_all_repos():
            return repo.author.ownerid == self.owner.ownerid
        return repo.service_id in self.repository_service_ids


class OwnerInstallationNameToUseForTask(
    ExportModelOperationsMixin("codecov_auth.github_app_installation"), BaseCodecovModel
):
    owner = models.ForeignKey(
        Owner,
        null=False,
        on_delete=models.CASCADE,
        blank=False,
        related_name="installation_name_to_use_for_tasks",
    )
    installation_name = models.TextField(null=False, blank=False)
    task_name = models.TextField(null=False, blank=False)

    class Meta:
        app_label = CODECOV_AUTH_APP_LABEL
        constraints = [
            # Only 1 app name per task per owner_id
            models.UniqueConstraint(
                "owner_id", "task_name", name="single_task_name_per_owner"
            )
        ]


class SentryUser(
    ExportModelOperationsMixin("codecov_auth.sentry_user"), BaseCodecovModel
):
    user = models.ForeignKey(
        User,
        null=False,
        on_delete=models.CASCADE,
        related_name="sentry_user",
    )
    access_token = models.TextField(null=True)
    refresh_token = models.TextField(null=True)
    sentry_id = models.TextField(null=False, unique=True)
    email = models.TextField(null=True)
    name = models.TextField(null=True)

    class Meta:
        app_label = CODECOV_AUTH_APP_LABEL


class OktaUser(ExportModelOperationsMixin("codecov_auth.okta_user"), BaseCodecovModel):
    user = models.ForeignKey(
        User,
        null=False,
        on_delete=models.CASCADE,
        related_name="okta_user",
    )
    access_token = models.TextField(null=True)
    okta_id = models.TextField(null=False, unique=True)
    email = models.TextField(null=True)
    name = models.TextField(null=True)

    class Meta:
        app_label = CODECOV_AUTH_APP_LABEL


class TokenTypeChoices(models.TextChoices):
    UPLOAD = "upload"


class OrganizationLevelToken(
    ExportModelOperationsMixin("codecov_auth.organization_level_token"),
    BaseCodecovModel,
):
    owner = models.ForeignKey(
        "Owner",
        db_column="ownerid",
        related_name="organization_tokens",
        on_delete=models.CASCADE,
    )
    token = models.UUIDField(unique=True, default=uuid.uuid4)
    valid_until = models.DateTimeField(blank=True, null=True)
    token_type = models.CharField(
        max_length=50, choices=TokenTypeChoices.choices, default=TokenTypeChoices.UPLOAD
    )

    class Meta:
        app_label = CODECOV_AUTH_APP_LABEL

    def save(self, *args, **kwargs):
        super().save(*args, **kwargs)


class OwnerProfile(
    ExportModelOperationsMixin("codecov_auth.owner_profile"), BaseCodecovModel
):
    class ProjectType(models.TextChoices):
        PERSONAL = "PERSONAL"
        YOUR_ORG = "YOUR_ORG"
        OPEN_SOURCE = "OPEN_SOURCE"
        EDUCATIONAL = "EDUCATIONAL"

    class Goal(models.TextChoices):
        STARTING_WITH_TESTS = "STARTING_WITH_TESTS"
        IMPROVE_COVERAGE = "IMPROVE_COVERAGE"
        MAINTAIN_COVERAGE = "MAINTAIN_COVERAGE"
        TEAM_REQUIREMENTS = "TEAM_REQUIREMENTS"
        OTHER = "OTHER"

    owner = models.OneToOneField(
        Owner, on_delete=models.CASCADE, unique=True, related_name="profile"
    )
    type_projects = ArrayField(
        models.TextField(choices=ProjectType.choices), default=list
    )
    goals = ArrayField(models.TextField(choices=Goal.choices), default=list)
    other_goal = models.TextField(null=True)
    default_org = models.ForeignKey(
        Owner, on_delete=models.CASCADE, null=True, related_name="profiles_with_default"
    )

    class Meta:
        app_label = CODECOV_AUTH_APP_LABEL


class Session(ExportModelOperationsMixin("codecov_auth.session"), models.Model):
    class Meta:
        app_label = CODECOV_AUTH_APP_LABEL
        db_table = "sessions"
        ordering = ["-lastseen"]

    class SessionType(models.TextChoices):
        API = "api"
        LOGIN = "login"

    sessionid = models.AutoField(primary_key=True)
    token = models.UUIDField(unique=True, default=uuid.uuid4, editable=False)
    name = models.TextField(null=True)
    useragent = models.TextField(null=True)
    ip = models.TextField(null=True)
    owner = models.ForeignKey(Owner, db_column="ownerid", on_delete=models.CASCADE)
    lastseen = models.DateTimeField(null=True)
    # Really an ENUM in db
    type = models.TextField(choices=SessionType.choices)
    login_session = models.ForeignKey(
        DjangoSession, on_delete=models.CASCADE, blank=True, null=True
    )


def _generate_key():
    return binascii.hexlify(os.urandom(20)).decode()


class RepositoryToken(
    ExportModelOperationsMixin("codecov_auth.repository_token"), BaseCodecovModel
):
    class TokenType(models.TextChoices):
        UPLOAD = "upload"
        PROFILING = "profiling"
        STATIC_ANALYSIS = "static_analysis"

    repository = models.ForeignKey(
        "core.Repository",
        db_column="repoid",
        on_delete=models.CASCADE,
        related_name="tokens",
    )
    token_type = models.CharField(max_length=50, choices=TokenType.choices)
    valid_until = models.DateTimeField(blank=True, null=True)
    key = models.CharField(
        max_length=40, unique=True, editable=False, default=_generate_key
    )

    class Meta:
        app_label = CODECOV_AUTH_APP_LABEL

    @classmethod
    def generate_key(cls):
        return _generate_key()


class UserToken(
    ExportModelOperationsMixin("codecov_auth.user_token"), BaseCodecovModel
):
    class TokenType(models.TextChoices):
        API = "api"

    class Meta:
        app_label = CODECOV_AUTH_APP_LABEL

    name = models.CharField(max_length=100, null=False, blank=False)
    owner = models.ForeignKey(
        "Owner",
        db_column="ownerid",
        related_name="user_tokens",
        on_delete=models.CASCADE,
    )
    token = models.UUIDField(unique=True, default=uuid.uuid4)
    valid_until = models.DateTimeField(blank=True, null=True)
    token_type = models.CharField(
        max_length=50, choices=TokenType.choices, default=TokenType.API
    )


class AccountsUsers(BaseModel):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    account = models.ForeignKey(Account, on_delete=models.CASCADE)

    class Meta:
        unique_together = ("user", "account")
        app_label = CODECOV_AUTH_APP_LABEL
        verbose_name_plural = "Accounts Users"


class OktaSettings(BaseModel):
    account = models.ForeignKey(
        Account, on_delete=models.CASCADE, related_name="okta_settings"
    )
    client_id = models.CharField(max_length=255)
    client_secret = models.CharField(max_length=255)
    url = models.CharField(max_length=255)
    enabled = models.BooleanField(default=True, blank=True)
    enforced = models.BooleanField(default=True, blank=True)

    class Meta:
        app_label = CODECOV_AUTH_APP_LABEL


class StripeBilling(BaseModel):
    account = models.OneToOneField(
        Account, on_delete=models.CASCADE, related_name="stripe_billing"
    )
    customer_id = models.CharField(max_length=255, unique=True)
    subscription_id = models.CharField(max_length=255, null=True, blank=True)
    is_active = models.BooleanField(default=True, null=False, blank=True)

    class Meta:
        app_label = CODECOV_AUTH_APP_LABEL

    def save(self, *args, **kwargs):
        if self.is_active:
            # inactivate all other billing methods
            StripeBilling.objects.filter(account=self.account, is_active=True).exclude(
                id=self.id
            ).update(is_active=False)
            InvoiceBilling.objects.filter(account=self.account, is_active=True).update(
                is_active=False
            )
        return super().save(*args, **kwargs)


class InvoiceBilling(BaseModel):
    account = models.OneToOneField(
        Account, on_delete=models.CASCADE, related_name="invoice_billing"
    )
    account_manager = models.CharField(max_length=255, null=True, blank=True)
    invoice_notes = models.TextField(null=True, blank=True)
    is_active = models.BooleanField(default=True, null=False, blank=True)

    class Meta:
        app_label = CODECOV_AUTH_APP_LABEL

    def save(self, *args, **kwargs):
        if self.is_active:
            # inactivate all other billing methods
            StripeBilling.objects.filter(account=self.account, is_active=True).update(
                is_active=False
            )
            InvoiceBilling.objects.filter(account=self.account, is_active=True).exclude(
                id=self.id
            ).update(is_active=False)
        return super().save(*args, **kwargs)


class BillingRate(models.TextChoices):
    MONTHLY = "monthly"
    ANNUALLY = "annually"


class Plan(BaseModel):
    tier = models.ForeignKey("Tier", on_delete=models.CASCADE, related_name="plans")
    base_unit_price = models.IntegerField(default=0, blank=True)
    benefits = ArrayField(models.TextField(), blank=True, default=list)
    billing_rate = models.TextField(
        choices=BillingRate.choices,
        null=True,
        blank=True,
    )
    is_active = models.BooleanField(default=True)
    marketing_name = models.CharField(max_length=255)
    max_seats = models.IntegerField(null=True, blank=True)
    monthly_uploads_limit = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, unique=True)
    paid_plan = models.BooleanField(default=False)
    stripe_id = models.CharField(max_length=255, null=True, blank=True)

    class Meta:
        app_label = CODECOV_AUTH_APP_LABEL

    def __str__(self):
        return self.name

    @property
    def is_free_plan(self):
        return not self.paid_plan

    @property
    def is_pro_plan(self):
        return (
            self.tier.tier_name == TierName.PRO.value
            or self.tier.tier_name == TierName.SENTRY.value
        )

    @property
    def is_team_plan(self):
        return self.tier.tier_name == TierName.TEAM.value

    @property
    def is_enterprise_plan(self):
        return self.tier.tier_name == TierName.ENTERPRISE.value

    @property
    def is_sentry_plan(self):
        return self.tier.tier_name == TierName.SENTRY.value

    @property
    def is_trial_plan(self):
        return self.tier.tier_name == TierName.TRIAL.value


class Tier(BaseModel):
    tier_name = models.CharField(max_length=255, unique=True)
    bundle_analysis = models.BooleanField(default=False)
    test_analytics = models.BooleanField(default=False)
    flaky_test_detection = models.BooleanField(default=False)
    project_coverage = models.BooleanField(default=False)
    private_repo_support = models.BooleanField(default=False)

    class Meta:
        app_label = CODECOV_AUTH_APP_LABEL

    def __str__(self):
        return self.tier_name
