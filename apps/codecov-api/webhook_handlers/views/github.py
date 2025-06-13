import hmac
import logging
import re
from contextlib import suppress
from hashlib import sha1, sha256
from typing import Literal

from django.db.models import Q
from django.utils import timezone
from django.utils.crypto import constant_time_compare
from rest_framework import status
from rest_framework.exceptions import NotFound, PermissionDenied
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView

from codecov_auth.models import (
    GITHUB_APP_INSTALLATION_DEFAULT_NAME,
    GithubAppInstallation,
    Owner,
)
from core.models import Branch, Commit, Pull, Repository
from services.billing import BillingService
from services.task import TaskService
from shared.events.amplitude import AmplitudeEventPublisher
from shared.helpers.redis import get_redis_connection
from utils.config import get_config
from webhook_handlers.constants import (
    GitHubHTTPHeaders,
    GitHubWebhookEvents,
    WebhookHandlerErrorMessages,
)

from . import WEBHOOKS_ERRORED, WEBHOOKS_RECEIVED

log = logging.getLogger(__name__)


# This should probably go somewhere where it can be easily shared
regexp_ci_skip = re.compile(r"\[(ci|skip| |-){3,}\]").search


class GithubWebhookHandler(APIView):
    """
    GitHub Webhook Handler. Method names correspond to events as defined in

        webhook_handlers.constants.GitHubWebhookEvents
    """

    permission_classes = [AllowAny]
    redis = get_redis_connection()

    service_name = "github"

    @property
    def ai_features_app_id(self):
        return get_config("github", "ai_features_app_id")

    def _inc_recv(self):
        action = self.request.data.get("action", "")
        WEBHOOKS_RECEIVED.labels(
            service=self.service_name, event=self.event, action=action
        ).inc()

    def _inc_err(self, reason: str):
        action = self.request.data.get("action", "")
        WEBHOOKS_ERRORED.labels(
            service=self.service_name,
            event=self.event,
            action=action,
            error_reason=reason,
        ).inc()

    def validate_signature(self, request):
        key = get_config(
            self.service_name,
            "webhook_secret",
            default=b"testixik8qdauiab1yiffydimvi72ekq",
        )
        if isinstance(key, str):
            # If "key" comes from k8s secret, it is of type str, so
            # must convert to bytearray for use with hmac
            key = bytes(key, "utf-8")

        expected_sig = None
        computed_sig = None
        if GitHubHTTPHeaders.SIGNATURE_256 in request.META:
            expected_sig = request.META.get(GitHubHTTPHeaders.SIGNATURE_256)
            computed_sig = (
                "sha256=" + hmac.new(key, request.body, digestmod=sha256).hexdigest()
            )
        elif GitHubHTTPHeaders.SIGNATURE in request.META:
            expected_sig = request.META.get(GitHubHTTPHeaders.SIGNATURE)
            computed_sig = (
                "sha1=" + hmac.new(key, request.body, digestmod=sha1).hexdigest()
            )

        if (
            computed_sig is None
            or expected_sig is None
            or len(computed_sig) != len(expected_sig)
            or not constant_time_compare(computed_sig, expected_sig)
        ):
            self._inc_err("validation_failed")
            raise PermissionDenied()

    def unhandled_webhook_event(self, request, *args, **kwargs):
        return Response(data=WebhookHandlerErrorMessages.UNSUPPORTED_EVENT)

    def _get_repo(self, request):
        """
        Attempts to fetch the repo first via the index on (ownerid, service_id),
        then naively on service, service_id if that fails.
        """
        repo_data = self.request.data.get("repository", {})
        repo_service_id = repo_data.get("id")
        owner_service_id = repo_data.get("owner", {}).get("id")
        repo_slug = repo_data.get("full_name")

        try:
            owner = Owner.objects.get(
                service=self.service_name, service_id=owner_service_id
            )
        except Owner.DoesNotExist:
            log.info(
                f"Error fetching owner with service_id {owner_service_id}, "
                f"using repository service id to get repo",
                extra={"repo_service_id": repo_service_id, "repo_slug": repo_slug},
            )
            try:
                log.info(
                    "Unable to find repository owner, fetching repo with service, service_id",
                    extra={"repo_service_id": repo_service_id, "repo_slug": repo_slug},
                )
                return Repository.objects.get(
                    author__service=self.service_name, service_id=repo_service_id
                )
            except Repository.DoesNotExist:
                log.info(
                    "Received event for non-existent repository",
                    extra={"repo_service_id": repo_service_id, "repo_slug": repo_slug},
                )
                self._inc_err("repo_not_found")
                raise NotFound("Repository does not exist")
        else:
            try:
                log.debug(
                    "Found repository owner, fetching repo with ownerid, service_id",
                    extra={"repo_service_id": repo_service_id, "repo_slug": repo_slug},
                )
                return Repository.objects.get(
                    author__ownerid=owner.ownerid, service_id=repo_service_id
                )
            except Repository.DoesNotExist:
                default_ghapp_installation = owner.github_app_installations.filter(
                    name=GITHUB_APP_INSTALLATION_DEFAULT_NAME
                ).first()
                if default_ghapp_installation or owner.integration_id:
                    log.info(
                        "Repository no found but owner is using integration, creating repository"
                    )
                    return Repository.objects.get_or_create_from_git_repo(
                        repo_data, owner
                    )[0]
                log.info(
                    "Received event for non-existent repository",
                    extra={"repo_service_id": repo_service_id, "repo_slug": repo_slug},
                )
                self._inc_err("repo_not_found")
                raise NotFound("Repository does not exist")

    def ping(self, request, *args, **kwargs):
        return Response(data="pong")

    def repository(self, request, *args, **kwargs):
        action, repo = self.request.data.get("action"), self._get_repo(request)
        if action == "publicized":
            repo.private, repo.activated = False, False
            repo.save()
            log.info(
                "Repository publicized",
                extra={"repoid": repo.repoid, "github_webhook_event": self.event},
            )
        elif action == "privatized":
            repo.private = True
            repo.save()
            log.info(
                "Repository privatized",
                extra={"repoid": repo.repoid, "github_webhook_event": self.event},
            )
        elif action == "deleted":
            log.info(f"Request to delete repository: {repo.repoid}")
            repo.deleted = True
            repo.activated = False
            repo.active = False
            repo.name = f"{repo.name}-deleted"
            repo.save(update_fields=["deleted", "activated", "active", "name"])
            log.info(
                "Repository soft-deleted",
                extra={"repoid": repo.repoid, "github_webhook_event": self.event},
            )
        else:
            log.warning(
                f"Unknown repository action: {action}", extra={"repoid": repo.repoid}
            )
        return Response()

    def delete(self, request, *args, **kwargs):
        ref_type = request.data.get("ref_type", "")
        repo = self._get_repo(request)
        if ref_type != "branch":
            log.info(
                f"Unsupported ref type: {ref_type}, exiting",
                extra={"repoid": repo.repoid, "github_webhook_event": self.event},
            )
            return Response("Unsupported ref type")
        branch_name = self.request.data.get("ref")[11:]
        Branch.objects.filter(
            repository=self._get_repo(request), name=branch_name
        ).delete()
        log.info(
            f"Branch '{branch_name}' deleted",
            extra={"repoid": repo.repoid, "github_webhook_event": self.event},
        )
        return Response()

    def public(self, request, *args, **kwargs):
        repo = self._get_repo(request)
        repo.private, repo.activated = False, False
        repo.save()
        log.info(
            "Repository publicized",
            extra={"repoid": repo.repoid, "github_webhook_event": self.event},
        )
        return Response()

    def push(self, request, *args, **kwargs):
        ref_type = "branch" if request.data.get("ref", "")[5:10] == "heads" else "tag"
        repo = self._get_repo(request)
        if ref_type != "branch":
            log.debug(
                "Ref is tag, not branch, ignoring push event",
                extra={"repoid": repo.repoid, "github_webhook_event": self.event},
            )
            return Response("Unsupported ref type")

        if not repo.active:
            log.debug(
                "Repository is not active, ignoring push event",
                extra={"repoid": repo.repoid, "github_webhook_event": self.event},
            )
            return Response(data=WebhookHandlerErrorMessages.SKIP_NOT_ACTIVE)

        push_webhook_ignore_repos = get_config(
            "setup", "push_webhook_ignore_repo_names", default=[]
        )
        if repo.name in push_webhook_ignore_repos:
            log.debug(
                "Codecov is configured to ignore this repository name",
                extra={
                    "repoid": repo.repoid,
                    "github_webhook_event": self.event,
                    "repo_name": repo.name,
                },
            )
            return Response(data=WebhookHandlerErrorMessages.SKIP_WEBHOOK_IGNORED)

        pushed_to_branch_name = self.request.data.get("ref")[11:]
        commits = self.request.data.get("commits", [])

        if not commits:
            log.debug(
                f"No commits in webhook payload for branch {pushed_to_branch_name}",
                extra={"repoid": repo.repoid, "github_webhook_event": self.event},
            )
            return Response()

        if pushed_to_branch_name == repo.branch:
            commits_queryset = Commit.objects.filter(
                ~Q(branch=pushed_to_branch_name),
                repository=repo,
                commitid__in=[commit.get("id") for commit in commits],
                merged=False,
            )
            commits_queryset.update(branch=pushed_to_branch_name, merged=True)
            log.info(
                f"Branch name updated for commits to {pushed_to_branch_name}; setting merged to True",
                extra={
                    "repoid": repo.repoid,
                    "github_webhook_event": self.event,
                    "commits": [commit.get("id") for commit in commits],
                },
            )

        most_recent_commit = commits[-1]

        if regexp_ci_skip(most_recent_commit.get("message")):
            log.info(
                "CI skip tag on head commit, not setting status",
                extra={
                    "repoid": repo.repoid,
                    "commit": most_recent_commit.get("id"),
                    "github_webhook_event": self.event,
                },
            )
            return Response(data="CI Skipped")

        if self.redis.sismember("beta.pending", repo.repoid):
            log.info(
                "Triggering status set pending task",
                extra={
                    "repoid": repo.repoid,
                    "commit": most_recent_commit.get("id"),
                    "github_webhook_event": self.event,
                },
            )
            TaskService().status_set_pending(
                repoid=repo.repoid,
                commitid=most_recent_commit.get("id"),
                branch=pushed_to_branch_name,
                on_a_pull_request=False,
            )

        return Response()

    def status(self, request, *args, **kwargs):
        repo = self._get_repo(request)
        commitid = request.data.get("sha")

        if not repo.active:
            log.debug(
                "Repository is not active, ignoring status event",
                extra={
                    "repoid": repo.repoid,
                    "commit": commitid,
                    "github_webhook_event": self.event,
                },
            )
            return Response(data=WebhookHandlerErrorMessages.SKIP_NOT_ACTIVE)
        if request.data.get("context", "")[:8] == "codecov/":
            log.debug(
                "Recieved a web hook for a Codecov status from GitHub. We ignore these, skipping.",
                extra={
                    "repoid": repo.repoid,
                    "commit": commitid,
                    "github_webhook_event": self.event,
                },
            )
            return Response(data=WebhookHandlerErrorMessages.SKIP_CODECOV_STATUS)
        if request.data.get("state") == "pending":
            log.debug(
                "Recieved a web hook for a `pending` status from GitHub. We ignore these, skipping.",
                extra={
                    "repoid": repo.repoid,
                    "commit": commitid,
                    "github_webhook_event": self.event,
                },
            )
            return Response(data=WebhookHandlerErrorMessages.SKIP_PENDING_STATUSES)

        if not Commit.objects.filter(
            repository=repo, commitid=commitid, state="complete"
        ).exists():
            return Response(data=WebhookHandlerErrorMessages.SKIP_PROCESSING)

        log.info(
            "Triggering notify task",
            extra={
                "repoid": repo.repoid,
                "commit": commitid,
                "github_webhook_event": self.event,
            },
        )

        TaskService().notify(repoid=repo.repoid, commitid=commitid)

        return Response()

    def _is_ai_features_request(self, request):
        target_id = request.META.get(GitHubHTTPHeaders.HOOK_INSTALLATION_TARGET_ID, "")

        is_match = str(target_id) == str(self.ai_features_app_id)
        if not is_match:
            log.info(
                "Hook installation target ID does not match Codecov AI app ID",
                extra={
                    "target_id": target_id,
                    "ai_features_app_id": self.ai_features_app_id,
                    "github_webhook_event": self.event,
                    "headers": dict(request.META),
                    "request": request.data,
                },
            )
        return is_match

    def pull_request(self, request, *args, **kwargs):
        if self._is_ai_features_request(request):
            return self.check_codecov_ai_auto_enabled_reviews(request)

        repo = self._get_repo(request)

        if not repo.active:
            log.info(
                "Repository is not active, ignoring pull request event",
                extra={"repoid": repo.repoid, "github_webhook_event": self.event},
            )
            return Response(data=WebhookHandlerErrorMessages.SKIP_NOT_ACTIVE)

        action, pullid = request.data.get("action"), request.data.get("number")

        if action in ["opened", "closed", "reopened", "synchronize", "labeled"]:
            log.info(
                f"Pull request action is '{action}', triggering pulls_sync task",
                extra={
                    "repoid": repo.repoid,
                    "github_webhook_event": self.event,
                    "pullid": pullid,
                },
            )
            TaskService().pulls_sync(repoid=repo.repoid, pullid=pullid)
        elif action == "edited":
            log.info(
                f"Pull request action is 'edited', updating pull title to "
                f"'{request.data.get('pull_request', {}).get('title')}'",
                extra={
                    "repoid": repo.repoid,
                    "github_webhook_event": self.event,
                    "pullid": pullid,
                },
            )
            Pull.objects.filter(repository=repo, pullid=pullid).update(
                title=request.data.get("pull_request", {}).get("title")
            )

        return Response()

    def check_codecov_ai_auto_enabled_reviews(self, request):
        org = Owner.objects.get(
            service=self.service_name,
            service_id=request.data["repository"]["owner"]["id"],
        )

        auto_review_enabled = (
            (org.yaml or {}).get("ai_pr_review", {}).get("auto_review", False)
        )
        return Response(
            data={
                "auto_review_enabled": auto_review_enabled,
            }
        )

    def _decide_app_name(self, ghapp: GithubAppInstallation) -> str:
        """
        Possibly update the name of a GithubAppInstallation that has been fetched from DB or created.
        Only the real default installation may use the name `GITHUB_APP_INSTALLATION_DEFAULT_NAME`
        (otherwise we break the app)
        We check that apps:
            * already were given a custom name (do nothing);
            * app_id matches the configured default app app_id (use default name);
            * none of the above (use 'unconfigured_app');

        Returns the app name that should be used
        """
        if ghapp.is_configured():
            return ghapp.name
        log.warning(
            "Github installation is unconfigured. Changing name to 'unconfigured_app'",
            extra={"installation": ghapp.external_id, "previous_name": ghapp.name},
        )
        return "unconfigured_app"

    def _invalid_owner_on_existing_app_install(
        self,
        ghapp: GithubAppInstallation,
        owner: Owner,
        request,
        app_id,
        installation_id,
    ):
        log.error(
            "Unexpected error in GitHub webhook: owner collision",
            extra={
                "payload": request.data,
                "app_id": app_id,
                "installation_id": installation_id,
                "existing_owner_on_installation": ghapp.owner_id,
                "incoming_owner_on_webhook": owner.ownerid,
            },
        )
        return Response(
            {"detail": "Internal error, event ignored."},
            status=status.HTTP_200_OK,
        )

    def _handle_installation_events(
        self,
        request,
        *args,
        event: Literal[
            GitHubWebhookEvents.INSTALLATION,
            GitHubWebhookEvents.INSTALLATION_REPOSITORIES,
        ],
        **kwargs,
    ):
        service_id = request.data["installation"]["account"]["id"]
        username = request.data["installation"]["account"]["login"]
        app_id = request.data["installation"]["app_id"]
        action = request.data.get("action")

        owner, _ = Owner.objects.get_or_create(
            service=self.service_name,
            service_id=service_id,
            defaults={
                "username": username,
                "createstamp": timezone.now(),
            },
        )

        installation_id = request.data["installation"]["id"]

        # https://docs.github.com/en/webhooks/webhook-events-and-payloads#installation
        # this action only comes from GitHubWebhookEvents.INSTALLATION
        if action == "deleted":
            ghapp_installation: GithubAppInstallation | None = (
                owner.github_app_installations.filter(
                    installation_id=installation_id, app_id=app_id
                ).first()
            )
            if ghapp_installation is not None:
                # edge case, do quick validation
                if ghapp_installation.owner_id != owner.ownerid:
                    return self._invalid_owner_on_existing_app_install(
                        ghapp_installation, owner, request, app_id, installation_id
                    )
                ghapp_installation.delete()

            # Deprecated flow - BEGIN
            # these fields are no longer used, but if they have been set, clean them out
            if owner.integration_id:
                owner.integration_id = None
                owner.save()
            owner.repository_set.all().update(using_integration=False, bot=None)
            # Deprecated flow - END

            log.info(
                "Owner deleted app integration",
                extra={"ownerid": owner.ownerid, "github_webhook_event": self.event},
            )
        else:
            ghapp_installation, was_created = (
                GithubAppInstallation.objects.get_or_create(
                    installation_id=installation_id,
                    app_id=app_id,
                    defaults={"owner": owner},
                )
            )
            if was_created:
                installer_username = request.data.get("sender", {}).get("login", None)
                installer = (
                    Owner.objects.filter(
                        service=self.service_name,
                        username=installer_username,
                    ).first()
                    if installer_username
                    else None
                )
                # If installer does not exist, just attribute the action to the org owner.
                AmplitudeEventPublisher().publish(
                    "App Installed",
                    {
                        "user_ownerid": (
                            installer.ownerid
                            if installer is not None
                            else owner.ownerid
                        ),
                        "ownerid": owner.ownerid,
                    },
                )
            else:
                # edge case, do quick validation
                if ghapp_installation.owner_id != owner.ownerid:
                    return self._invalid_owner_on_existing_app_install(
                        ghapp_installation, owner, request, app_id, installation_id
                    )

            # Either update or set
            ghapp_installation.name = self._decide_app_name(ghapp_installation)

            all_repos_affected = (
                request.data["installation"].get("repository_selection")
                if event == GitHubWebhookEvents.INSTALLATION
                else request.data["repository_selection"]
            )
            if all_repos_affected == "all":
                ghapp_installation.repository_service_ids = None
            else:
                # installation event has "repositories"
                if request.data.get("repositories"):
                    repositories_service_ids = [
                        obj["id"] for obj in request.data.get("repositories", [])
                    ]
                    ghapp_installation.repository_service_ids = repositories_service_ids
                else:
                    # installation_repositories event has "repositories_added" and "repositories_removed"
                    # https://docs.github.com/en/webhooks/webhook-events-and-payloads#installation_repositories
                    current_repos = set(ghapp_installation.repository_service_ids or [])
                    repositories_added_service_ids = {
                        obj["id"] for obj in request.data.get("repositories_added", [])
                    }
                    repositories_removed_service_ids = {
                        obj["id"]
                        for obj in request.data.get("repositories_removed", [])
                    }
                    repo_list_to_save = current_repos.union(
                        repositories_added_service_ids
                    ).difference(repositories_removed_service_ids)
                    ghapp_installation.repository_service_ids = list(repo_list_to_save)

            # these actions come from GitHubWebhookEvents.INSTALLATION
            if action in ["suspend", "unsuspend"]:
                log.info(
                    "Request to suspend/unsuspend App",
                    extra={
                        "action": action,
                        "is_currently_suspended": ghapp_installation.is_suspended,
                        "ownerid": owner.ownerid,
                        "installation_id": request.data["installation"]["id"],
                    },
                )
                ghapp_installation.is_suspended = action == "suspend"

            ghapp_installation.save()

            log.info(
                "Triggering refresh task to sync repos",
                extra={"ownerid": owner.ownerid, "github_webhook_event": self.event},
            )

            repos_affected = (
                request.data.get("repositories", [])
                + request.data.get("repositories_added", [])
                + request.data.get("repositories_removed", [])
            )
            repos_affected_clean = {
                (obj["id"], obj["node_id"]) for obj in repos_affected
            }

            TaskService().refresh(
                ownerid=owner.ownerid,
                username=username,
                sync_teams=False,
                sync_repos=True,
                using_integration=True,
                repos_affected=list(repos_affected_clean),
            )

        return Response(data="Integration webhook received")

    def installation(self, request, *args, **kwargs):
        return self._handle_installation_events(
            request, *args, **kwargs, event=GitHubWebhookEvents.INSTALLATION
        )

    def installation_repositories(self, request, *args, **kwargs):
        return self._handle_installation_events(
            request,
            *args,
            **kwargs,
            event=GitHubWebhookEvents.INSTALLATION_REPOSITORIES,
        )

    def organization(self, request, *args, **kwargs):
        action = request.data.get("action")
        if action == "member_removed":
            log.info(
                f"Removing user with service-id {request.data['membership']['user']['id']} "
                f"from organization with service-id {request.data['organization']['id']}",
                extra={"github_webhook_event": self.event},
            )

            try:
                org = Owner.objects.get(
                    service=self.service_name,
                    service_id=request.data["organization"]["id"],
                )
            except Owner.DoesNotExist:
                log.info("Organization does not exist, exiting")
                return Response(
                    status=status.HTTP_400_BAD_REQUEST,
                    data="Attempted to remove member from non-Codecov org failed",
                )

            try:
                member = Owner.objects.get(
                    service=self.service_name,
                    service_id=request.data["membership"]["user"]["id"],
                )
            except Owner.DoesNotExist:
                log.info(
                    f"Member with service-id {request.data['membership']['user']['id']} "
                    f"does not exist, exiting",
                    extra={"ownerid": org.ownerid, "github_webhook_event": self.event},
                )
                return Response(
                    status=status.HTTP_400_BAD_REQUEST,
                    data="Attempted to remove non Codecov user from Codecov org failed",
                )

            # Force a sync for the removed member to remove their access to the
            # org and its private repositories.
            TaskService().refresh(
                ownerid=member.ownerid,
                username=member.username,
                sync_teams=True,
                sync_repos=True,
                using_integration=False,
            )

            try:
                if org.plan_activated_users:
                    org.plan_activated_users.remove(member.ownerid)
                    org.save(update_fields=["plan_activated_users"])
            except ValueError:
                pass

            log.info(
                f"User removal of {member.ownerid}, success",
                extra={"ownerid": org.ownerid, "github_webhook_event": self.event},
            )

        return Response()

    def _handle_marketplace_events(self, request, *args, **kwargs):
        log.info(
            "Triggering sync_plans task", extra={"github_webhook_event": self.event}
        )
        with suppress(Exception):
            # log if users purchase GHM plans while having a stripe plan
            username = request.data["marketplace_purchase"]["account"]["login"]
            new_plan_seats = request.data["marketplace_purchase"]["unit_count"]
            new_plan_name = request.data["marketplace_purchase"]["plan"]["name"]
            owner = Owner.objects.get(service=self.service_name, username=username)
            subscription = BillingService(requesting_user=owner).get_subscription(owner)
            if subscription.status == "active":
                log.warning(
                    "GHM webhook - user purchasing but has a Stripe Subscription",
                    extra={
                        "username": username,
                        "old_plan_name": subscription.plan.get("name", None),
                        "old_plan_seats": subscription.quantity,
                        "new_plan_name": new_plan_name,
                        "new_plan_seats": new_plan_seats,
                    },
                )
        TaskService().sync_plans(
            sender=request.data["sender"],
            account=request.data["marketplace_purchase"]["account"],
            action=request.data["action"],
        )
        return Response()

    def marketplace_purchase(self, request, *args, **kwargs):
        return self._handle_marketplace_events(request, *args, **kwargs)

    def member(self, request, *args, **kwargs):
        action = request.data["action"]
        if action == "removed":
            repo = self._get_repo(request)
            log.info(
                "Request to remove read permissions for user",
                extra={"repoid": repo.repoid, "github_webhook_event": self.event},
            )
            try:
                member = Owner.objects.get(
                    service=self.service_name, service_id=request.data["member"]["id"]
                )
            except Owner.DoesNotExist:
                log.info(
                    "Repository permissions unchanged -- owner doesn't exist",
                    extra={"repoid": repo.repoid, "github_webhook_event": self.event},
                )
                return Response(status=status.HTTP_404_NOT_FOUND)

            try:
                member.permission.remove(repo.repoid)
                member.save(update_fields=["permission"])
                log.info(
                    "Successfully updated read permissions for repository",
                    extra={
                        "repoid": repo.repoid,
                        "ownerid": member.ownerid,
                        "github_webhook_event": self.event,
                    },
                )
            except (ValueError, AttributeError):
                log.info(
                    "Member didn't have read permissions, didn't update",
                    extra={
                        "repoid": repo.repoid,
                        "ownerid": member.ownerid,
                        "github_webhook_event": self.event,
                    },
                )

        return Response()

    def post(self, request, *args, **kwargs):
        self.event = self.request.META.get(GitHubHTTPHeaders.EVENT)
        log.info(
            "GitHub Webhook Handler invoked",
            extra={
                "github_webhook_event": self.event,
                "delivery": self.request.META.get(GitHubHTTPHeaders.DELIVERY_TOKEN),
            },
        )
        self.validate_signature(request)

        if handler := getattr(self, self.event, None):
            self._inc_recv()
            return handler(request, *args, **kwargs)
        else:
            self._inc_err("unhandled_event")
            return self.unhandled_webhook_event(request, *args, **kwargs)


class GithubEnterpriseWebhookHandler(GithubWebhookHandler):
    service_name = "github_enterprise"
