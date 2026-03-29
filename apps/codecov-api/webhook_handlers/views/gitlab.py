import logging

from django.http import HttpRequest
from django.shortcuts import get_object_or_404
from django.utils.crypto import constant_time_compare
from rest_framework.exceptions import PermissionDenied
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView

from codecov_auth.models import Owner
from core.models import Commit, Pull, PullStates, Repository
from services.refresh import RefreshService
from services.task import TaskService
from utils.config import get_config
from webhook_handlers.constants import (
    GitLabHTTPHeaders,
    GitLabWebhookEvents,
    WebhookHandlerErrorMessages,
)

from . import WEBHOOKS_ERRORED, WEBHOOKS_RECEIVED

log = logging.getLogger(__name__)


class GitLabWebhookHandler(APIView):
    permission_classes = [AllowAny]
    service_name = "gitlab"

    def _inc_recv(self):
        event_name = self.request.data.get("event_name")
        if not event_name:
            event_name = self.request.data.get("object_kind")
        action = self.request.data.get("object_attributes", {}).get("action", "")

        WEBHOOKS_RECEIVED.labels(
            service=self.service_name, event=event_name, action=action
        ).inc()

    def _inc_err(self, reason: str):
        event_name = self.request.data.get("event_name")
        if not event_name:
            event_name = self.request.data.get("object_kind")
        action = self.request.data.get("object_attributes", {}).get("action", "")

        WEBHOOKS_ERRORED.labels(
            service=self.service_name,
            event=event_name,
            action=action,
            error_reason=reason,
        ).inc()

    def _get_owner_namespace(self, request) -> str | None:
        """Extract the owner namespace from the webhook payload.

        Different GitLab event types place this in different fields:
        - Push/MR/Pipeline events: project.path_with_namespace ("namespace/repo")
        - System hooks: path_with_namespace ("namespace/repo")
        - Job hooks: project_name ("namespace/repo" or "namespace / repo")

        See: https://docs.gitlab.com/ee/user/project/integrations/webhook_events.html
        """
        path_with_namespace = request.data.get("project", {}).get(
            "path_with_namespace"
        ) or request.data.get("path_with_namespace")

        if path_with_namespace:
            parts = path_with_namespace.split("/")
            if len(parts) >= 2:
                return parts[0].strip()

        project_name = request.data.get("project_name")
        if project_name:
            parts = project_name.split("/")
            if len(parts) >= 2:
                return parts[0].strip()

        return None

    def _get_repo_from_webhook(self, request, project_id) -> Repository:
        """Look up the Repository for the given webhook, disambiguating by owner
        namespace when multiple repos share the same service_id."""
        base_filter = {
            "author__service": self.service_name,
            "service_id": project_id,
        }
        owner_namespace = self._get_owner_namespace(request)

        if owner_namespace:
            try:
                return Repository.objects.get(
                    **base_filter, author__username=owner_namespace
                )
            except Repository.DoesNotExist:
                pass
            except Repository.MultipleObjectsReturned:
                log.warning(
                    "Multiple repos found even with namespace filter",
                    extra={
                        "service_id": project_id,
                        "namespace": owner_namespace,
                    },
                )

        return get_object_or_404(Repository, **base_filter)

    def post(self, request, *args, **kwargs):
        """
        Helpful docs for working with GitLab webhooks
        https://docs.gitlab.com/ee/user/project/integrations/webhooks.html#webhook-receiver-requirements
        for those special system hooks: https://docs.gitlab.com/ee/administration/system_hooks.html#hooks-request-example
        all the other hooks: https://docs.gitlab.com/ee/user/project/integrations/webhook_events.html
        """
        event = self.request.META.get(GitLabHTTPHeaders.EVENT)

        log.info("GitLab webhook message received", extra={"event": event})

        project_id = request.data.get("project_id") or request.data.get(
            "object_attributes", {}
        ).get("target_project_id")

        event_name = self.request.data.get(
            "event_name", self.request.data.get("object_kind")
        )

        is_enterprise = True if get_config("setup", "enterprise_license") else False

        # special case - only event that doesn't have a repo yet
        if event_name == "project_create":
            if event == GitLabWebhookEvents.SYSTEM and is_enterprise:
                self._inc_recv()
                return self._handle_system_project_create_hook_event()
            else:
                self._inc_err("permission_denied")
                raise PermissionDenied()

        try:
            repo = self._get_repo_from_webhook(request, project_id)
        except Repository.MultipleObjectsReturned as e:
            self._inc_err("repo_multiple_found")
            raise e
        except Exception as e:
            self._inc_err("repo_not_found")
            raise e

        webhook_validation = bool(
            get_config(
                self.service_name, "webhook_validation", default=False
            )  # TODO: backfill migration then switch to True
        )
        if webhook_validation or repo.webhook_secret:
            self._validate_secret(request, repo.webhook_secret)

        if event == GitLabWebhookEvents.PUSH:
            self._inc_recv()
            return self._handle_push_event(repo)
        elif event == GitLabWebhookEvents.JOB:
            self._inc_recv()
            return self._handle_job_event(repo)
        elif event == GitLabWebhookEvents.MERGE_REQUEST:
            self._inc_recv()
            return self._handle_merge_request_event(repo)
        elif event == GitLabWebhookEvents.SYSTEM:
            # SYSTEM events have always been gated behind is_enterprise, requires an enterprise_license
            if not is_enterprise:
                self._inc_err("permission_denied")
                raise PermissionDenied()
            self._inc_recv()
            return self._handle_system_hook_event(repo, event_name)

        self._inc_err("unhandled_event")
        return Response()

    def _handle_push_event(self, repo):
        """
        Triggered when pushing to the repository except when pushing tags.

        https://docs.gitlab.com/ce/user/project/integrations/webhooks.html#push-events
        """
        message = "No yaml cached yet."
        return Response(data=message)

    def _handle_job_event(self, repo):
        """
        Triggered on status change of a job.

        This is equivalent to legacy "Build Hook" handling (https://gitlab.com/gitlab-org/gitlab-foss/issues/28226)

        https://docs.gitlab.com/ee/user/project/integrations/webhooks.html#job-events
        """
        if self.request.data.get("build_status") == "pending":
            return Response(data=WebhookHandlerErrorMessages.SKIP_PENDING_STATUSES)

        if repo.active is not True:
            return Response(data=WebhookHandlerErrorMessages.SKIP_PROCESSING)

        commitid = self.request.data.get("sha")

        try:
            commit = repo.commits.get(
                commitid=commitid, state=Commit.CommitStates.COMPLETE
            )
        except Commit.DoesNotExist:
            return Response(data=WebhookHandlerErrorMessages.SKIP_PROCESSING)

        TaskService().notify(repoid=commit.repository.repoid, commitid=commitid)
        return Response(data="Notify queued.")

    def _handle_merge_request_event(self, repo):
        """
        Triggered when a new merge request is created, an existing merge request was updated/merged/closed or
        a commit is added in the source branch.

        https://docs.gitlab.com/ce/user/project/integrations/webhooks.html#merge-request-events
        """
        repoid = repo.repoid

        pull = self.request.data.get("object_attributes", {})
        action = pull.get("action")
        message = None
        if action == "open":
            TaskService().pulls_sync(repoid=repoid, pullid=pull["iid"])
            message = "Opening pull request in Codecov"

        elif action == "close":
            Pull.objects.filter(repository__repoid=repoid, pullid=pull["iid"]).update(
                state=PullStates.CLOSED
            )
            message = "Pull request closed"

        elif action == "merge":
            TaskService().pulls_sync(repoid=repoid, pullid=pull["iid"])
            message = "Pull request merged"

        elif action == "update":
            TaskService().pulls_sync(repoid=repoid, pullid=pull["iid"])
            message = "Pull request synchronize queued"

        else:
            log.warning(
                "Unhandled Gitlab webhook merge_request action",
                extra={"action": action},
            )

        return Response(data=message)

    def _initiate_sync_for_owner(self, owner):
        """
        default: will sync_teams and sync_repos for owner
        sync_teams to update owner.organizations list (expired memberships are removed and new memberships are added),
        and username, name, email, and avatar of each Org in owner.organizations.
        sync_repos to update owner.permission list (private repo access),
        and name, language, private, repoid, and deleted=False for each repo the owner has access to.
        """
        RefreshService().trigger_refresh(
            ownerid=owner.ownerid,
            username=owner.username,
            using_integration=False,
            manual_trigger=False,
        )

    def _try_initiate_sync_for_owner(self):
        owner_email = self.request.data.get("owner_email")

        # email is a strong identifier (GL users must have a unique email)
        try:
            owner = Owner.objects.get(
                service=self.service_name,
                oauth_token__isnull=False,
                email=owner_email,
            )
        except (Owner.DoesNotExist, Owner.MultipleObjectsReturned):
            # could be the username of the OwnerUser or OwnerOrg. Sync only works with an OwnerUser.
            owner_username_best_guess = self.request.data.get(
                "path_with_namespace", ""
            ).split("/")[0]
            try:
                owner = Owner.objects.get(
                    service=self.service_name,
                    oauth_token__isnull=False,
                    username=owner_username_best_guess,
                )
            except (Owner.DoesNotExist, Owner.MultipleObjectsReturned):
                return

        self._initiate_sync_for_owner(owner)

    def _handle_system_project_create_hook_event(self):
        self._try_initiate_sync_for_owner()
        return Response(data="Sync initiated")

    def _try_initiate_sync_for_repo(self, repo):
        # most GL repos have bots - try to sync with bot as Owner
        if repo.bot:
            bot_owner = Owner.objects.filter(
                service=self.service_name,
                ownerid=repo.bot.ownerid,
                oauth_token__isnull=False,
            ).first()
            if bot_owner:
                return self._initiate_sync_for_owner(owner=bot_owner)
        self._try_initiate_sync_for_owner()

    def _handle_system_hook_event(self, repo, event_name):
        """
        GitLab Enterprise instance can send system hooks for changes on user, group, project, etc
        """
        message = None

        if event_name == "project_destroy":
            repo.deleted = True
            repo.activated = False
            repo.active = False
            repo.name = f"{repo.name}-deleted"
            repo.save(update_fields=["deleted", "activated", "active", "name"])
            message = "Repository deleted"

        elif event_name in ("project_rename", "project_transfer"):
            self._try_initiate_sync_for_repo(repo=repo)
            message = "Sync initiated"

        elif (
            event_name in ("user_add_to_team", "user_remove_from_team")
            and self.request.data.get("project_visibility") == "private"
        ):
            # the payload from these hooks includes the ownerid
            ownerid = self.request.data.get("user_id")
            user = Owner.objects.filter(
                service=self.service_name,
                service_id=ownerid,
                oauth_token__isnull=False,
            ).first()
            message = "Sync initiated"
            if user:
                self._initiate_sync_for_owner(owner=user)

        return Response(data=message)

    def _validate_secret(self, request: HttpRequest, webhook_secret: str):
        token = request.META.get(GitLabHTTPHeaders.TOKEN)
        if token and webhook_secret:
            if constant_time_compare(webhook_secret, token):
                return
        self._inc_err("validation_failed")
        raise PermissionDenied()


class GitLabEnterpriseWebhookHandler(GitLabWebhookHandler):
    service_name = "gitlab_enterprise"
