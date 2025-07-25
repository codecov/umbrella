import logging
from collections.abc import Iterable
from datetime import datetime, timedelta

from celery import Celery, chain, group, signature
from pydantic import BaseModel
from sentry_sdk import set_tag

from core.models import Repository
from services.task.task_router import route_task
from shared import celery_config
from shared.django_apps.upload_breadcrumbs.models import BreadcrumbData
from shared.utils.pydantic_serializer import PydanticModelDump, register_preserializer
from shared.utils.sentry import current_sentry_trace_id
from timeseries.models import Dataset, MeasurementName

register_preserializer(PydanticModelDump)(BaseModel)
celery_app = Celery("tasks")
celery_app.config_from_object("shared.celery_config:BaseCeleryConfig")

log = logging.getLogger(__name__)


class TaskService:
    def _create_signature(self, name, args=None, kwargs=None, immutable=False):
        """
        Create Celery signature
        """
        queue_and_config = route_task(name, args=args, kwargs=kwargs)
        queue_name = queue_and_config["queue"]
        extra_config = queue_and_config.get("extra_config", {})
        celery_compatible_config = {
            "time_limit": extra_config.get("hard_timelimit", None),
            "soft_time_limit": extra_config.get("soft_timelimit", None),
        }
        headers = {"created_timestamp": datetime.now().isoformat()}
        set_tag("celery.queue", queue_name)
        return signature(
            name,
            args=args,
            kwargs=kwargs,
            app=celery_app,
            queue=queue_name,
            headers=headers,
            immutable=immutable,
            **celery_compatible_config,
        )

    def schedule_task(self, task_name, *, kwargs, apply_async_kwargs):
        return self._create_signature(
            task_name,
            kwargs=kwargs,
        ).apply_async(**apply_async_kwargs)

    def compute_comparison(self, comparison_id):
        self._create_signature(
            celery_config.compute_comparison_task_name,
            kwargs={"comparison_id": comparison_id},
        ).apply_async()

    def compute_comparisons(self, comparison_ids: list[int]):
        """
        Enqueue a batch of comparison tasks using a Celery group
        """
        if len(comparison_ids) > 0:
            queue_and_config = route_task(
                celery_config.compute_comparison_task_name,
                args=None,
                kwargs={"comparison_id": comparison_ids[0]},
            )
            celery_compatible_config = {
                "queue": queue_and_config["queue"],
                "time_limit": queue_and_config.get("extra_config", {}).get(
                    "hard_timelimit", None
                ),
                "soft_time_limit": queue_and_config.get("extra_config", {}).get(
                    "soft_timelimit", None
                ),
            }
            signatures = [
                signature(
                    celery_config.compute_comparison_task_name,
                    args=None,
                    kwargs={"comparison_id": comparison_id},
                    app=celery_app,
                    **celery_compatible_config,
                )
                for comparison_id in comparison_ids
            ]
            for comparison_id in comparison_ids:
                # log each separately so it can be filtered easily in the logs
                log.info(
                    "Triggering compute comparison task",
                    extra={"comparison_id": comparison_id},
                )
            group(signatures).apply_async()

    def status_set_pending(self, repoid, commitid, branch, on_a_pull_request):
        self._create_signature(
            celery_config.status_set_pending_task_name,
            kwargs={
                "repoid": repoid,
                "commitid": commitid,
                "branch": branch,
                "on_a_pull_request": on_a_pull_request,
            },
        ).apply_async()

    def upload_signature(
        self,
        repoid,
        commitid,
        report_type=None,
        arguments=None,
        debug=False,
        rebuild=False,
        immutable=False,
    ):
        return self._create_signature(
            celery_config.upload_task_name,
            kwargs={
                "repoid": repoid,
                "commitid": commitid,
                "report_type": report_type,
                "arguments": arguments,
                "debug": debug,
                "rebuild": rebuild,
            },
            immutable=immutable,
        )

    def upload(
        self,
        repoid,
        commitid,
        report_type=None,
        arguments=None,
        countdown=0,
        debug=False,
        rebuild=False,
    ):
        return self.upload_signature(
            repoid,
            commitid,
            report_type=report_type,
            arguments=arguments,
            debug=debug,
            rebuild=rebuild,
        ).apply_async(countdown=countdown)

    def upload_breadcrumb(
        self,
        commit_sha: str,
        repo_id: int,
        breadcrumb_data: BreadcrumbData,
        upload_ids: list[str] = [],
    ):
        return self._create_signature(
            celery_config.upload_breadcrumb_task_name,
            kwargs={
                "commit_sha": commit_sha,
                "repo_id": repo_id,
                "breadcrumb_data": breadcrumb_data,
                "upload_ids": upload_ids,
                "sentry_trace_id": current_sentry_trace_id(),
            },
        ).apply_async()

    def notify_signature(self, repoid, commitid, current_yaml=None, empty_upload=None):
        return self._create_signature(
            celery_config.notify_task_name,
            kwargs={
                "repoid": repoid,
                "commitid": commitid,
                "current_yaml": current_yaml,
                "empty_upload": empty_upload,
            },
        )

    def notify(self, repoid, commitid, current_yaml=None, empty_upload=None):
        self.notify_signature(
            repoid, commitid, current_yaml=current_yaml, empty_upload=empty_upload
        ).apply_async()

    def pulls_sync(self, repoid, pullid):
        self._create_signature(
            celery_config.pulls_task_name, kwargs={"repoid": repoid, "pullid": pullid}
        ).apply_async()

    def refresh(
        self,
        ownerid,
        username,
        sync_teams=True,
        sync_repos=True,
        using_integration=False,
        manual_trigger=False,
        repos_affected: list[tuple[str, str]] | None = None,
    ):
        """
        Send sync_teams and/or sync_repos task message
        If running both tasks on new worker, we create a chain with sync_teams to run
        first so that when sync_repos starts it has the most up to date teams/groups
        data for the user. Otherwise, we may miss some repos.
        """
        chain_to_call = []
        if sync_teams:
            chain_to_call.append(
                self._create_signature(
                    celery_config.sync_teams_task_name,
                    kwargs={
                        "ownerid": ownerid,
                        "username": username,
                        "using_integration": using_integration,
                    },
                )
            )

        if sync_repos:
            chain_to_call.append(
                self._create_signature(
                    celery_config.sync_repos_task_name,
                    kwargs={
                        "ownerid": ownerid,
                        "username": username,
                        "using_integration": using_integration,
                        "manual_trigger": manual_trigger,
                        "repository_service_ids": repos_affected,
                    },
                )
            )

        return chain(*chain_to_call).apply_async()

    def sync_plans(self, sender=None, account=None, action=None):
        self._create_signature(
            celery_config.ghm_sync_plans_task_name,
            kwargs={"sender": sender, "account": account, "action": action},
        ).apply_async()

    def delete_owner(self, ownerid):
        log.info(f"Triggering delete_owner task for owner: {ownerid}")
        self._create_signature(
            celery_config.delete_owner_task_name, kwargs={"ownerid": ownerid}
        ).apply_async()

    def backfill_repo(
        self,
        repository: Repository,
        start_date: datetime,
        end_date: datetime,
        dataset_names: Iterable[str] | None = None,
    ):
        log.info(
            "Triggering timeseries backfill tasks for repo",
            extra={
                "repoid": repository.pk,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "dataset_names": dataset_names,
            },
        )

        # This controls the batch size for the task - we'll backfill
        # measurements 10 days at a time in this case.  I picked this
        # somewhat arbitrarily - we might need to tweak to see what's
        # most appropriate.
        delta = timedelta(days=10)

        signatures = []

        task_end_date = end_date
        while task_end_date > start_date:
            task_start_date = task_end_date - delta
            if task_start_date < start_date:
                task_start_date = start_date

            kwargs = {
                "repoid": repository.pk,
                "start_date": task_start_date.isoformat(),
                "end_date": task_end_date.isoformat(),
            }
            if dataset_names is not None:
                kwargs["dataset_names"] = dataset_names

            signatures.append(
                self._create_signature(
                    celery_config.timeseries_backfill_task_name,
                    kwargs=kwargs,
                )
            )

            task_end_date = task_start_date

        group(signatures).apply_async()

    def backfill_dataset(
        self,
        dataset: Dataset,
        start_date: datetime,
        end_date: datetime,
    ):
        log.info(
            "Triggering dataset backfill",
            extra={
                "dataset_id": dataset.pk,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            },
        )

        self._create_signature(
            celery_config.timeseries_backfill_dataset_task_name,
            kwargs={
                "dataset_id": dataset.pk,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            },
        ).apply_async()

    def transplant_report(self, repo_id: int, from_sha: str, to_sha: str) -> None:
        self._create_signature(
            celery_config.transplant_report_task_name,
            kwargs={"repo_id": repo_id, "from_sha": from_sha, "to_sha": to_sha},
        ).apply_async()

    def update_commit(self, commitid, repoid):
        self._create_signature(
            celery_config.commit_update_task_name,
            kwargs={"commitid": commitid, "repoid": repoid},
        ).apply_async()

    def http_request(self, url, method="POST", headers=None, data=None, timeout=None):
        self._create_signature(
            celery_config.http_request_task_name,
            kwargs={
                "url": url,
                "method": method,
                "headers": headers,
                "data": data,
                "timeout": timeout,
            },
        ).apply_async()

    def flush_repo(self, repository_id: int):
        self._create_signature(
            celery_config.flush_repo_task_name,
            kwargs={"repoid": repository_id},
        ).apply_async()

    def manual_upload_completion_trigger(self, repoid, commitid, current_yaml=None):
        self._create_signature(
            celery_config.manual_upload_completion_trigger_task_name,
            kwargs={
                "commitid": commitid,
                "repoid": repoid,
                "current_yaml": current_yaml,
            },
        ).apply_async()

    def preprocess_upload(self, repoid, commitid):
        self._create_signature(
            celery_config.pre_process_upload_task_name,
            kwargs={"repoid": repoid, "commitid": commitid},
        ).apply_async()

    def send_email(
        self,
        to_addr: str,
        subject: str,
        template_name: str,
        from_addr: str | None = None,
        **kwargs,
    ):
        # Templates can be found in worker/templates
        self._create_signature(
            celery_config.send_email_task_name,
            kwargs=dict(
                to_addr=to_addr,
                subject=subject,
                template_name=template_name,
                from_addr=from_addr,
                **kwargs,
            ),
        ).apply_async()

    def delete_component_measurements(self, repoid: int, component_id: str) -> None:
        log.info(
            "Delete component measurements data",
            extra={"repository_id": repoid, "component_id": component_id},
        )
        self._create_signature(
            celery_config.timeseries_delete_task_name,
            kwargs={
                "repository_id": repoid,
                "measurement_only": True,
                "measurement_type": MeasurementName.COMPONENT_COVERAGE.value,
                "measurement_id": component_id,
            },
        ).apply_async()

    def cache_test_results_redis(self, repoid: int, branch: str) -> None:
        self._create_signature(
            celery_config.cache_test_rollups_redis_task_name,
            kwargs={"repoid": repoid, "branch": branch},
        ).apply_async()
