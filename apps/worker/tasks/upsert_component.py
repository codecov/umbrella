import logging

from sqlalchemy.orm import Session

from app import celery_app
from database.models import Commit
from services.report import ReportService
from services.timeseries import ComponentForMeasurement, upsert_components_measurements
from services.yaml import get_repo_yaml
from shared.celery_config import timeseries_upsert_component_task_name
from shared.reports.readonly import ReadOnlyReport
from tasks.base import BaseCodecovTask

log = logging.getLogger(__name__)


class UpsertComponentTask(BaseCodecovTask, name=timeseries_upsert_component_task_name):
    def run_impl(
        self,
        db_session: Session,
        commitid: str,
        repoid: int,
        component_id: str,
        flags: list[str],
        paths: list[str],
        *args,
        **kwargs,
    ):
        log.info("Upserting component", extra={"commitid": commitid, "repoid": repoid})

        commit = (
            db_session.query(Commit)
            .filter(Commit.repoid == repoid, Commit.commitid == commitid)
            .first()
        )

        current_yaml = get_repo_yaml(commit.repository)
        report_service = ReportService(current_yaml)
        report = report_service.get_existing_report_for_commit(
            commit, report_class=ReadOnlyReport
        )
        assert report, "expected a `Report` to exist"

        upsert_components_measurements(
            commit, report, [ComponentForMeasurement(component_id, flags, paths)]
        )


registered_task = celery_app.register_task(UpsertComponentTask())
upsert_component_task = celery_app.tasks[registered_task.name]
