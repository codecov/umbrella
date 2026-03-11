import logging
from datetime import timedelta

from sqlalchemy import func

from app import celery_app
from celery_config import stuck_uploads_check_cron_task_name
from database.enums import ReportType
from database.models.core import Commit
from database.models.reports import CommitReport, Upload
from helpers.clock import get_utc_now
from shared.celery_config import upload_merger_task_name
from shared.helpers.redis import get_redis_connection
from shared.reports.enums import UploadState
from tasks.crontasks import CodecovCronTask
from tasks.upload_merger import MERGER_GATE_TTL, merger_gate_key

log = logging.getLogger(__name__)

STUCK_THRESHOLD_MINUTES = 15


class StuckUploadsCheckTask(CodecovCronTask, name=stuck_uploads_check_cron_task_name):
    @classmethod
    def get_min_seconds_interval_between_executions(cls):
        return 3300  # 55 minutes

    def run_cron_task(self, db_session, *args, **kwargs):
        cutoff = get_utc_now() - timedelta(minutes=STUCK_THRESHOLD_MINUTES)

        stuck_commits = (
            db_session.query(
                CommitReport.commit_id,
                Commit.repoid,
                Commit.commitid,
                func.count(Upload.id_).label("stuck_count"),
            )
            .join(CommitReport, Upload.report_id == CommitReport.id_)
            .join(Commit, CommitReport.commit_id == Commit.id_)
            .filter(
                Upload.state_id == UploadState.PROCESSED.db_id,
                Upload.updated_at <= cutoff,
                (CommitReport.report_type == None)  # noqa: E711
                | (CommitReport.report_type == ReportType.COVERAGE.value),
            )
            .group_by(CommitReport.commit_id, Commit.repoid, Commit.commitid)
            .all()
        )

        if not stuck_commits:
            log.info("No stuck uploads found")
            return {"stuck_commits": 0, "triggered": 0}

        redis = get_redis_connection()
        triggered = 0

        for row in stuck_commits:
            log.error(
                "Stuck uploads detected, triggering merger",
                extra={
                    "repoid": row.repoid,
                    "commitid": row.commitid,
                    "stuck_count": row.stuck_count,
                    "threshold_minutes": STUCK_THRESHOLD_MINUTES,
                },
            )

            gate_key = merger_gate_key(row.repoid, row.commitid)
            if redis.set(gate_key, "1", nx=True, ex=MERGER_GATE_TTL):
                self.app.tasks[upload_merger_task_name].apply_async(
                    kwargs={
                        "repoid": row.repoid,
                        "commitid": row.commitid,
                        "commit_yaml": {},
                        "trigger": "cron",
                    }
                )
                triggered += 1

        log.info(
            "Stuck uploads check complete",
            extra={
                "stuck_commits": len(stuck_commits),
                "triggered": triggered,
            },
        )
        return {"stuck_commits": len(stuck_commits), "triggered": triggered}


RegisteredStuckUploadsCheckTask = celery_app.register_task(StuckUploadsCheckTask())
stuck_uploads_check_task = celery_app.tasks[RegisteredStuckUploadsCheckTask.name]
