import logging

from sqlalchemy.orm import Session

from app import celery_app
from database.models import CompareCommit, CompareFlag
from database.models.reports import RepositoryFlag
from helpers.github_installation import get_installation_name_for_owner_for_task
from services.comparison_utils import get_comparison_proxy
from services.report import ReportService
from services.yaml import get_repo_yaml
from shared.celery_config import compute_flag_comparison_task_name
from tasks.base import BaseCodecovTask

log = logging.getLogger(__name__)


class ComputeFlagComparisonTask(BaseCodecovTask, name=compute_flag_comparison_task_name):
    def run_impl(
        self,
        db_session: Session,
        comparison_id: int,
        flag_name: str,
        *args,
        **kwargs,
    ):
        comparison: CompareCommit = db_session.query(CompareCommit).get(comparison_id)
        repo = comparison.compare_commit.repository

        log_extra = {
            "comparison_id": comparison_id,
            "repoid": repo.repoid,
            "commit": comparison.compare_commit.commitid,
            "flag_name": flag_name,
        }
        log.info("Computing flag comparison", extra=log_extra)

        current_yaml = get_repo_yaml(repo)
        installation_name_to_use = get_installation_name_for_owner_for_task(
            self.name, repo.author
        )
        report_service = ReportService(
            current_yaml, gh_app_installation_name=installation_name_to_use
        )
        comparison_proxy = get_comparison_proxy(comparison, report_service)

        # Compute totals for this single flag
        flag_head_report = comparison_proxy.comparison.head.report.flags.get(flag_name)
        if flag_head_report is None:
            log.warning(
                "Flag not found in head report; skipping",
                extra=log_extra,
            )
            return {"successful": False, "error": "flag_not_in_head_report"}

        flag_base_report = (
            comparison_proxy.comparison.project_coverage_base.report.flags.get(
                flag_name
            )
        )
        head_totals = flag_head_report.totals.asdict()
        base_totals = None if not flag_base_report else flag_base_report.totals.asdict()
        patch_totals = None
        diff = comparison_proxy.get_diff()
        if diff:
            computed_patch = flag_head_report.apply_diff(diff)
            if computed_patch:
                patch_totals = computed_patch.asdict()

        totals = {
            "head_totals": head_totals,
            "base_totals": base_totals,
            "patch_totals": patch_totals,
        }

        # Look up (or create) the RepositoryFlag row
        repository_id = repo.repoid
        repository_flag = (
            db_session.query(RepositoryFlag)
            .filter_by(repository_id=repository_id, flag_name=flag_name)
            .first()
        )
        if not repository_flag:
            repository_flag = RepositoryFlag(
                repository_id=repository_id,
                flag_name=flag_name,
            )
            db_session.add(repository_flag)
            db_session.flush()

        # Upsert the CompareFlag row
        flag_comparison = (
            db_session.query(CompareFlag)
            .filter_by(
                commit_comparison_id=comparison.id,
                repositoryflag_id=repository_flag.id,
            )
            .first()
        )
        if flag_comparison:
            flag_comparison.head_totals = totals["head_totals"]
            flag_comparison.base_totals = totals["base_totals"]
            flag_comparison.patch_totals = totals["patch_totals"]
        else:
            flag_comparison = CompareFlag(
                commit_comparison=comparison,
                repositoryflag=repository_flag,
                patch_totals=totals["patch_totals"],
                head_totals=totals["head_totals"],
                base_totals=totals["base_totals"],
            )
            db_session.add(flag_comparison)
        db_session.flush()

        log.info("Finished computing flag comparison", extra=log_extra)
        return {"successful": True}


RegisteredComputeFlagComparisonTask = celery_app.register_task(
    ComputeFlagComparisonTask()
)
compute_flag_comparison_task = celery_app.tasks[
    RegisteredComputeFlagComparisonTask.name
]