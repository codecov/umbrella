# Experimental Test Analytics Tasks

This module contains an experimental pipeline that mirrors an aspirational Test Analytics (TA) flow. The code is **not** production-ready—each task exists solely for exploration and will evolve substantially before promotion.

## Task Topology

1. `IngestTestruns`
   - Parses raw test-result uploads and writes normalized rows via the timeseries helpers.
   - Creates `TAUpload` placeholders when the repository is eligible, queueing work for downstream tasks.
2. `DetectFlakes`
   - Claims queued `TAUpload` rows and updates both `Testrun` and `Flake` state to reflect newly-detected flaky behavior.
   - Reschedules itself while the queue has pending uploads, acting as the flake-processing loop.
3. `TestAnalyticsNotifierTask`
   - Hydrates a `TestResultsNotifier` with the processed summaries and posts draft PR commentary where configured.
   - Uses fencing tokens and redis locks to deduplicate concurrent notifications.

All tasks are registered with Celery but intentionally sequestered beneath the `experimental` namespace to prevent accidental inclusion in default worker imports.

## Coordinating the Pipeline

Within `apps/codecov-api/services/task/task.py`, the `TaskService` remains the entry point for scheduling. A prospective orchestration flow might resemble:

```python
service.detect_flakes(repo_id)  # ensures the processing loop is active
service.schedule_task(
    celery_config.ingest_testruns_task_name,
    kwargs={"repoid": repo_id, "upload_context": context},
    apply_async_kwargs={"countdown": 0},
)
```

Downstream notifications would be triggered either by ingest completion hooks or explicit API routes once the ingest/detect stages succeed. As we iterate, we will introduce dedicated service methods that chain the experimental tasks; for now, invoke the Celery signatures directly via the `TaskService` helpers when running local experiments.

## Running Locally

- Ensure the worker environment has access to Redis, the Django ORM, and any storage backends referenced by the upload context.
- Because these tasks are prototypes, expect migrations or data contracts to change. Clear related test data frequently during iteration.

Please share findings in the TA pipeline RFC and surface blockers to the Data Platform crew.
