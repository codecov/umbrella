#!/bin/bash

# things to check
# - django settings
# - conftest.py
# - pytest.ini
# - django installed_apps
# - django middleware
# - django db routers
# - worker "templates"
# - ruff check/format

# need to update
# - entrypoints in makefiles and the like
# - top-level manage.py that selects the right settings module
#   - maybe a top-level settings module that routes to worker/shared/api/test settings depending on env...

# Make a PR in worker/shared/api that fix abnormal module references so that they will be automatable
# example:
#   libs/shared/shared/api_archive/archive.py:68:24: F821 Undefined name `shared`
#     self.storage = shared.storage.get_appropriate_storage_service(
#                    ^^^^^^ F821

# Add `apps.worker` to every worker module import path
worker_module_regex="(database|django_scaffold|enterprise|helpers|rollouts|services|ta_storage|tasks|templates|test_utils|tests|app|celery_config|celery_task_router)"
find apps/worker -type f -name '*.py' -not -path '*.venv*' -exec sed -E -i '' "s/^(import|from) $worker_module_regex/\\1 apps.worker.\\2/" {} +

# Add `apps.codecov_api` to every api module import path
api_module_regex="(api|billing|codecov|codecov_auth|compare|core|docker|graphql_api|graphs|label_analysis|legacy_migrations|reports|rollouts|services|staticanalysis|templates|timeseries|upload|utils|validate|webhook_handlers)"
find apps/codecov-api -type f -name '*.py' -not -path '*.venv*' -exec sed -E -i '' "s/^(import|from) $api_module_regex/\\1 apps.codecov_api.\\2/" {} +

# Add `libs.shared` to every shared import path
find . -type f -name '*.py' -not -path '*.venv*' -exec sed -E -i '' "s/^(import|from) shared/\\1 libs.shared.shared/" {} +

ruff format
ruff check --fix
