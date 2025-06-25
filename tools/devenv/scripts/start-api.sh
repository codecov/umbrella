#!/bin/bash

# Editably install `shared` until we properly move to repo-relative imports and
# get rid of Python packaging.
pip install -e /app/libs/shared

# Install a hot-reload utility. gunicorn and `manage.py runserver` both have
# their own but they don't support watching other directories.
pip install watchdog[watchmedo]
PATH="~/.local/bin:$PATH"

if [ -n "$PROMETHEUS_MULTIPROC_DIR" ]; then
    rm -r ${PROMETHEUS_MULTIPROC_DIR?}/* 2> /dev/null
    mkdir -p "$PROMETHEUS_MULTIPROC_DIR"
fi

export PYTHONWARNINGS=always
if [ "$RUN_ENV" == "ENTERPRISE" ] || [ "$RUN_ENV" == "DEV" ]; then
    python manage.py migrate
    python manage.py migrate --database "timeseries" timeseries
    python manage.py pgpartition --yes
    python manage.py insert_data_to_db_from_csv core/management/commands/codecovTiers-Jan25.csv --model tiers
    python manage.py insert_data_to_db_from_csv core/management/commands/codecovPlans-Jan25.csv --model plans
fi

# Auto-restart codecov-api when Python files change.
watchmedo auto-restart \
    --directory /app/apps/codecov-api \
    --directory /app/libs/shared \
    --recursive \
    --patterns="*.py" \
    --ignore-patterns="**/tests/**" \
    --signal=SIGTERM \
    python manage.py runserver 0.0.0.0:8000
