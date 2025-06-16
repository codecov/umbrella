#!/bin/bash

# Editably install `shared` until we properly move to repo-relative imports and
# get rid of Python packaging.
pip install -e /app/libs/shared

# Install a hot-reload utility
pip install watchdog[watchmedo]
PATH="~/.local/bin:$PATH"

# TODO: Deduplicate this logic with worker's actual startup script
if [ -n "$PROMETHEUS_MULTIPROC_DIR" ]; then
    rm -r "$PROMETHEUS_MULTIPROC_DIR" 2> /dev/null
    mkdir "$PROMETHEUS_MULTIPROC_DIR"
fi

# TODO: Deduplicate this logic with worker's actual startup script
queues=""
if [ "$CODECOV_WORKER_QUEUES" ]; then
  queues="--queue $CODECOV_WORKER_QUEUES"
fi

python manage.py migrate
python manage.py migrate --database "timeseries"
python migrate_timeseries.py

# Auto-restart worker when Python files change.
watchmedo auto-restart \
    --directory /app/apps/worker \
    --directory /app/libs/shared \
    --recursive \
    --patterns="*.py" \
    --ignore-patterns="**/tests/**" \
    --signal=SIGTERM \
    python main.py worker ${queues}
