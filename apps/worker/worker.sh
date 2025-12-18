#!/usr/bin/env bash
set -euo pipefail

# Default entrypoint for worker
echo "Starting worker"

# Script section to keep in sync with api.sh
#### Start ####

# Optional prefix and suffix for all python commands
pre="${CODECOV_WRAPPER:-}"
post="${CODECOV_WRAPPER_POST:-}"

# Whether to ignore the prefix and suffix on migration commands
if [[ -n "${CODECOV_WRAPPER_IGNORE_MIGRATE:-}" ]]; then
  pre_migrate=""
  post_migrate=""
else
  pre_migrate="$pre"
  post_migrate="$post"
fi

# Berglas is used to manage secrets in GCP.
berglas=""
if [[ -f "/usr/local/bin/berglas" ]]; then
  berglas="berglas exec --"
fi

#### End ####

if [[ -n "${PROMETHEUS_MULTIPROC_DIR:-}" ]]; then
  rm -r "${PROMETHEUS_MULTIPROC_DIR:?}"/* 2>/dev/null || true
  mkdir -p "$PROMETHEUS_MULTIPROC_DIR"
else
  # Set a default prometheus directory to prevent cleanup errors during shutdown
  export PROMETHEUS_MULTIPROC_DIR="${HOME}/.prometheus"
  mkdir -p "$PROMETHEUS_MULTIPROC_DIR"
fi

queues=""
if [[ -n "${CODECOV_WORKER_QUEUES:-}" ]]; then
  queues="--queue $CODECOV_WORKER_QUEUES"
fi

if [[ "${CODECOV_SKIP_MIGRATIONS:-}" != "true" && ("${RUN_ENV:-}" = "ENTERPRISE" || "${RUN_ENV:-}" = "DEV") ]]; then
  echo "Running migrations"
  $pre_migrate $berglas python manage.py migrate $post_migrate
  $pre_migrate $berglas python migrate_timeseries.py $post_migrate
  $pre_migrate $berglas python manage.py pgpartition --yes --skip-delete $post_migrate
fi

# Signal handling for graceful shutdown
# Forward SIGTERM/SIGINT to the worker process and let Celery handle graceful shutdown
shutdown() {
  echo "Received shutdown signal, forwarding to worker process..."
  if [[ -n "${worker_pid:-}" ]]; then
    # Forward the signal to the worker process
    kill -TERM "$worker_pid" 2>/dev/null || true
    # Wait for the worker to exit (Celery will handle graceful shutdown)
    wait "$worker_pid" 2>/dev/null || true
  fi
  exit 0
}

# Trap SIGTERM and SIGINT
trap shutdown SIGTERM SIGINT

if [[ -z "${1:-}" ]]; then
  # Start the worker in the background and capture its PID
  $pre $berglas python main.py worker $queues $post &
  worker_pid=$!
  echo "Worker started with PID $worker_pid"
  
  # Wait for the background process
  wait "$worker_pid"
else
  exec "$@"
fi
