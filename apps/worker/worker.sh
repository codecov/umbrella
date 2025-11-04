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
shutdown() {
  echo "Received SIGTERM, shutting down gracefully..."
  if [[ -n "${worker_pid:-}" ]]; then
    # Send SIGTERM to the worker process
    kill -TERM "$worker_pid" 2>/dev/null || true
    
    # Wait for graceful shutdown with timeout (default 25s, less than K8s terminationGracePeriodSeconds)
    local timeout=${SHUTDOWN_TIMEOUT:-25}
    local count=0
    
    while kill -0 "$worker_pid" 2>/dev/null && [[ $count -lt $timeout ]]; do
      sleep 1
      ((count++))
    done
    
    # If process is still running, force kill
    if kill -0 "$worker_pid" 2>/dev/null; then
      echo "Worker did not shutdown gracefully after ${timeout}s, forcing shutdown..."
      kill -KILL "$worker_pid" 2>/dev/null || true
      wait "$worker_pid" 2>/dev/null || true
    else
      echo "Worker shutdown complete"
    fi
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
