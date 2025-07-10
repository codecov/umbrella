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

if [[ "${RUN_ENV:-}" = "ENTERPRISE" ]] || [[ "${RUN_ENV:-}" = "DEV" ]]; then
  echo "Running migrations"
  $pre_migrate $berglas python manage.py migrate $post_migrate
  $pre_migrate $berglas python migrate_timeseries.py $post_migrate
  $pre_migrate $berglas python manage.py pgpartition --yes --skip-delete $post_migrate
fi

if [[ -z "${1:-}" ]]; then
  $pre $berglas python main.py worker $queues $post
else
  exec "$@"
fi
