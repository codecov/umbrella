#!/usr/bin/env bash
set -euo pipefail

# Default entrypoint for api
echo "Starting api"

# Script section to keep in sync with worker.sh
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

GUNICORN_WORKERS=${GUNICORN_WORKERS:-1}
if [[ "$GUNICORN_WORKERS" -gt 1 ]]; then
  export PROMETHEUS_MULTIPROC_DIR="${PROMETHEUS_MULTIPROC_DIR:-$HOME/.prometheus}"
  rm -r "${PROMETHEUS_MULTIPROC_DIR:?}"/* 2>/dev/null || true
  mkdir -p "$PROMETHEUS_MULTIPROC_DIR"
fi

statsd=""
if [[ -n "${STATSD_HOST:-}" ]]; then
  statsd="--statsd-host ${STATSD_HOST:?}:${STATSD_PORT:?}"
fi

if [[ "${RUN_ENV:-}" = "ENTERPRISE" ]] || [[ "${RUN_ENV:-}" = "DEV" ]]; then
  echo "Running migrations"
  $pre_migrate $berglas python manage.py migrate $post_migrate
  $pre_migrate $berglas python manage.py migrate --database "timeseries" $post_migrate
  $pre_migrate $berglas python manage.py pgpartition --yes --skip-delete $post_migrate
fi

if [[ "${RUN_ENV:-}" = "STAGING" ]] || [[ "${RUN_ENV:-}" = "DEV" ]]; then
  export PYTHONWARNINGS=always
fi

if [[ -z "${1:-}" ]]; then
  added_args=""

  case "${RUN_ENV:-}" in
  "PROD")
    echo "Starting gunicorn in production mode"
    added_args="--disable-redirect-access-to-syslog --config=gunicorn.conf.py --max-requests=50000 --max-requests-jitter=300"
    ;;
  "STAGING")
    echo "Starting gunicorn in staging mode"
    added_args="--disable-redirect-access-to-syslog --config=gunicorn.conf.py"
    ;;
  *)
    echo "Starting gunicorn in default mode"
    ;;
  esac
  $pre $berglas gunicorn codecov.wsgi:application \
    $added_args \
    $statsd \
    --workers="$GUNICORN_WORKERS" \
    --threads="${GUNICORN_THREADS:-1}" \
    --worker-connections="${GUNICORN_WORKER_CONNECTIONS:-1000}" \
    --bind "${CODECOV_API_BIND:-0.0.0.0}":"${CODECOV_API_PORT:-8000}" \
    --access-logfile '-' \
    --timeout "${GUNICORN_TIMEOUT:-600}" \
    $post
else
  echo "Executing custom command"
  exec "$@"
fi
