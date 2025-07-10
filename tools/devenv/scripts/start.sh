#!/usr/bin/env bash
set -euo pipefail

app=""
if [[ "$1" == "codecov-api" ]]; then
  app="codecov-api"
elif [[ "$1" == "worker" ]]; then
  app="worker"
else
  echo "Usage: $0 <codecov-api|worker>"
  exit 1
fi

# Editably install `shared` until we properly move to repo-relative imports and
# get rid of Python packaging.
pip install -e /app/libs/shared

# Install a hot-reload utility. gunicorn and `manage.py runserver` both have
# their own but they don't support watching other directories.
pip install watchdog[watchmedo]
export PATH="~/.local/bin:$PATH"

# Auto-restart the specified app when Python files change.
export CODECOV_WRAPPER="watchmedo auto-restart \
  --directory /app/apps/$app \
  --directory /app/libs/shared \
  --recursive \
  --patterns='*.py' \
  --ignore-patterns='**/tests/**' \
  --signal=SIGTERM"
export CODECOV_WRAPPER_IGNORE_MIGRATE=true

# Start the specified app.
case "$app" in
codecov-api)
  # We override the run command to be able to execute our custom commands before starting the server
  bash "$ENTRYPOINT" python manage.py insert_data_to_db_from_csv core/management/commands/codecovTiers-Jan25.csv --model tiers &&
    python manage.py insert_data_to_db_from_csv core/management/commands/codecovPlans-Jan25.csv --model plans &&
    $CODECOV_WRAPPER python manage.py runserver 0.0.0.0:8000
  ;;
worker)
  bash "$ENTRYPOINT"
  ;;
*)
  echo "Unknown app: $app"
  exit 1
  ;;
esac
