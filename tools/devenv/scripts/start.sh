#!/usr/bin/env bash
set -euo pipefail

# Signal handling for graceful shutdown
shutdown_in_progress=false
shutdown() {
  # Prevent duplicate shutdown attempts (in case both shell and process receive signal)
  if [[ "$shutdown_in_progress" == "true" ]]; then
    return 0
  fi
  shutdown_in_progress=true
  
  echo "Received SIGTERM, shutting down gracefully..."
  if [[ -n "${app_pid:-}" ]]; then
    # Send SIGTERM to the app process
    kill -TERM "$app_pid" 2>/dev/null || true
    
    # Wait for graceful shutdown with timeout
    local timeout=${SHUTDOWN_TIMEOUT:-25}
    local count=0
    
    while kill -0 "$app_pid" 2>/dev/null && [[ $count -lt $timeout ]]; do
      sleep 1
      ((count++))
    done
    
    # If process is still running, force kill
    if kill -0 "$app_pid" 2>/dev/null; then
      echo "App did not shutdown gracefully after ${timeout}s, forcing shutdown..."
      kill -KILL "$app_pid" 2>/dev/null || true
      wait "$app_pid" 2>/dev/null || true
    else
      echo "App shutdown complete"
    fi
  fi
  exit 0
}

# Trap SIGTERM and SIGINT
trap shutdown SIGTERM SIGINT

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
  (
    bash "$ENTRYPOINT" python manage.py insert_data_to_db_from_csv core/management/commands/codecovTiers-Jan25.csv --model tiers &&
      python manage.py insert_data_to_db_from_csv core/management/commands/codecovPlans-Jan25.csv --model plans &&
      $CODECOV_WRAPPER python manage.py runserver 0.0.0.0:8000
  ) &
  app_pid=$!
  echo "codecov-api started with PID $app_pid"
  ;;
worker)
  bash "$ENTRYPOINT" &
  app_pid=$!
  echo "worker started with PID $app_pid"
  ;;
*)
  echo "Unknown app: $app"
  exit 1
  ;;
esac

# Wait for the app process
wait "$app_pid"
