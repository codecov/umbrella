# Agent Instructions

This file provides guidance for AI agents working in this codebase.

## Project Overview

`umbrella` is Codecov's Python monorepo containing:

- `apps/codecov-api` - The Codecov API (Django)
- `apps/worker` - The Codecov worker (Celery)
- `libs/shared` - Shared library code used by both apps

## Development Environment

The project uses:
- **Python 3.13+**
- **uv** for dependency management (pyproject.toml, uv.lock)
- **Docker Compose** for local development
- **pre-commit** for code quality hooks

## Running Tests

Tests run inside Docker containers. The dev environment must be running first (`make devenv.start`).

### For humans (interactive terminal)

```bash
make devenv.test.worker    # Worker tests
make devenv.test.api       # API tests
make devenv.test.shared    # Shared library tests

# Run specific tests
make devenv.test.shared EXTRA_PYTEST_ARGS="tests/unit/reports"
```

### For AI agents (non-interactive)

The `make devenv.test.*` commands use `-it` flags that require a TTY. To run tests:

1. Check `tools/devenv/Makefile.test` to see the underlying `docker exec` commands
2. Run the same command but remove the `-it` flags
3. Adjust test paths as needed (paths are relative to the container's working directory)

**Note for AI agents**: These commands require `["all"]` permissions.

## Pre-commit Hooks

**IMPORTANT**: Always run pre-commit on ALL files before committing:

```bash
uv run pre-commit run --all-files
```

**Note for AI agents**: This command requires `["all"]` permissions due to system calls that are blocked by sandboxing.

Pre-commit hooks include:
- **ruff-check** - Linting with auto-fix
- **ruff-format** - Code formatting
- **uv-lock** - Ensures lock files are in sync with pyproject.toml

Do NOT use `--no-verify` when committing. Let pre-commit hooks run and fix any issues they identify.

## Linting and Formatting

The project uses **ruff** for both linting and formatting (configured in `ruff.toml`).

```bash
# Check for issues
uv run ruff check .

# Auto-fix issues
uv run ruff check --fix .

# Format code
uv run ruff format .
```

**Note for AI agents**: Check `ruff.toml` for enabled rules and exceptions before writing code.

## Dependencies

Dependencies are managed with **uv**:

```bash
# Sync dependencies
uv sync

# Add a new dependency (edit pyproject.toml, then)
uv lock
```

**Note for AI agents**: uv commands that fetch packages require `["network"]` permissions.

Dependency groups in `pyproject.toml`:
- `shared` - Dependencies for libs/shared
- `codecov-api` - API-specific dependencies
- `worker` - Worker-specific dependencies
- `dev` - Development and testing dependencies
- `prod` - Combined production dependencies

## Code Organization

When making changes:

1. **Shared code** (`libs/shared/`) - Used by both API and worker. Changes here affect both.
2. **API code** (`apps/codecov-api/`) - Django REST API and GraphQL.
3. **Worker code** (`apps/worker/`) - Celery tasks for background processing.

## Import Patterns

**Understanding import paths**: This monorepo has specific import conventions:

- **Shared library imports**: Use `from shared.<module> import ...` (e.g., `from shared.reports.types import ...`)
- **API imports**: Use the app name directly (e.g., `from codecov_auth.models import ...`)
- **Worker imports**: Use the module path from worker root (e.g., `from services.report import ...`)

**Top-level imports required**: All imports must be at the top of the module, not inside functions. Function-level imports will fail pre-commit checks.

## Common Make Targets

```bash
make devenv              # Build and start everything
make devenv.start        # Start without rebuilding
make devenv.stop         # Stop all containers
make devenv.refresh      # Rebuild and restart
make devenv.migrate      # Run database migrations
```

## Git Workflow

1. **Create a branch** with a descriptive name (e.g., `username/add-feature-x`)
2. Run `uv run pre-commit run --all-files` before every commit
3. Ensure tests pass for affected components
4. **Create a PR** with a clear description
5. The main branch is `main`

**Note for AI agents**: Git commits require `["all"]` permissions due to GPG signing via 1Password.
