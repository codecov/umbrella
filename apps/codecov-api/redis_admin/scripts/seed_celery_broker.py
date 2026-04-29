"""Seed fake `Owner` / `Repository` rows + matching celery_broker envelopes.

Local-dev helper for poking at the `redis_admin` celery_broker queue
admin (specifically its `(task_name, repoid, commitid)` frequency
chart) without having to hand-craft kombu envelopes in a Django shell.

Run from inside the api container, where ``WORKDIR=/app/apps/codecov-api``::

    docker compose exec api python redis_admin/scripts/seed_celery_broker.py
    docker compose exec api python redis_admin/scripts/seed_celery_broker.py --queue notify
    docker compose exec api python redis_admin/scripts/seed_celery_broker.py --clear

What it does:

1. Idempotently ensures three Owner+Repo pairs exist in Postgres
   (`github:codecov/example`, `github:codecov/another-repo`,
   `gitlab:other-org/some-project`) so the chart's repo column has
   real rows to render `service:owner/name` against. Falls back to
   creating fresh rows on first run; subsequent runs reuse the
   existing pairs by `(service, username)` + `(author, name)`.
2. Pushes a varied mix of kombu envelopes onto a configurable broker
   queue (default ``bundle_analysis``), referencing those repoids,
   so the frequency chart has more than one bucket and exercises
   both github and non-github service rendering.
3. Prints the queue depth, the seeded repoids, and the operator-
   facing admin URL so the URL can be copy/pasted into a browser.

This script is intentionally NOT a test fixture — it's a one-shot dev
seeder. Don't import from a test suite. The envelope shape is
duplicated from ``redis_admin.tests.test_celery_broker_queue._build_envelope``
on purpose so the test helper stays a test helper.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import uuid
from pathlib import Path
from typing import Any

# Make sure the `codecov-api` directory is on `sys.path` so `import codecov`
# resolves the settings module, regardless of CWD. Python only adds the
# *script's* directory (here: `redis_admin/scripts/`) to `sys.path`, not the
# project root, so a bare `python redis_admin/scripts/seed_celery_broker.py`
# invocation needs the parent-of-`redis_admin` injected manually.
_API_ROOT = Path(__file__).resolve().parents[2]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

# Bootstrap Django before any django-touching imports. The api container's
# wsgi entrypoint pulls DJANGO_SETTINGS_MODULE from `shared.django_apps.utils.config.get_settings_module`,
# but a bare `python redis_admin/scripts/seed_celery_broker.py` invocation
# skips that path — so default it ourselves to keep the script invokable
# both from `docker compose exec api` and from a developer's local shell
# pointing at the same checkout.
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "codecov.settings_dev")

import django  # noqa: E402

django.setup()

from redis_admin import conn as redis_admin_conn  # noqa: E402
from shared.django_apps.codecov_auth.models import Owner  # noqa: E402
from shared.django_apps.core.models import Repository  # noqa: E402
from shared.django_apps.core.tests.factories import (  # noqa: E402
    OwnerFactory,
    RepositoryFactory,
)


def _log(msg: str = "") -> None:
    """Wrapper around `print` so the T201 ignore lives in one place.

    This is a CLI dev runner — printing to stdout is the whole point — so
    we centralise the lint suppression rather than sprinkling
    `# noqa: T201` on every callsite.
    """

    print(msg)  # noqa: T201


# ---- envelope construction -------------------------------------------------


def _build_envelope(
    *,
    task: str,
    task_id: str | None = None,
    args: list | None = None,
    kwargs: dict[str, Any] | None = None,
) -> str:
    """Build a kombu broker envelope JSON string suitable for RPUSH.

    Mirrors the shape ``redis_admin.tests.test_celery_broker_queue._build_envelope``
    builds — duplicated here on purpose so this dev runner doesn't reach
    into a test module.
    """

    body = json.dumps([args or [], kwargs or {}, {}])
    body_b64 = base64.b64encode(body.encode("utf-8")).decode("ascii")
    headers: dict[str, Any] = {"task": task, "id": task_id or str(uuid.uuid4())}
    return json.dumps({"body": body_b64, "headers": headers})


# ---- DB seeding ------------------------------------------------------------


def _ensure_owner_repo(*, service: str, username: str, repo_name: str) -> Repository:
    """Idempotently ensure an Owner + Repository pair exists, return the repo.

    Looks up by the ``(service, username)`` unique constraint on Owner and
    the ``(author, name)`` unique constraint on Repository so re-running
    the script is a no-op for the DB. Falls back to the factories on first
    run because they handle the long tail of NOT NULL-but-not-meaningful
    columns (``oauth_token``, ``upload_token``, …) without us having to
    enumerate them here.
    """

    try:
        owner = Owner.objects.get(service=service, username=username)
    except Owner.DoesNotExist:
        owner = OwnerFactory(service=service, username=username)

    try:
        repo = Repository.objects.get(author=owner, name=repo_name)
    except Repository.DoesNotExist:
        repo = RepositoryFactory(author=owner, name=repo_name)

    return repo


def _seed_repos() -> list[tuple[str, Repository]]:
    """Create / reuse the three canonical fixtures, return display strings."""

    fixtures = [
        ("github", "codecov", "example"),
        ("github", "codecov", "another-repo"),
        ("gitlab", "other-org", "some-project"),
    ]
    out: list[tuple[str, Repository]] = []
    for service, username, name in fixtures:
        repo = _ensure_owner_repo(service=service, username=username, repo_name=name)
        display = f"{service}:{username}/{name}"
        out.append((display, repo))
    return out


# ---- queue seeding ---------------------------------------------------------

# Stable commit SHAs so reruns produce a small, predictable bucket set in
# the frequency chart instead of unique-per-run buckets.
_COMMIT_A = "0832c110a744ddb8185bfdf0524aad41d3c3d21a"
_COMMIT_B = "abcdef0123456789abcdef0123456789abcdef01"
_COMMIT_C = "deadbeef"

_BUNDLE_ANALYSIS_TASK = "app.tasks.bundle_analysis.BundleAnalysisProcessor"
_NOTIFY_TASK = "app.tasks.notify.NotifyTask"


def _push_envelopes(
    redis: Any,
    queue: str,
    repos: list[tuple[str, Repository]],
) -> int:
    """Push the canonical mix of envelopes; return the number pushed."""

    example_repo = repos[0][1]
    another_repo = repos[1][1]
    gitlab_repo = repos[2][1]

    plan: list[tuple[int, str, dict[str, Any]]] = [
        # 5x BundleAnalysisProcessor on the github:codecov/example repo,
        # commit A — the dominant bucket in the chart.
        (
            5,
            _BUNDLE_ANALYSIS_TASK,
            {"repoid": example_repo.repoid, "commitid": _COMMIT_A},
        ),
        # 3x BundleAnalysisProcessor same repo, different commit — proves
        # the chart groups by `(repoid, commitid)` rather than just repoid.
        (
            3,
            _BUNDLE_ANALYSIS_TASK,
            {"repoid": example_repo.repoid, "commitid": _COMMIT_B},
        ),
        # 2x NotifyTask on the second github repo — proves the chart
        # groups by task_name too.
        (
            2,
            _NOTIFY_TASK,
            {"repoid": another_repo.repoid, "commitid": _COMMIT_A},
        ),
        # 1x NotifyTask on a gitlab repo with a pullid kwarg — exercises
        # the non-github service column rendering and the `pullid`
        # extraction for owner/pull-shaped tasks.
        (
            1,
            _NOTIFY_TASK,
            {"repoid": gitlab_repo.repoid, "commitid": _COMMIT_C, "pullid": 42},
        ),
    ]

    total = 0
    for count, task, kwargs in plan:
        for _ in range(count):
            envelope = _build_envelope(task=task, kwargs=kwargs)
            redis.rpush(queue, envelope)
            total += 1
    return total


# ---- entry point -----------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--queue",
        default="bundle_analysis",
        help="Celery broker queue name to push envelopes onto (default: bundle_analysis).",
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Run only `redis.delete(queue)` and exit. Handy between seeds.",
    )
    args = parser.parse_args(argv)

    redis = redis_admin_conn.get_connection(kind="broker")
    queue = args.queue

    if args.clear:
        before = redis.llen(queue)
        redis.delete(queue)
        _log(
            f"cleared queue {queue!r}: deleted key (LLEN was {before}, now "
            f"{redis.llen(queue)})"
        )
        return 0

    repos = _seed_repos()
    pushed = _push_envelopes(redis, queue, repos)
    depth = redis.llen(queue)

    _log("=" * 72)
    _log(f"Seeded {pushed} celery envelope(s) onto queue {queue!r}")
    _log(f"  LLEN({queue}) = {depth}")
    _log()
    _log("Owner+Repo fixtures (resolved to `service:owner/name` in the chart):")
    for display, repo in repos:
        _log(f"  repoid={repo.repoid:<10} {display}")
    _log()
    _log("Open this URL to see the frequency chart pick up real repo links:")
    _log(
        f"  http://localhost:8000/admin/redis_admin/celerybrokerqueue/?queue_name__exact={queue}"
    )
    _log()
    _log(
        "Reminder: the queue only shows up on the celery summary landing page "
        "when its name is in the resolved set, which means listing it under "
        "`SETUP__REDIS_ADMIN__CELERY_QUEUES` (comma-separated) on the api pod. "
        "The drill-down URL above bypasses the summary list and works either way."
    )
    _log("=" * 72)
    return 0


if __name__ == "__main__":
    sys.exit(main())
