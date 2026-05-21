"""Route-aware ``traces_sampler`` for the Sentry SDK.

The codecov-api WSGI app handles a handful of route families that emit a lot
of requests but carry low per-request debugging signal:

* ``/``, ``/health/``, ``/api_health/`` — Kubernetes / load-balancer health
  probes.
* ``/monitoring/...`` — ``django-prometheus`` scrape endpoints.
* ``/{service}/{owner}/{repo}/.../badge.{ext}`` — coverage and bundle badge
  SVGs. These are extremely high-volume and CDN-cacheable, and we never need
  per-request traces to debug them.
* ``/webhooks/github`` — the GitHub webhook handler. By far the highest-volume
  transaction in the app, and most events are uniform (push / status); we
  only need a small fraction sampled for latency and error visibility.

Everything else falls back to the default sample rate from ``settings``.

Precedence
----------
Path-based drops and down-samples take precedence over ``parent_sampled``.
This is deliberate: the shelter ingress sits in front of ``codecov-api`` and
initializes its own Sentry SDK with ``traces_sample_rate=1.0``, which means
every request shelter forwards arrives at api with ``parent_sampled=True``.
If we honored that first we would re-sample every webhook, badge and health
probe at 100%, which is the opposite of the goal of this sampler. So we
apply path rules first, then honor ``parent_sampled`` for everything else
so distributed traces from internal callers (worker → api, etc.) stay
coherent on routes we *do* care about.

The factory pattern (:func:`make_traces_sampler`) lets the settings module
inject the configurable defaults while keeping the matching logic pure and
unit-testable.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from typing import Any

# Matches all four badge route patterns registered in ``graphs.urls``:
#   /{service}/{owner}/{repo}/(graph|graphs)/badge.{ext}
#   /{service}/{owner}/{repo}/branch/{branch}/(graph|graphs)/badge.{ext}
#   /{service}/{owner}/{repo}/(graph|graphs)/bundle/{bundle}/badge.{ext}
#   /{service}/{owner}/{repo}/branch/{branch}/(graph|graphs)/bundle/{bundle}/badge.{ext}
_BADGE_PATH_RE = re.compile(r"/graphs?/(?:bundle/[^/]+/)?badge\.[^/]+/?$")

# Exact paths that we drop unconditionally — health probes only.
_HEALTH_PATHS = frozenset({"/", "/health", "/health/", "/api_health", "/api_health/"})

# Path prefix for the Prometheus scrape endpoint.
_MONITORING_PREFIX = "/monitoring/"

# Exact paths for the GitHub webhook handler. Other webhook providers
# (gitlab, bitbucket, github_enterprise, etc.) are intentionally excluded —
# only the github route is high-volume enough to need its own sample rate.
_WEBHOOK_GITHUB_PATHS = frozenset({"/webhooks/github", "/webhooks/github/"})

SamplingContext = Mapping[str, Any]
TracesSampler = Callable[[SamplingContext], float]


def _extract_path(sampling_context: SamplingContext) -> str:
    """Return the request path for HTTP transactions, or ``""`` otherwise.

    ``sampling_context`` shape varies by integration. For Django/WSGI requests
    Sentry passes the WSGI environ under ``"wsgi_environ"``; for ASGI it uses
    ``"asgi_scope"``; non-HTTP transactions (Celery, etc.) have neither.
    """
    wsgi_environ = sampling_context.get("wsgi_environ")
    if isinstance(wsgi_environ, Mapping):
        path = wsgi_environ.get("PATH_INFO")
        if isinstance(path, str):
            return path

    asgi_scope = sampling_context.get("asgi_scope")
    if isinstance(asgi_scope, Mapping):
        path = asgi_scope.get("path")
        if isinstance(path, str):
            return path

    return ""


def make_traces_sampler(
    *,
    default_rate: float,
    badge_rate: float,
    webhook_github_rate: float,
) -> TracesSampler:
    """Build a ``traces_sampler`` closure with the given fallback rates.

    Parameters
    ----------
    default_rate:
        Sample rate applied to any transaction that doesn't match a known
        low-value route. Should mirror the previous ``traces_sample_rate``.
    badge_rate:
        Sample rate applied to badge SVG endpoints. These are high-volume
        and low-variance; ``0.001`` (0.1%) keeps enough samples for latency
        monitoring without paying for full traces on every request.
    webhook_github_rate:
        Sample rate applied to ``/webhooks/github``. This is the single
        highest-volume transaction in the app and most events are uniform;
        ``0.001`` (0.1%) preserves latency and error visibility without
        tracing every callback.
    """

    def traces_sampler(sampling_context: SamplingContext) -> float:
        # Path-based rules first: a "drop this route" decision must beat
        # parent_sampled, otherwise shelter's traces_sample_rate=1.0 in
        # front of api would re-sample every webhook/badge/health probe.
        path = _extract_path(sampling_context)
        if path:
            if path in _HEALTH_PATHS:
                return 0.0
            if path.startswith(_MONITORING_PREFIX):
                return 0.0
            if path in _WEBHOOK_GITHUB_PATHS:
                return webhook_github_rate
            if _BADGE_PATH_RE.search(path):
                return badge_rate

        # Otherwise honor upstream sampling decisions so distributed traces
        # from internal callers (worker → api, etc.) stay coherent.
        parent_sampled = sampling_context.get("parent_sampled")
        if parent_sampled is True:
            return 1.0
        if parent_sampled is False:
            return 0.0

        return default_rate

    return traces_sampler
