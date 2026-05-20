"""Route-aware ``traces_sampler`` for the Sentry SDK.

The codecov-api WSGI app handles a handful of route families that emit a lot
of requests but carry no debugging signal:

* ``/``, ``/health/``, ``/api_health/`` — Kubernetes / load-balancer health
  probes.
* ``/monitoring/...`` — ``django-prometheus`` scrape endpoints.
* ``/{service}/{owner}/{repo}/.../badge.{ext}`` — coverage and bundle badge
  SVGs. These are extremely high-volume and CDN-cacheable, and we never need
  per-request traces to debug them.

Everything else falls back to the default sample rate from ``settings``.

The sampler also honors ``parent_sampled`` so distributed traces initiated
upstream (e.g. by the shelter ingress) keep a consistent sampling decision.

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
    """

    def traces_sampler(sampling_context: SamplingContext) -> float:
        # Honor upstream sampling decisions so distributed traces stay coherent.
        parent_sampled = sampling_context.get("parent_sampled")
        if parent_sampled is True:
            return 1.0
        if parent_sampled is False:
            return 0.0

        path = _extract_path(sampling_context)
        if not path:
            return default_rate

        if path in _HEALTH_PATHS:
            return 0.0
        if path.startswith(_MONITORING_PREFIX):
            return 0.0
        if _BADGE_PATH_RE.search(path):
            return badge_rate

        return default_rate

    return traces_sampler
