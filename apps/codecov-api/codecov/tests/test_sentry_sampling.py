import pytest

from codecov.sentry_sampling import make_traces_sampler


@pytest.fixture
def sampler():
    return make_traces_sampler(default_rate=0.5, badge_rate=0.001)


def wsgi_ctx(path: str) -> dict:
    return {"wsgi_environ": {"PATH_INFO": path, "REQUEST_METHOD": "GET"}}


def asgi_ctx(path: str) -> dict:
    return {"asgi_scope": {"type": "http", "path": path, "method": "GET"}}


class TestHealthAndMonitoring:
    @pytest.mark.parametrize(
        "path",
        ["/", "/health", "/health/", "/api_health", "/api_health/"],
    )
    def test_health_paths_drop_to_zero(self, sampler, path):
        assert sampler(wsgi_ctx(path)) == 0.0

    @pytest.mark.parametrize(
        "path",
        ["/monitoring/", "/monitoring/metrics", "/monitoring/metrics/"],
    )
    def test_monitoring_paths_drop_to_zero(self, sampler, path):
        assert sampler(wsgi_ctx(path)) == 0.0

    def test_health_paths_drop_to_zero_under_asgi(self, sampler):
        assert sampler(asgi_ctx("/")) == 0.0
        assert sampler(asgi_ctx("/monitoring/metrics")) == 0.0


class TestBadgeRoutes:
    @pytest.mark.parametrize(
        "path",
        [
            # default-badge
            "/gh/codecov/example/graph/badge.svg",
            "/gh/codecov/example/graphs/badge.svg",
            # branch-badge
            "/gh/codecov/example/branch/main/graph/badge.svg",
            "/gh/codecov/example/branch/feature/some-branch/graphs/badge.png",
            # default-bundle-badge
            "/gh/codecov/example/graph/bundle/web/badge.svg",
            "/gh/codecov/example/graphs/bundle/api/badge.svg",
            # branch-bundle-badge
            "/gh/codecov/example/branch/main/graph/bundle/web/badge.svg",
            # trailing slash tolerated
            "/gh/codecov/example/graph/badge.svg/",
        ],
    )
    def test_badge_paths_use_badge_rate(self, sampler, path):
        assert sampler(wsgi_ctx(path)) == 0.001

    @pytest.mark.parametrize(
        "path",
        [
            # graph charts, not badges
            "/gh/codecov/example/graph/tree.svg",
            "/gh/codecov/example/branch/main/graph/sunburst.svg",
            "/gh/codecov/example/pull/123/graph/icicle.svg",
            # words that share a prefix but aren't badges
            "/gh/codecov/example/graphqlbadge.svg",
            "/gh/codecov/example/badge.svg",
            # API endpoints that include "badge" in a payload, not a path segment
            "/api/v2/repos/badge-config",
        ],
    )
    def test_non_badge_paths_fall_through(self, sampler, path):
        assert sampler(wsgi_ctx(path)) == 0.5


class TestDefaultRate:
    @pytest.mark.parametrize(
        "path",
        [
            "/webhooks/github",
            "/upload/v4/",
            "/api/v2/github/codecov/repos/example/file_report/foo.py/",
            "/billing/",
            "/graphql/",
        ],
    )
    def test_normal_paths_use_default_rate(self, sampler, path):
        assert sampler(wsgi_ctx(path)) == 0.5

    def test_non_http_transaction_uses_default_rate(self, sampler):
        # Celery / cron transactions arrive without wsgi_environ or asgi_scope.
        ctx = {"transaction_context": {"name": "app.tasks.notify.Notify"}}
        assert sampler(ctx) == 0.5

    def test_empty_context_uses_default_rate(self, sampler):
        assert sampler({}) == 0.5

    def test_malformed_wsgi_environ_uses_default_rate(self, sampler):
        # PATH_INFO missing / wrong type.
        assert sampler({"wsgi_environ": {"REQUEST_METHOD": "GET"}}) == 0.5
        assert sampler({"wsgi_environ": "not-a-mapping"}) == 0.5


class TestParentSampled:
    def test_parent_sampled_true_always_samples(self, sampler):
        # Even on a normally-dropped path, the upstream decision wins.
        ctx = {**wsgi_ctx("/monitoring/metrics"), "parent_sampled": True}
        assert sampler(ctx) == 1.0

    def test_parent_sampled_false_always_drops(self, sampler):
        ctx = {**wsgi_ctx("/webhooks/github"), "parent_sampled": False}
        assert sampler(ctx) == 0.0

    def test_parent_sampled_none_falls_through_to_path_rules(self, sampler):
        ctx = {**wsgi_ctx("/monitoring/metrics"), "parent_sampled": None}
        assert sampler(ctx) == 0.0


class TestConfigurability:
    def test_default_rate_is_honored(self):
        sampler = make_traces_sampler(default_rate=0.25, badge_rate=0.01)
        assert sampler(wsgi_ctx("/webhooks/github")) == 0.25

    def test_badge_rate_is_honored(self):
        sampler = make_traces_sampler(default_rate=1.0, badge_rate=0.0)
        assert sampler(wsgi_ctx("/gh/codecov/example/graph/badge.svg")) == 0.0

    def test_health_drops_regardless_of_default(self):
        sampler = make_traces_sampler(default_rate=1.0, badge_rate=1.0)
        assert sampler(wsgi_ctx("/health/")) == 0.0
