from django.test import TestCase, override_settings

from utils.test_utils import Client


@override_settings(
    CORS_ALLOWED_ORIGINS=["http://localhost:3000"],
    CORS_ALLOWED_ORIGIN_REGEXES=[],
    CORS_ALLOW_ALL_ORIGINS=True,
    CORS_ALLOW_CREDENTIALS=True,
    CORS_EXPOSE_HEADERS=["Test-Expose"],
)
class MiddlewareTest(TestCase):
    options_cors_headers = (
        "Access-Control-Allow-Headers",
        "Access-Control-Allow-Methods",
        "Access-Control-Max-Age",
    )

    def setUp(self):
        self.client = Client()

    def test_get_whitelisted_origin(self):
        res = self.client.get("/health", headers={"Origin": "http://localhost:3000"})

        assert res.headers["Access-Control-Allow-Origin"] == "http://localhost:3000"
        assert res.headers["Access-Control-Allow-Credentials"] == "true"
        assert res.headers["Access-Control-Expose-Headers"] == "Test-Expose"

        for header in self.options_cors_headers:
            assert header not in res.headers

    def test_options_whitelisted_origin(self):
        res = self.client.options(
            "/health", headers={"Origin": "http://localhost:3000"}
        )

        assert res.headers["Access-Control-Allow-Origin"] == "http://localhost:3000"
        assert res.headers["Access-Control-Allow-Credentials"] == "true"
        assert res.headers["Access-Control-Expose-Headers"] == "Test-Expose"

        for header in self.options_cors_headers:
            assert header in res.headers

    def test_get_non_whitelisted_origin(self):
        res = self.client.get("/health", headers={"Origin": "http://example.com"})

        assert res.headers["Access-Control-Allow-Origin"] == "*"
        assert "Access-Control-Allow-Credentials" not in res.headers
        assert res.headers["Access-Control-Expose-Headers"] == "Test-Expose"

        for header in self.options_cors_headers:
            assert header not in res.headers

    def test_options_non_whitelisted_origin(self):
        res = self.client.options("/health", headers={"Origin": "http://example.com"})

        assert res.headers["Access-Control-Allow-Origin"] == "*"
        assert "Access-Control-Allow-Credentials" not in res.headers
        assert res.headers["Access-Control-Expose-Headers"] == "Test-Expose"

        for header in self.options_cors_headers:
            assert header in res.headers

    @override_settings(CORS_ALLOW_ALL_ORIGINS=False)
    def test_options_cors_allow_all_origins_false(self):
        res = self.client.options("/health", headers={"Origin": "http://example.com"})

        assert "Access-Control-Allow-Origin" not in res.headers
        assert "Access-Control-Allow-Credentials" not in res.headers
        assert "Access-Control-Expose-Headers" not in res.headers

        for header in self.options_cors_headers:
            assert header not in res.headers

    @override_settings(CORS_ALLOW_ALL_ORIGINS=False, CORS_ALLOW_CREDENTIALS=False)
    def test_get_cors_allow_all_origins_credentials_false(self):
        res = self.client.get("/health", headers={"Origin": "http://example.com"})

        assert "Access-Control-Allow-Origin" not in res.headers
        assert "Access-Control-Allow-Credentials" not in res.headers
        assert "Access-Control-Expose-Headers" not in res.headers

        for header in self.options_cors_headers:
            assert header not in res.headers

    @override_settings(CORS_ALLOW_CREDENTIALS=False)
    def test_options_cors_allow_credentials_false(self):
        res = self.client.options(
            "/health", headers={"Origin": "http://localhost:3000"}
        )

        assert res.headers["Access-Control-Allow-Origin"] == "*"
        assert "Access-Control-Allow-Credentials" not in res.headers
        assert res.headers["Access-Control-Expose-Headers"] == "Test-Expose"

        for header in self.options_cors_headers:
            assert header in res.headers
