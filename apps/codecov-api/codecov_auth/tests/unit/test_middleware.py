from django.test import TestCase, override_settings

from utils.test_utils import Client


@override_settings(
    CORS_ALLOWED_ORIGINS=["http://localhost:3000"],
    CORS_ALLOWED_ORIGIN_REGEXES=[],
    EXTERNAL_CORS_ALLOW_ALL_ORIGINS=True,
)
class MiddlewareTest(TestCase):
    def setUp(self):
        self.client = Client()

    def test_whitelisted_origin(self):
        res = self.client.get("/health", headers={"Origin": "http://localhost:3000"})

        assert res.headers["Access-Control-Allow-Origin"] == "http://localhost:3000"
        assert res.headers["Access-Control-Allow-Credentials"] == "true"

    def test_non_whitelisted_origin(self):
        res = self.client.get("/health", headers={"Origin": "http://example.com"})

        assert res.headers["Access-Control-Allow-Origin"] == "*"
        assert "Access-Control-Allow-Credentials" not in res.headers

    @override_settings(EXTERNAL_CORS_ALLOW_ALL_ORIGINS=False)
    def test_external_cors_allow_all_origins_false(self):
        res = self.client.get("/health", headers={"Origin": "http://example.com"})

        assert "Access-Control-Allow-Origin" not in res.headers
        assert "Access-Control-Allow-Credentials" not in res.headers
