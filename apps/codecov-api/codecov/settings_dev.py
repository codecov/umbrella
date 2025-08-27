import socket

from .settings_base import *

# Remove CSP headers from local development build to allow GQL Playground
MIDDLEWARE.remove("csp.middleware.CSPMiddleware")

DEBUG = True
# for shelter add "host.docker.internal" and make sure to map it to localhost
# in your /etc/hosts
ALLOWED_HOSTS = get_config(
    "setup",
    "api_allowed_hosts",
    default=["localhost", "api.lt.codecov.dev", "host.docker.internal"],
)

WEBHOOK_URL = ""  # NGROK TUNNEL HERE

# Django debug toolbar configuration
INSTALLED_APPS.append("debug_toolbar")
MIDDLEWARE.insert(0, "debug_toolbar.middleware.DebugToolbarMiddleware")
SHOW_TOOLBAR_CALLBACK = "debug_toolbar.middleware.show_toolbar_with_docker"


def get_internal_ips() -> list[str]:
    ips = ["127.0.0.1"]
    try:
        # Dynamically resolve gateway hostname to IP
        gateway_ip = socket.gethostbyname("gateway")
        ips.append(gateway_ip)
    except socket.gaierror:
        # If hostname resolution fails, only use localhost
        pass
    return ips


INTERNAL_IPS = get_internal_ips()

STRIPE_API_KEY = get_config("services", "stripe", "api_key", default="default")
STRIPE_ENDPOINT_SECRET = get_config(
    "services", "stripe", "endpoint_secret", default="default"
)

CODECOV_URL = get_config("setup", "codecov_url", default="http://localhost:8080")
CODECOV_API_URL = get_config("setup", "codecov_api_url", default=CODECOV_URL)
CODECOV_DASHBOARD_URL = get_config(
    "setup", "codecov_dashboard_url", default=CODECOV_URL
)

CORS_ALLOWED_ORIGINS = [
    CODECOV_DASHBOARD_URL,
    "http://localhost:9002",  # Minio port when not using gateway
]

COOKIES_DOMAIN = "localhost"
SESSION_COOKIE_DOMAIN = "localhost"

# add for shelter
# SHELTER_SHARED_SECRET = "shelter-shared-secret"

GUEST_ACCESS = True

GRAPHQL_INTROSPECTION_ENABLED = True
