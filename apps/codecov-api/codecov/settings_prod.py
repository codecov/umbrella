import os

from .settings_base import *

DEBUG = False
THIS_POD_IP = os.environ.get("THIS_POD_IP")
ALLOWED_HOSTS = get_config(
    "setup", "api_allowed_hosts", default=["codecov.io-shadow", ".codecov.io"]
)
if THIS_POD_IP:
    ALLOWED_HOSTS.append(THIS_POD_IP)

WEBHOOK_URL = get_config("setup", "webhook_url", default="https://codecov.io")


STRIPE_API_KEY = os.environ.get("SERVICES__STRIPE__API_KEY", None)
STRIPE_ENDPOINT_SECRET = os.environ.get("SERVICES__STRIPE__ENDPOINT_SECRET", None)

CORS_ALLOW_HEADERS += ["sentry-trace", "baggage"]
CODECOV_URL = get_config("setup", "codecov_url", default="https://codecov.io")
CODECOV_API_URL = get_config("setup", "codecov_api_url", default=CODECOV_URL)
CODECOV_DASHBOARD_URL = get_config(
    "setup", "codecov_dashboard_url", default="https://app.codecov.io"
)
CORS_ALLOWED_ORIGINS = [
    CODECOV_URL,
    CODECOV_DASHBOARD_URL,
    "https://gazebo.netlify.app",  # to access unreleased URL of gazebo
]
# We are also using the CORS settings to verify if the domain is safe to
# Redirect after authentication, update this setting with care
CORS_ALLOWED_ORIGIN_REGEXES = []

# 25MB in bytes
DATA_UPLOAD_MAX_MEMORY_SIZE = 26214400

SILENCED_SYSTEM_CHECKS = ["urls.W002"]

# Reinforcing the Cookie SameSite configuration to be sure it's Lax in prod
COOKIE_SAME_SITE = "Lax"

CSRF_TRUSTED_ORIGINS = [
    get_config("setup", "trusted_origin", default="https://*.codecov.io")
]
