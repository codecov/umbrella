AVATAR_GITHUB_BASE_URL = "https://avatars0.githubusercontent.com"
BITBUCKET_BASE_URL = "https://bitbucket.org"
GITLAB_BASE_URL = "https://gitlab.com"
GRAVATAR_BASE_URL = "https://www.gravatar.com"
AVATARIO_BASE_URL = "https://avatars.io"
OWNER_YAML_TO_STRING_KEY = "to_string"
USE_SENTRY_APP_INDICATOR = "should_use_sentry_app"

# Inbound supertoken API access (the path that fetched Codecov data for Sentry's
# UI) is turned OFF, but deliberately kept in place rather than deleted: the
# plumbing is dormant and reusable if we later want to repurpose it for a Harness
# supertoken. Forcing this False disables the path everywhere the indicator is
# read (all read sites getattr-default to False). Flip to True to revive it.
SENTRY_INBOUND_API_ENABLED = False
