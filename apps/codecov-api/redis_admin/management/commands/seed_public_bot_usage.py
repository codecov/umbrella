import time

from django.core.management.base import BaseCommand

from shared.helpers.redis import get_redis_connection
from shared.rate_limits.public_bot import (
    POOL_BUDGET_KEY_PREFIX,
    POOL_RESET_KEY_PREFIX,
    REPO_USAGE_KEY_PREFIX,
    PublicBotUsageStore,
    repo_cap,
)

# (bot, repo slug, hits in the current GitHub window)
_MOCK_USAGE = [
    ("commit", "mongodb/mongo-python-driver", 1200),
    ("commit", "apache/texera", 420),
    ("commit", "keras-team/keras", 180),
    ("commit", "microsoft/typescript-go", 95),
    ("pull", "mongodb/mongo-python-driver", 900),
    ("pull", "rapidsai/cudf", 650),
    ("pull", "awslabs/nx-plugin-for-aws", 310),
    ("pull", "ydb-platform/ydb-go-sdk", 140),
    ("read", "snapcore/snapd", 220),
    ("read", "scaleway/scaleway-sdk-python", 80),
    ("status", "telekom/k8s-breakglass", 55),
    ("tokenless", "NatLabRockies/rdtools", 40),
    ("tokenless_bot", "siliconcompiler/siliconcompiler", 25),
]

_GITHUB_BUDGET = 15000
_GITHUB_WINDOW_SECONDS = 3600
_POOL_STATE_TTL_SECONDS = 3600


class Command(BaseCommand):
    help = "Seed Redis with sample public-bot usage for the admin dashboard."

    def add_arguments(self, parser):
        parser.add_argument(
            "--clear",
            action="store_true",
            help="Delete existing public-bot usage keys before seeding.",
        )

    def handle(self, *args, **options):
        redis = get_redis_connection()
        store = PublicBotUsageStore(redis)

        if options["clear"]:
            self._clear_public_bot_keys(redis)

        reset_ts = int(time.time()) + _GITHUB_WINDOW_SECONDS
        cap = repo_cap(_GITHUB_BUDGET)

        for bot, repo, hits in _MOCK_USAGE:
            self._seed_repo_usage(redis, bot, repo, hits, reset_ts=reset_ts)

        for bot in {row[0] for row in _MOCK_USAGE}:
            store.update_pool_from_headers(
                bot,
                remaining=max(0, _GITHUB_BUDGET - 5000),
                limit=_GITHUB_BUDGET,
                reset=reset_ts,
            )
            redis.expire(f"{POOL_BUDGET_KEY_PREFIX}:{bot}", _POOL_STATE_TTL_SECONDS)
            redis.expire(f"{POOL_RESET_KEY_PREFIX}:{bot}", _POOL_STATE_TTL_SECONDS)

        over_cap = sum(1 for _, _, hits in _MOCK_USAGE if hits >= cap)
        self.stdout.write(
            self.style.SUCCESS(
                f"Seeded {len(_MOCK_USAGE)} repo rows "
                f"({over_cap} over the 5% cap of {cap}). "
                "Open /admin/redis_admin/publicbotusage/"
            )
        )

    def _clear_public_bot_keys(self, redis) -> None:
        patterns = (
            f"{REPO_USAGE_KEY_PREFIX}:*",
            f"{POOL_BUDGET_KEY_PREFIX}:*",
            f"{POOL_RESET_KEY_PREFIX}:*",
        )
        deleted = 0
        for pattern in patterns:
            for key in redis.scan_iter(match=pattern):
                redis.delete(key)
                deleted += 1
        self.stdout.write(f"Cleared {deleted} existing public-bot keys.")

    def _seed_repo_usage(
        self, redis, bot: str, repo: str, hits: int, *, reset_ts: int
    ) -> None:
        minute_bucket = int(time.time() // 60)
        key = f"{REPO_USAGE_KEY_PREFIX}:{bot}:{repo}:{minute_bucket}"
        redis.set(key, hits, ex=_GITHUB_WINDOW_SECONDS + 60)
