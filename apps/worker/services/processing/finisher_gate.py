from shared.helpers.redis import get_redis_connection

FINISHER_GATE_KEY_PREFIX = "upload_merger_lock"
FINISHER_GATE_TTL_SECONDS = 900


def finisher_gate_key(repo_id: int, commit_sha: str) -> str:
    return f"{FINISHER_GATE_KEY_PREFIX}_{repo_id}_{commit_sha}"


def try_acquire_finisher_gate(repo_id: int, commit_sha: str) -> bool:
    return bool(
        get_redis_connection().set(
            finisher_gate_key(repo_id, commit_sha),
            "1",
            nx=True,
            ex=FINISHER_GATE_TTL_SECONDS,
        )
    )


def refresh_finisher_gate_ttl(repo_id: int, commit_sha: str) -> None:
    get_redis_connection().expire(
        finisher_gate_key(repo_id, commit_sha), FINISHER_GATE_TTL_SECONDS
    )


def delete_finisher_gate(repo_id: int, commit_sha: str) -> None:
    get_redis_connection().delete(finisher_gate_key(repo_id, commit_sha))
