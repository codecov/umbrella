from datetime import UTC, datetime


def get_utc_now() -> datetime:
    return datetime.now(UTC)


def get_utc_now_as_iso_format() -> str:
    return get_utc_now().isoformat()


def get_seconds_to_next_hour() -> int:
    now = datetime.now(UTC)
    current_seconds = (now.minute * 60) + now.second
    return 3600 - current_seconds
