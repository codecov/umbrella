from typing import Any

from ariadne import ObjectType
from django.conf import settings

version_bindable = ObjectType("Version")


def _parse_version() -> dict[str, int]:
    version_str = getattr(settings, "VERSION", "0.0.0")
    parts = version_str.split(".")
    try:
        major = int(parts[0]) if len(parts) > 0 else 0
        minor = int(parts[1]) if len(parts) > 1 else 0
        patch = int(parts[2]) if len(parts) > 2 else 0
    except (ValueError, IndexError):
        major, minor, patch = 0, 0, 0
    return {"major": major, "minor": minor, "patch": patch}


@version_bindable.field("major")
def resolve_major(obj: Any, info: Any) -> int:
    return obj["major"]


@version_bindable.field("minor")
def resolve_minor(obj: Any, info: Any) -> int:
    return obj["minor"]


@version_bindable.field("patch")
def resolve_patch(obj: Any, info: Any) -> int:
    return obj["patch"]
