import logging
import os

from shared.config import get_config

log = logging.getLogger(__name__)

RUN_ENV: str = os.environ.get("RUN_ENV", "PRODUCTION")

_settings_module: str | None = None


def get_settings_module(parent_module: str = "codecov") -> str:
    global _settings_module  # noqa: PLW0603
    if not _settings_module:
        match RUN_ENV:
            case "DEV":
                _settings_module = f"{parent_module}.settings_dev"
            case "STAGING":
                _settings_module = f"{parent_module}.settings_staging"
            case "TESTING":
                _settings_module = f"{parent_module}.settings_test"
            case "ENTERPRISE":
                _settings_module = f"{parent_module}.settings_enterprise"
            case "PRODUCTION":
                _settings_module = f"{parent_module}.settings_prod"
            case _:
                log.warning("Unknown value for RUN_ENV", extra={"RUN_ENV": RUN_ENV})
                _settings_module = f"{parent_module}.settings_prod"

    return _settings_module


def should_write_data_to_storage_config_check(
    master_switch_key: str, is_codecov_repo: bool, repoid: int
) -> bool:
    """
    master_write_switch can be: general_access, codecov_access, restricted_access, True, or False
    This function includes legacy support: previously this was a bool config, where True meant codecov_access
    and False meant no one.
    """
    master_write_switch = get_config(
        "setup",
        "save_report_data_in_storage",
        master_switch_key,
        default=False,
    )

    if master_write_switch == "general_access":
        # for everyone
        return True
    if master_write_switch in {"codecov_access", True}:
        # for us
        return is_codecov_repo
    if master_write_switch == "restricted_access":
        # for us and any special repos we have in repo_ids config
        allowed_repo_ids = get_config(
            "setup", "save_report_data_in_storage", "repo_ids", default=[]
        )
        is_in_allowed_repoids = repoid in allowed_repo_ids
        return is_codecov_repo or is_in_allowed_repoids

    return False
