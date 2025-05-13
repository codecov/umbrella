import os
from enum import Enum

from shared.config import get_config


class SettingsModule(Enum):
    DEV = "codecov.settings_dev"
    STAGING = "codecov.settings_staging"
    TESTING = "codecov.settings_test"
    ENTERPRISE = "codecov.settings_enterprise"
    PRODUCTION = "codecov.settings_prod"


RUN_ENV = os.environ.get("RUN_ENV", "PRODUCTION")

if RUN_ENV == "DEV":
    settings_module = SettingsModule.DEV.value
elif RUN_ENV == "STAGING":
    settings_module = SettingsModule.STAGING.value
elif RUN_ENV == "TESTING":
    settings_module = SettingsModule.TESTING.value
elif RUN_ENV == "ENTERPRISE":
    settings_module = SettingsModule.ENTERPRISE.value
else:
    settings_module = SettingsModule.PRODUCTION.value


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
