import logging

from shared.config import get_config as shared_get_config


class MissingConfigException(Exception):
    pass


log = logging.getLogger(__name__)


def get_config(*path, default=None):
    return shared_get_config(*path, default=default)
