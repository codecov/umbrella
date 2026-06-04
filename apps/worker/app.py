import logging
import logging.config

from celery import Celery, signals
from django.db import close_old_connections
from pydantic import BaseModel

from helpers.logging_config import get_logging_config_dict
from helpers.sentry import initialize_sentry, is_sentry_enabled
from shared.utils.pydantic_serializer import PydanticModelDump, register_preserializer

log = logging.getLogger(__name__)

_config_dict = get_logging_config_dict()
logging.config.dictConfig(_config_dict)

register_preserializer(PydanticModelDump)(BaseModel)
celery_app = Celery("tasks")
celery_app.config_from_object("celery_config:CeleryWorkerConfig")


@signals.celeryd_init.connect
def init_sentry(**_kwargs):
    if is_sentry_enabled():
        initialize_sentry()


@signals.task_prerun.connect
def close_stale_db_connections(**_kwargs):
    close_old_connections()
