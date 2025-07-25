import logging
import os
import sys

import click
import django
from celery.signals import worker_process_shutdown
from prometheus_client import REGISTRY, CollectorRegistry, multiprocess

import app
import shared.storage
from helpers.environment import get_external_dependencies_folder
from helpers.logging_config import get_logging_config_dict
from helpers.version import get_current_version
from shared.celery_config import BaseCeleryConfig
from shared.config import get_config
from shared.django_apps.utils.config import get_settings_module
from shared.license import startup_license_logging
from shared.metrics import start_prometheus
from shared.storage.exceptions import BucketAlreadyExistsError

log = logging.getLogger(__name__)

initialization_text = """
  _____          _
 / ____|        | |
| |     ___   __| | ___  ___ _____   __
| |    / _ \\ / _` |/ _ \\/ __/ _ \\ \\ / /
| |___| (_) | (_| |  __/ (_| (_) \\ V /
 \\_____\\___/ \\__,_|\\___|\\___\\___/ \\_/
                              {version}

"""


# Prometheus needs us to do this when running in multiprocess mode:
# https://prometheus.github.io/client_python/multiprocess/
@worker_process_shutdown.connect
def mark_process_dead(pid, exitcode, **kwargs):
    multiprocess.mark_process_dead(pid)


@click.group()
@click.pass_context
def cli(ctx: click.Context):
    pass


def setup_worker():
    _config_dict = get_logging_config_dict()
    logging.config.dictConfig(_config_dict)

    log.info(initialization_text.format(version=get_current_version()))

    if getattr(sys, "frozen", False):
        # Only for enterprise builds
        external_deps_folder = get_external_dependencies_folder()
        log.info(f"External dependencies folder configured to {external_deps_folder}")
        sys.path.append(external_deps_folder)

    registry = REGISTRY
    if "PROMETHEUS_MULTIPROC_DIR" in os.environ:
        log.info(
            f"Setting up Prometheus multiprocess collection in {os.environ['PROMETHEUS_MULTIPROC_DIR']}"
        )
        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry)

    log.info("Starting Prometheus collection")
    start_prometheus(9996, registry=registry)  # 9996 is an arbitrary port number

    minio_config = get_config("services", "minio", default={})
    auto_create_bucket = minio_config.get("auto_create_bucket", False)
    if auto_create_bucket:
        try:
            bucket_name = minio_config.get("bucket", "archive")
            region = minio_config.get("region", "us-east-1")

            # note that this is a departure from the old default behavior.
            # This is intended as the bucket will exist in most cases where IAC or manual setup is used
            log.info("Initializing bucket %s", bucket_name)

            # this storage client is only used to create the bucket so it doesn't need to be
            # aware of the repoid
            storage_client = shared.storage.get_appropriate_storage_service()
            storage_client.create_root_storage(bucket_name, region)
        except BucketAlreadyExistsError:
            pass

    os.environ.setdefault(
        "DJANGO_SETTINGS_MODULE", get_settings_module("django_scaffold")
    )
    log.info(
        f"Configuring Django with settings in {os.environ['DJANGO_SETTINGS_MODULE']}"
    )
    django.setup()

    startup_license_logging()


@cli.command()
@click.option("--name", envvar="HOSTNAME", default="worker", help="Node name")
@click.option(
    "--concurrency", type=int, default=2, help="Number for celery concurrency"
)
@click.option("--debug", is_flag=True, default=False, help="Enable celery debug mode")
@click.option(
    "--queue",
    multiple=True,
    default=["celery"],
    help="Queues to listen to for this worker",
)
def worker(name: str, concurrency: int, debug: bool, queue: list[str]):
    setup_worker()
    args = [
        "worker",
        "-n",
        name,
        "-c",
        concurrency,
        "-l",
        ("debug" if debug else "info"),
    ]
    if get_config("setup", "celery_queues_enabled", default=True):
        actual_queues = _get_queues_param_from_queue_input(queue)
        args += ["-Q", actual_queues]

    if get_config("setup", "celery_beat_enabled", default=True):
        args += ["-B", "-s", "/home/codecov/celerybeat-schedule"]

    return app.celery_app.worker_main(argv=args)


def _get_queues_param_from_queue_input(queues: list[str]) -> str:
    # We always run the health_check queue to make sure the healthcheck is performed
    # And also to avoid that queue fillign up with no workers to consume from it

    # Support passing comma separated values, as those will be split again:
    joined_queues = ",".join(queues)
    enterprise_queues = []
    if get_config("setup", "enterprise_queues_enabled", default=True):
        enterprise_queues = [
            "enterprise_" + q if not q.startswith("enterprise_") else ""
            for q in joined_queues.split(",")
        ]
    all_queues = [
        joined_queues,
        *enterprise_queues,
        BaseCeleryConfig.health_check_default_queue,
    ]

    return ",".join([q for q in all_queues if q])


def main():
    cli(obj={})


if __name__ == "__main__":
    main()
