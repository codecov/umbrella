import os
import sys
from unittest import mock

from click.testing import CliRunner

# Import `app` to register logs correctly before calling `worker_main` in tests.
import app as _  # noqa: F401
from main import _get_queues_param_from_queue_input, cli, main, setup_worker
from shared.celery_config import BaseCeleryConfig


def test_get_queues_param_from_queue_input():
    assert (
        _get_queues_param_from_queue_input(["worker,profiling,notify"])
        == f"worker,profiling,notify,enterprise_worker,enterprise_profiling,enterprise_notify,{BaseCeleryConfig.health_check_default_queue}"
    )
    assert (
        _get_queues_param_from_queue_input(["worker", "profiling", "notify"])
        == f"worker,profiling,notify,enterprise_worker,enterprise_profiling,enterprise_notify,{BaseCeleryConfig.health_check_default_queue}"
    )


def test_get_queues_param_from_queue_input_disabled_enterprise_queues(mocker):
    mocker.patch("main.get_config", return_value=False)
    assert (
        _get_queues_param_from_queue_input(["worker,profiling,notify"])
        == f"worker,profiling,notify,{BaseCeleryConfig.health_check_default_queue}"
    )
    assert (
        _get_queues_param_from_queue_input(["worker", "profiling", "notify"])
        == f"worker,profiling,notify,{BaseCeleryConfig.health_check_default_queue}"
    )


def test_get_queues_param_from_queue_input_does_not_double_enterprise():
    assert (
        _get_queues_param_from_queue_input(
            ["enterprise_worker,enterprise_profiling,enterprise_notify"]
        )
        == f"enterprise_worker,enterprise_profiling,enterprise_notify,{BaseCeleryConfig.health_check_default_queue}"
    )
    assert (
        _get_queues_param_from_queue_input(
            ["enterprise_worker", "enterprise_profiling", "enterprise_notify"]
        )
        == f"enterprise_worker,enterprise_profiling,enterprise_notify,{BaseCeleryConfig.health_check_default_queue}"
    )


@mock.patch("main.start_prometheus")
def test_run_empty_config(mock_prometheus, mock_storage, mock_configuration):
    assert not mock_storage.root_storage_created
    res = setup_worker()
    assert res is None
    assert not mock_storage.root_storage_created
    assert mock_storage.config == {}


@mock.patch("main.start_prometheus")
def test_sys_path_append_on_enterprise(
    mock_prometheus, mock_storage, mock_configuration
):
    sys.frozen = True
    res = setup_worker()
    assert res is None
    assert "./external_deps" in sys.path


@mock.patch("main.start_prometheus")
def test_run_already_existing_root_storage(
    mock_prometheus, mock_storage, mock_configuration
):
    mock_storage.root_storage_created = True
    res = setup_worker()
    assert res is None
    assert mock_storage.config == {}
    assert mock_storage.root_storage_created


def test_get_cli_help():
    runner = CliRunner()
    res = runner.invoke(cli, ["--help"])
    expected_output = "\n".join(
        [
            "Usage: cli [OPTIONS] COMMAND [ARGS]...",
            "",
            "Options:",
            "  --help  Show this message and exit.",
            "",
            "Commands:",
            "  worker",
            "",
        ]
    )

    assert res.output == expected_output


@mock.patch("main.start_prometheus")
def test_deal_worker_command_default(mock_prometheus, mocker, mock_storage):
    mocker.patch.dict(os.environ, {"HOSTNAME": "simpleworker"})
    mocked_get_current_version = mocker.patch(
        "main.get_current_version", return_value="some_version_12.3"
    )
    mock_app = mocker.patch("main.app")
    runner = CliRunner()
    res = runner.invoke(cli, ["worker"])
    expected_output = "\n".join(
        [
            "INFO: ",
            "  _____          _",
            " / ____|        | |",
            "| |     ___   __| | ___  ___ _____   __",
            "| |    / _ \\ / _` |/ _ \\/ __/ _ \\ \\ / /",
            "| |___| (_) | (_| |  __/ (_| (_) \\ V /",
            " \\_____\\___/ \\__,_|\\___|\\___\\___/ \\_/",
            "                              some_version_12.3",
        ]
    )
    assert res.output.startswith(expected_output)
    mocked_get_current_version.assert_called_with()
    mock_app.celery_app.worker_main.assert_called_with(
        argv=[
            "worker",
            "-n",
            "simpleworker",
            "-c",
            2,
            "-l",
            "info",
            "-Q",
            f"celery,enterprise_celery,{BaseCeleryConfig.health_check_default_queue}",
            "-B",
            "-s",
            "/home/codecov/celerybeat-schedule",
        ]
    )


@mock.patch("main.start_prometheus")
def test_deal_worker_command(mock_prometheus, mocker, mock_storage):
    mocker.patch.dict(os.environ, {"HOSTNAME": "simpleworker"})
    mocked_get_current_version = mocker.patch(
        "main.get_current_version", return_value="some_version_12.3"
    )
    mock_app = mocker.patch("main.app")
    runner = CliRunner()
    res = runner.invoke(cli, ["worker", "--queue", "simple,one,two", "--queue", "some"])
    expected_output = "\n".join(
        [
            "INFO: ",
            "  _____          _",
            " / ____|        | |",
            "| |     ___   __| | ___  ___ _____   __",
            "| |    / _ \\ / _` |/ _ \\/ __/ _ \\ \\ / /",
            "| |___| (_) | (_| |  __/ (_| (_) \\ V /",
            " \\_____\\___/ \\__,_|\\___|\\___\\___/ \\_/",
            "                              some_version_12.3",
        ]
    )
    assert res.output.startswith(expected_output)
    mocked_get_current_version.assert_called_with()
    mock_app.celery_app.worker_main.assert_called_with(
        argv=[
            "worker",
            "-n",
            "simpleworker",
            "-c",
            2,
            "-l",
            "info",
            "-Q",
            f"simple,one,two,some,enterprise_simple,enterprise_one,enterprise_two,enterprise_some,{BaseCeleryConfig.health_check_default_queue}",
            "-B",
            "-s",
            "/home/codecov/celerybeat-schedule",
        ]
    )


@mock.patch("main.start_prometheus")
def test_deal_worker_no_beat(
    mock_prometheus, mocker, mock_storage, empty_configuration
):
    mocker.patch.dict(
        os.environ, {"HOSTNAME": "simpleworker", "SETUP__CELERY_BEAT_ENABLED": "False"}
    )
    mocked_get_current_version = mocker.patch(
        "main.get_current_version", return_value="some_version_12.3"
    )
    mock_app = mocker.patch("main.app")
    runner = CliRunner()
    res = runner.invoke(cli, ["worker", "--queue", "simple,one,two", "--queue", "some"])
    expected_output = "\n".join(
        [
            "INFO: ",
            "  _____          _",
            " / ____|        | |",
            "| |     ___   __| | ___  ___ _____   __",
            "| |    / _ \\ / _` |/ _ \\/ __/ _ \\ \\ / /",
            "| |___| (_) | (_| |  __/ (_| (_) \\ V /",
            " \\_____\\___/ \\__,_|\\___|\\___\\___/ \\_/",
            "                              some_version_12.3",
        ]
    )
    assert res.output.startswith(expected_output)
    mocked_get_current_version.assert_called_with()
    mock_app.celery_app.worker_main.assert_called_with(
        argv=[
            "worker",
            "-n",
            "simpleworker",
            "-c",
            2,
            "-l",
            "info",
            "-Q",
            f"simple,one,two,some,enterprise_simple,enterprise_one,enterprise_two,enterprise_some,{BaseCeleryConfig.health_check_default_queue}",
        ]
    )


@mock.patch("main.start_prometheus")
def test_deal_worker_no_queues(
    mock_prometheus, mocker, mock_storage, empty_configuration
):
    mocker.patch.dict(
        os.environ,
        {"HOSTNAME": "simpleworker", "SETUP__CELERY_QUEUES_ENABLED": "False"},
    )
    mocked_get_current_version = mocker.patch(
        "main.get_current_version", return_value="some_version_12.3"
    )
    mock_app = mocker.patch("main.app")
    runner = CliRunner()
    res = runner.invoke(cli, ["worker", "--queue", "simple,one,two", "--queue", "some"])
    expected_output = "\n".join(
        [
            "INFO: ",
            "  _____          _",
            " / ____|        | |",
            "| |     ___   __| | ___  ___ _____   __",
            "| |    / _ \\ / _` |/ _ \\/ __/ _ \\ \\ / /",
            "| |___| (_) | (_| |  __/ (_| (_) \\ V /",
            " \\_____\\___/ \\__,_|\\___|\\___\\___/ \\_/",
            "                              some_version_12.3",
        ]
    )
    assert res.output.startswith(expected_output)
    mocked_get_current_version.assert_called_with()
    mock_app.celery_app.worker_main.assert_called_with(
        argv=[
            "worker",
            "-n",
            "simpleworker",
            "-c",
            2,
            "-l",
            "info",
            "-B",
            "-s",
            "/home/codecov/celerybeat-schedule",
        ]
    )


@mock.patch("main.start_prometheus")
def test_deal_worker_no_queues_or_beat(
    mock_prometheus, mocker, mock_storage, empty_configuration
):
    env = {
        "HOSTNAME": "simpleworker",
        "SETUP__CELERY_QUEUES_ENABLED": "False",
        "SETUP__CELERY_BEAT_ENABLED": "False",
    }
    mocked_get_current_version = mocker.patch(
        "main.get_current_version", return_value="some_version_12.3"
    )
    mock_app = mocker.patch("main.app")
    runner = CliRunner()
    res = runner.invoke(
        cli, ["worker", "--queue", "simple,one,two", "--queue", "some"], env=env
    )
    expected_output = "\n".join(
        [
            "INFO: ",
            "  _____          _",
            " / ____|        | |",
            "| |     ___   __| | ___  ___ _____   __",
            "| |    / _ \\ / _` |/ _ \\/ __/ _ \\ \\ / /",
            "| |___| (_) | (_| |  __/ (_| (_) \\ V /",
            " \\_____\\___/ \\__,_|\\___|\\___\\___/ \\_/",
            "                              some_version_12.3",
        ]
    )
    assert res.output.startswith(expected_output)
    mocked_get_current_version.assert_called_with()
    mock_app.celery_app.worker_main.assert_called_with(
        argv=[
            "worker",
            "-n",
            "simpleworker",
            "-c",
            2,
            "-l",
            "info",
        ]
    )


def test_main(mocker):
    mock_cli = mocker.patch("main.cli")
    assert main() is None
    mock_cli.assert_called_with(obj={})
