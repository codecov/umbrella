import os
import sys
from unittest import mock

import pytest

from shared.django_apps.manage import main


@mock.patch.dict(os.environ, {}, clear=True)
@mock.patch("django.core.management.execute_from_command_line")
def test_main_sets_django_settings_module(mock_execute_command):
    """Test that main() sets the DJANGO_SETTINGS_MODULE environment variable."""
    # Environment variables are cleared by the mock.patch.dict decorator

    # Call the main function
    main()

    # Assert DJANGO_SETTINGS_MODULE was set correctly
    assert (
        os.environ.get("DJANGO_SETTINGS_MODULE") == "shared.django_apps.settings_test"
    )

    # Verify execute_from_command_line was called once
    mock_execute_command.assert_called_once_with(sys.argv)


def test_main_handles_django_import_error():
    """Test that main() properly handles Django import errors."""
    # We need to patch the import itself, which requires patching __import__
    original_import = __import__

    def mock_import(name, *args, **kwargs):
        if name == "django.core.management" or name == "django":
            raise ImportError("Mock Django import error")
        return original_import(name, *args, **kwargs)

    # Use the context manager approach to patch builtins.__import__
    with mock.patch("builtins.__import__", side_effect=mock_import):
        # Assert the specific ImportError message is raised
        with pytest.raises(ImportError) as excinfo:
            main()

        error_message = str(excinfo.value)
        assert "Couldn't import Django" in error_message
        assert "PYTHONPATH environment variable" in error_message
        assert "virtual environment" in error_message
        assert "Mock Django import error" in str(excinfo.value.__cause__)
