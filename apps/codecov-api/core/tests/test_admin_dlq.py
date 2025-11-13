from unittest.mock import MagicMock, patch

from django.contrib.messages import get_messages
from django.test import TestCase
from django.urls import reverse

from shared.django_apps.codecov_auth.tests.factories import UserFactory
from utils.test_utils import Client


class DLQAdminTest(TestCase):
    def setUp(self):
        self.user = UserFactory()
        self.client = Client()

    def test_staff_can_access_dlq_list(self):
        """Test that staff users can access the DLQ list page."""
        self.user.is_staff = True
        self.user.save()
        self.client.force_login(self.user)

        with patch("core.admin_dlq.celery_app") as mock_celery_app:
            mock_result = MagicMock()
            mock_result.get.return_value = {
                "success": True,
                "keys": [],
                "total_keys": 0,
            }
            mock_celery_app.send_task.return_value = mock_result

            response = self.client.get(reverse("admin:core_dlq_list"))
            self.assertEqual(response.status_code, 200)
            self.assertContains(response, "Dead Letter Queue")

    def test_non_staff_cannot_access_dlq_list(self):
        """Test that non-staff users cannot access the DLQ list page."""
        self.client.force_login(self.user)
        response = self.client.get(reverse("admin:core_dlq_list"))
        self.assertEqual(response.status_code, 302)  # Redirect to login

    def test_dlq_list_displays_keys(self):
        """Test that DLQ list page displays DLQ keys correctly."""
        self.user.is_staff = True
        self.user.save()
        self.client.force_login(self.user)

        mock_keys = [
            {
                "key": "task_dlq/app.tasks.upload.Upload/123/abc123",
                "count": 2,
                "ttl_seconds": 604800,
            },
            {
                "key": "task_dlq/app.tasks.upload_processor.UploadProcessor/456/def456",
                "count": 1,
                "ttl_seconds": 604800,
            },
        ]

        with patch("core.admin_dlq.celery_app") as mock_celery_app:
            mock_result = MagicMock()
            mock_result.get.return_value = {
                "success": True,
                "keys": mock_keys,
                "total_keys": 2,
            }
            mock_celery_app.send_task.return_value = mock_result

            response = self.client.get(reverse("admin:core_dlq_list"))
            self.assertEqual(response.status_code, 200)
            self.assertContains(response, "task_dlq/app.tasks.upload.Upload/123/abc123")
            self.assertContains(
                response,
                "task_dlq/app.tasks.upload_processor.UploadProcessor/456/def456",
            )
            self.assertContains(response, "Total DLQ keys: <strong>2</strong>")

    def test_dlq_list_with_task_name_filter(self):
        """Test that DLQ list can be filtered by task name."""
        self.user.is_staff = True
        self.user.save()
        self.client.force_login(self.user)

        mock_keys = [
            {
                "key": "task_dlq/app.tasks.upload.Upload/123/abc123",
                "count": 1,
                "ttl_seconds": 604800,
            },
        ]

        with patch("core.admin_dlq.celery_app") as mock_celery_app:
            mock_result = MagicMock()
            mock_result.get.return_value = {
                "success": True,
                "keys": mock_keys,
                "total_keys": 1,
            }
            mock_celery_app.send_task.return_value = mock_result

            response = self.client.get(
                reverse("admin:core_dlq_list"), {"task_name": "app.tasks.upload.Upload"}
            )
            self.assertEqual(response.status_code, 200)
            # Verify filter was passed to task
            mock_celery_app.send_task.assert_called_once()
            call_kwargs = mock_celery_app.send_task.call_args[1]["kwargs"]
            self.assertEqual(call_kwargs["task_name_filter"], "app.tasks.upload.Upload")

    def test_dlq_list_handles_error(self):
        """Test that DLQ list handles errors gracefully."""
        self.user.is_staff = True
        self.user.save()
        self.client.force_login(self.user)

        with patch("core.admin_dlq.celery_app") as mock_celery_app:
            mock_result = MagicMock()
            mock_result.get.return_value = {
                "success": False,
                "error": "Redis connection failed",
            }
            mock_celery_app.send_task.return_value = mock_result

            response = self.client.get(reverse("admin:core_dlq_list"))
            self.assertEqual(response.status_code, 302)  # Redirect to admin index
            messages = list(get_messages(response.wsgi_request))
            self.assertEqual(len(messages), 1)
            self.assertIn("Failed to list DLQ keys", str(messages[0]))

    def test_recover_dlq_success(self):
        """Test recovering tasks from DLQ."""
        self.user.is_staff = True
        self.user.save()
        self.client.force_login(self.user)

        dlq_key = "task_dlq/app.tasks.upload.Upload/123/abc123"

        with patch("core.admin_dlq.celery_app") as mock_celery_app:
            mock_result = MagicMock()
            mock_result.get.return_value = {
                "success": True,
                "recovered_count": 2,
                "failed_count": 0,
            }
            mock_celery_app.send_task.return_value = mock_result

            response = self.client.get(
                reverse("admin:core_dlq_recover", kwargs={"dlq_key": dlq_key})
            )
            self.assertEqual(response.status_code, 302)  # Redirect to list
            self.assertEqual(response.url, reverse("admin:core_dlq_list"))

            # Verify task was called with correct parameters
            mock_celery_app.send_task.assert_called_once()
            call_kwargs = mock_celery_app.send_task.call_args[1]["kwargs"]
            self.assertEqual(call_kwargs["action"], "recover")
            self.assertEqual(call_kwargs["dlq_key"], dlq_key)

    def test_recover_dlq_with_failures(self):
        """Test recovering tasks from DLQ when some fail."""
        self.user.is_staff = True
        self.user.save()
        self.client.force_login(self.user)

        dlq_key = "task_dlq/app.tasks.upload.Upload/123/abc123"

        with patch("core.admin_dlq.celery_app") as mock_celery_app:
            mock_result = MagicMock()
            mock_result.get.return_value = {
                "success": True,
                "recovered_count": 1,
                "failed_count": 1,
            }
            mock_celery_app.send_task.return_value = mock_result

            response = self.client.get(
                reverse("admin:core_dlq_recover", kwargs={"dlq_key": dlq_key})
            )
            self.assertEqual(response.status_code, 302)
            messages = list(get_messages(response.wsgi_request))
            # Should have both success and warning messages
            self.assertGreaterEqual(len(messages), 1)
            message_texts = [str(m) for m in messages]
            self.assertTrue(
                any("Successfully recovered" in msg for msg in message_texts)
            )
            self.assertTrue(any("Failed to recover" in msg for msg in message_texts))

    def test_recover_dlq_error(self):
        """Test handling errors when recovering DLQ tasks."""
        self.user.is_staff = True
        self.user.save()
        self.client.force_login(self.user)

        dlq_key = "task_dlq/app.tasks.upload.Upload/123/abc123"

        with patch("core.admin_dlq.celery_app") as mock_celery_app:
            mock_result = MagicMock()
            mock_result.get.return_value = {
                "success": False,
                "error": "DLQ key not found",
            }
            mock_celery_app.send_task.return_value = mock_result

            response = self.client.get(
                reverse("admin:core_dlq_recover", kwargs={"dlq_key": dlq_key})
            )
            self.assertEqual(response.status_code, 302)
            messages = list(get_messages(response.wsgi_request))
            self.assertEqual(len(messages), 1)
            self.assertIn("Failed to recover tasks", str(messages[0]))

    def test_delete_dlq_success(self):
        """Test deleting tasks from DLQ."""
        self.user.is_staff = True
        self.user.save()
        self.client.force_login(self.user)

        dlq_key = "task_dlq/app.tasks.upload.Upload/123/abc123"

        with patch("core.admin_dlq.celery_app") as mock_celery_app:
            mock_result = MagicMock()
            mock_result.get.return_value = {
                "success": True,
                "deleted_count": 2,
            }
            mock_celery_app.send_task.return_value = mock_result

            response = self.client.get(
                reverse("admin:core_dlq_delete", kwargs={"dlq_key": dlq_key})
            )
            self.assertEqual(response.status_code, 302)  # Redirect to list
            self.assertEqual(response.url, reverse("admin:core_dlq_list"))

            # Verify task was called with correct parameters
            mock_celery_app.send_task.assert_called_once()
            call_kwargs = mock_celery_app.send_task.call_args[1]["kwargs"]
            self.assertEqual(call_kwargs["action"], "delete")
            self.assertEqual(call_kwargs["dlq_key"], dlq_key)

    def test_delete_dlq_error(self):
        """Test handling errors when deleting DLQ tasks."""
        self.user.is_staff = True
        self.user.save()
        self.client.force_login(self.user)

        dlq_key = "task_dlq/app.tasks.upload.Upload/123/abc123"

        with patch("core.admin_dlq.celery_app") as mock_celery_app:
            mock_result = MagicMock()
            mock_result.get.return_value = {
                "success": False,
                "error": "DLQ key not found",
            }
            mock_celery_app.send_task.return_value = mock_result

            response = self.client.get(
                reverse("admin:core_dlq_delete", kwargs={"dlq_key": dlq_key})
            )
            self.assertEqual(response.status_code, 302)
            messages = list(get_messages(response.wsgi_request))
            self.assertEqual(len(messages), 1)
            self.assertIn("Failed to delete tasks", str(messages[0]))

    def test_dlq_list_empty_state(self):
        """Test DLQ list page when there are no keys."""
        self.user.is_staff = True
        self.user.save()
        self.client.force_login(self.user)

        with patch("core.admin_dlq.celery_app") as mock_celery_app:
            mock_result = MagicMock()
            mock_result.get.return_value = {
                "success": True,
                "keys": [],
                "total_keys": 0,
            }
            mock_celery_app.send_task.return_value = mock_result

            response = self.client.get(reverse("admin:core_dlq_list"))
            self.assertEqual(response.status_code, 200)
            self.assertContains(response, "No DLQ keys found")
