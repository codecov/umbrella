"""
Django Admin interface for Dead Letter Queue (DLQ) management.

This provides a web-based interface for inspecting and recovering tasks
that were saved to the DLQ after exhausting all retries.
"""

from urllib.parse import unquote

from django.contrib import admin, messages
from django.http import HttpResponseRedirect
from django.shortcuts import render
from django.urls import path, reverse

from services.task.task import celery_app
from shared.celery_config import dlq_recovery_task_name


def list_dlq(request):
    """List all DLQ keys."""
    task_name_filter = request.GET.get("task_name", None)
    # Call DLQ recovery task synchronously via Celery
    # The task is registered in the worker app, but send_task will route it correctly
    result = celery_app.send_task(
        dlq_recovery_task_name,
        kwargs={"action": "list", "task_name_filter": task_name_filter},
    ).get(timeout=30)  # 30 second timeout for admin operations

    if result.get("success"):
        keys = result.get("keys", [])
        context = {
            **admin.site.each_context(request),
            "title": "Dead Letter Queue",
            "keys": keys,
            "total_keys": result.get("total_keys", 0),
            "task_name_filter": task_name_filter,
            "opts": {"app_label": "core", "model_name": "dlq"},
        }
        return render(request, "admin/core/dlq_list.html", context)
    else:
        messages.error(request, f"Failed to list DLQ keys: {result.get('error')}")
        return HttpResponseRedirect(reverse("admin:index"))


def recover_dlq(request, dlq_key):
    """Recover tasks from a DLQ key."""
    dlq_key = unquote(dlq_key)  # Decode URL-encoded key
    # Call DLQ recovery task synchronously via Celery
    result = celery_app.send_task(
        dlq_recovery_task_name,
        kwargs={"action": "recover", "dlq_key": dlq_key},
    ).get(timeout=60)  # 60 second timeout for recovery operations

    if result.get("success"):
        recovered = result.get("recovered_count", 0)
        failed = result.get("failed_count", 0)
        if recovered > 0:
            messages.success(
                request,
                f"Successfully recovered {recovered} task(s) from DLQ key: {dlq_key}",
            )
        if failed > 0:
            messages.warning(
                request,
                f"Failed to recover {failed} task(s) from DLQ key: {dlq_key}",
            )
    else:
        messages.error(
            request, f"Failed to recover tasks: {result.get('error', 'Unknown error')}"
        )

    return HttpResponseRedirect(reverse("admin:core_dlq_list"))


def delete_dlq(request, dlq_key):
    """Delete tasks from a DLQ key."""
    dlq_key = unquote(dlq_key)  # Decode URL-encoded key
    # Call DLQ recovery task synchronously via Celery
    result = celery_app.send_task(
        dlq_recovery_task_name,
        kwargs={"action": "delete", "dlq_key": dlq_key},
    ).get(timeout=30)  # 30 second timeout for delete operations

    if result.get("success"):
        deleted = result.get("deleted_count", 0)
        messages.success(
            request,
            f"Successfully deleted {deleted} task(s) from DLQ key: {dlq_key}",
        )
    else:
        messages.error(
            request, f"Failed to delete tasks: {result.get('error', 'Unknown error')}"
        )

    return HttpResponseRedirect(reverse("admin:core_dlq_list"))


# Register DLQ admin URLs by extending admin site's get_urls
_original_get_urls = admin.site.get_urls


def get_urls_with_dlq():
    """Add DLQ URLs to admin site."""
    dlq_urls = [
        path("dlq/", admin.site.admin_view(list_dlq), name="core_dlq_list"),
        path(
            "dlq/recover/<str:dlq_key>/",
            admin.site.admin_view(recover_dlq),
            name="core_dlq_recover",
        ),
        path(
            "dlq/delete/<str:dlq_key>/",
            admin.site.admin_view(delete_dlq),
            name="core_dlq_delete",
        ),
    ]
    return dlq_urls + _original_get_urls()


admin.site.get_urls = get_urls_with_dlq
