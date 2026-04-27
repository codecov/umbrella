from django.apps import AppConfig


class RedisAdminConfig(AppConfig):
    name = "redis_admin"
    verbose_name = "Redis Queues"
    default_auto_field = "django.db.models.BigAutoField"
