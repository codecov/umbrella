from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("codecov_auth", "0081_case_insensitive_text_fields"),
    ]

    operations = [
        migrations.CreateModel(
            name="PlanChange",
            fields=[
                ("id", models.BigAutoField(primary_key=True, serialize=False)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "entity",
                    models.CharField(
                        choices=[("owner", "Owner"), ("account", "Account")],
                        max_length=16,
                    ),
                ),
                ("owner_id", models.IntegerField(blank=True, null=True)),
                ("account_id", models.BigIntegerField(blank=True, null=True)),
                ("service", models.TextField(blank=True, null=True)),
                ("username", models.TextField(blank=True, null=True)),
                ("old_plan", models.TextField(blank=True, null=True)),
                ("new_plan", models.TextField(blank=True, null=True)),
                ("changes", models.JSONField(blank=True, default=dict)),
                ("snapshot", models.JSONField(blank=True, default=dict)),
                ("actor", models.JSONField(blank=True, default=dict)),
                ("source", models.TextField(blank=True, null=True)),
                ("caller", models.JSONField(blank=True, default=list)),
            ],
            options={
                "db_table": "codecov_auth_planchange",
                "ordering": ["-created_at"],
                "indexes": [
                    models.Index(
                        fields=["owner_id", "created_at"], name="plan_change_owner_idx"
                    ),
                    models.Index(
                        fields=["account_id", "created_at"],
                        name="plan_change_account_idx",
                    ),
                ],
            },
        ),
    ]
