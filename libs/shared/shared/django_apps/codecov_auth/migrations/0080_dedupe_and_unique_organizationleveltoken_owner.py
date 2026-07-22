from django.db import migrations, models


def deduplicate_org_tokens(apps, schema_editor):
    """Keep only the most recently created token per owner, delete the rest."""
    OrganizationLevelToken = apps.get_model(
        "codecov_auth", "OrganizationLevelToken"
    )
    # Collect owner IDs that have more than one token
    seen = set()
    for token in OrganizationLevelToken.objects.order_by("owner_id", "-id"):
        if token.owner_id in seen:
            token.delete()
        else:
            seen.add(token.owner_id)


class Migration(migrations.Migration):
    dependencies = [
        ("codecov_auth", "0079_ownertobedeleted_requested_by_and_on_hold"),
    ]

    operations = [
        # Step 1: remove duplicate rows, keeping the newest per owner
        migrations.RunPython(
            deduplicate_org_tokens,
            migrations.RunPython.noop,
        ),
        # Step 2: enforce uniqueness at the DB level
        migrations.AlterField(
            model_name="organizationleveltoken",
            name="owner",
            field=models.OneToOneField(
                db_column="ownerid",
                on_delete=models.deletion.CASCADE,
                related_name="organization_tokens",
                to="codecov_auth.owner",
            ),
        ),
    ]