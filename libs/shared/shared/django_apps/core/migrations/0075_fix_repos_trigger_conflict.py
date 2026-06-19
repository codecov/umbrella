# Fix for repos trigger conflict issue
# This migration fixes the repos_before_insert_or_update trigger to prevent
# it from trying to update the same row that's currently being updated

from django.db import migrations

from shared.django_apps.migration_utils import RiskyRunSQL


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0074_increment_version"),
    ]

    operations = [
        RiskyRunSQL(
            """
            -- Fix the repos trigger to use case-insensitive comparison
            -- This prevents the trigger from firing on case-only changes like 'Qaptain' -> 'qaptain'
            
            -- Update the trigger condition to be case-insensitive
            drop trigger if exists repos_before_update on repos;
            
            create trigger repos_before_update before update on repos
            for each row
            when (new.name is not null and lower(new.name) is distinct from lower(coalesce(old.name, '')))
            execute procedure repos_before_insert_or_update();
            """,
            reverse_sql="""
            -- Revert to original trigger condition
            drop trigger if exists repos_before_update on repos;
            
            create trigger repos_before_update before update on repos
            for each row
            when (new.name is not null and new.name is distinct from old.name)
            execute procedure repos_before_insert_or_update();
            """,
        )
    ]
