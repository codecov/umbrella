from django.contrib.postgres.operations import AddIndexConcurrently
from django.db import migrations, models


class Migration(migrations.Migration):
    """Add a btree index on `users.email`.

    The `users` table can be very large, so the index is built with
    CREATE INDEX CONCURRENTLY to avoid locking the table against writes.
    CONCURRENTLY cannot run inside a transaction, hence `atomic = False`.

    `email` is a citext column; its default btree operator class is
    case-insensitive, so this index backs the `email=` equality lookups used
    when linking an Okta identity to an existing user.
    """

    atomic = False

    dependencies = [
        ("codecov_auth", "0079_ownertobedeleted_requested_by_and_on_hold"),
    ]

    operations = [
        AddIndexConcurrently(
            model_name="user",
            index=models.Index(fields=["email"], name="users_email_idx"),
        ),
    ]
