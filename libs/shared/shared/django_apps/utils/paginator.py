from django.core.paginator import Paginator
from django.db import connections
from django.utils.functional import cached_property


class EstimatedCountPaginator(Paginator):
    """
    Overrides the count method of QuerySet objects to avoid timeouts.

    Based on: https://gist.github.com/adinhodovic/f27f6c466900086a914e113e1d41031c

    * Get an estimate instead of actual count when not filtered (this estimate
      can be stale and hence not fit for situations where the count of objects
      actually matters)
    * If any other exception occurred fall back to the default behaviour
    """

    @cached_property
    def count(self) -> int:
        """
        Returns an estimated number of objects, across all pages.
        """
        if not self.object_list.query.where:  # type: ignore
            try:
                with connections[self.object_list.db].cursor() as cursor:  # type: ignore
                    # Obtain estimated values (only valid with PostgreSQL)

                    table_name = self.object_list.query.model._meta.db_table  # type: ignore

                    # Check if the table is partitioned
                    cursor.execute(
                        """
                        SELECT EXISTS (
                            SELECT 1 FROM pg_inherits i
                            JOIN pg_class p ON i.inhparent = p.oid
                            WHERE p.relname = %s
                        )
                    """,
                        [table_name],
                    )

                    is_partitioned = cursor.fetchone()[0]

                    if is_partitioned:
                        # For partitioned tables, sum estimates across all partitions
                        cursor.execute(
                            """
                            SELECT SUM(c.reltuples)::bigint as estimated_count
                            FROM pg_class c
                            JOIN pg_inherits i ON c.oid = i.inhrelid
                            JOIN pg_class p ON i.inhparent = p.oid
                            WHERE p.relname = %s
                            AND c.relkind = 'r'
                        """,
                            [table_name],
                        )
                    else:
                        # For regular tables, use the simple query
                        cursor.execute(
                            "SELECT reltuples FROM pg_class WHERE relname = %s",
                            [table_name],
                        )

                    result = cursor.fetchone()
                    estimate = int(result[0]) if result and result[0] is not None else 0
                    return estimate
            except Exception:
                # If any other exception occurred fall back to default behaviour
                pass
        return super().count
