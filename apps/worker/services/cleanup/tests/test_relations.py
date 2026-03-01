from unittest.mock import patch

import pytest
import sqlparse
from django.core.exceptions import EmptyResultSet
from django.db.models.query import QuerySet
from django.db.models.sql.subqueries import DeleteQuery

from services.cleanup.relations import (
    _chunked_in_filter,
    build_relation_graph,
    simplified_lookup,
)
from shared.django_apps.codecov_auth.models import Owner
from shared.django_apps.core.models import Repository
from shared.django_apps.reports.models import UploadLevelTotals


def dump_delete_queries(queryset: QuerySet) -> str:
    relations = build_relation_graph(queryset)

    queries = ""
    for relation in relations:
        if queries:
            queries += "\n\n"
        queries += f"-- {relation.model.__name__}\n"

        for query in relation.querysets:
            compiler = query.query.chain(DeleteQuery).get_compiler(query.db)
            try:
                sql, params = compiler.as_sql()
            except EmptyResultSet:
                queries += "-- (empty)\n"
                continue
            sql = sqlparse.format(sql, reindent=True, keyword_case="upper")
            queries += sql + ";\n"
            if params:
                queries += "-- ["
                for i, param in enumerate(params):
                    if i:
                        queries += ", "
                    queries += str(param)
                queries += "]\n"

    return queries


@pytest.mark.django_db
def test_builds_delete_queries(snapshot):
    repo = Repository.objects.filter(repoid=123)
    org = Owner.objects.filter(ownerid=123)

    # if you change any of the model relations, this snapshot will most likely change.
    # in that case, feel free to update this using `pytest --insta update`.
    assert dump_delete_queries(repo) == snapshot("repository.txt")
    assert dump_delete_queries(org) == snapshot("owner.txt")


@pytest.mark.django_db
def test_can_simplify_queries():
    repo = Repository.objects.filter(repoid=123)
    assert simplified_lookup(repo) == [123]

    repo = Repository.objects.filter(repoid__in=[123, 456])
    assert simplified_lookup(repo) == [123, 456]

    repo = Repository.objects.filter(fork=123)
    assert simplified_lookup(repo) == []

    owner_repos = Repository.objects.filter(author=123)
    repo = Repository.objects.filter(repoid__in=owner_repos)
    assert simplified_lookup(repo) == []

    # Over-threshold: falls back to the original queryset.
    # Threshold of -1 ensures any result (even empty) exceeds it.
    repo = Repository.objects.filter(fork=123)
    with patch("services.cleanup.relations._get_eager_eval_threshold", return_value=-1):
        assert simplified_lookup(repo) == repo


@pytest.mark.django_db
def test_leaf_table(snapshot):
    query = UploadLevelTotals.objects.all()
    assert dump_delete_queries(query) == snapshot("leaf.txt")


def test_chunked_in_filter_passthrough():
    # QuerySets are yielded unchanged
    qs = Repository.objects.filter(repoid=1)
    assert list(_chunked_in_filter(qs)) == [qs]


def test_chunked_in_filter_empty_list():
    # An empty list produces exactly one empty chunk so downstream
    # filters generate an empty queryset rather than being skipped.
    assert list(_chunked_in_filter([])) == [[]]


def test_chunked_in_filter_splits():
    # 3 IDs with chunk_size=2 → [[1, 2], [3]]
    with patch("services.cleanup.relations._get_delete_chunk_size", return_value=2):
        assert list(_chunked_in_filter([1, 2, 3])) == [[1, 2], [3]]


@pytest.mark.django_db
def test_chunks_large_in_filter():
    """build_relation_graph produces multiple querysets when the materialized
    ID list exceeds the chunk size."""
    # Use a chunk size of 2 so that 3 upload IDs → 2 querysets on child tables.
    with patch(
        "services.cleanup.relations._get_delete_chunk_size", return_value=2
    ), patch(
        "services.cleanup.relations._get_eager_eval_threshold", return_value=10
    ):
        # simplified_lookup will materialise the repoid __in list [1, 2, 3]
        repo_qs = Repository.objects.filter(repoid__in=[1, 2, 3])
        relations = build_relation_graph(repo_qs)

    repo_relation = next(r for r in relations if r.model is Repository)
    # The root queryset is never split (it is the original QuerySet, not a list)
    assert len(repo_relation.querysets) == 1

    # At least one child model should have more than one queryset due to chunking
    child_counts = [len(r.querysets) for r in relations if r.model is not Repository]
    assert any(count > 1 for count in child_counts), (
        "Expected at least one child model to have multiple querysets from chunking, "
        f"but got counts: {child_counts}"
    )
