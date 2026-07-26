from core.incident_replay import (
    INCIDENT_REPLAY_BATCH_CAP,
    build_incident_replay_preview,
    parse_incident_replay_input,
)
from shared.django_apps.core.tests.factories import CommitFactory, RepositoryFactory


def test_parse_incident_replay_input_accepts_repoid_sha_pairs():
    raw = "19160257 12ab23c58acfd7c7ade6ec60857b3340f5965287\n# comment\n19160257 6ef2ca68f44883374e31f03485dc72239370fa80"
    pairs, errors, duplicate_count = parse_incident_replay_input(raw)

    assert duplicate_count == 0
    assert errors == []
    assert len(pairs) == 2
    assert pairs[0].repoid == 19160257
    assert pairs[0].commit_sha == "12ab23c58acfd7c7ade6ec60857b3340f5965287"


def test_parse_incident_replay_input_rejects_invalid_lines():
    pairs, errors, duplicate_count = parse_incident_replay_input("abc deadbeef")

    assert pairs == []
    assert duplicate_count == 0
    assert len(errors) == 1


def test_build_incident_replay_preview_resolves_and_deduplicates(db):
    repository = RepositoryFactory()
    commit = CommitFactory(
        repository=repository,
        commitid="12ab23c58acfd7c7ade6ec60857b3340f5965287",
    )
    raw = "\n".join(
        [
            f"{repository.repoid} {commit.commitid}",
            f"{repository.repoid} {commit.commitid.upper()}",
            f"{repository.repoid} 0000000000000000000000000000000000000000",
        ]
    )

    preview = build_incident_replay_preview(raw)

    assert preview.duplicate_count == 1
    assert len(preview.resolved) == 1
    assert preview.resolved[0][1].pk == commit.pk
    assert len(preview.missing) == 1


def test_build_incident_replay_preview_truncates_to_batch_cap(db):
    repository = RepositoryFactory()
    lines = []
    for index in range(INCIDENT_REPLAY_BATCH_CAP + 5):
        commit = CommitFactory(
            repository=repository,
            commitid=f"{index:040x}",
        )
        lines.append(f"{repository.repoid} {commit.commitid}")

    preview = build_incident_replay_preview("\n".join(lines))

    assert preview.truncated is True
    assert len(preview.parsed_pairs) == INCIDENT_REPLAY_BATCH_CAP
    assert len(preview.resolved) == INCIDENT_REPLAY_BATCH_CAP
