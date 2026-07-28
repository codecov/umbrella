import logging
import re
from dataclasses import dataclass, field

from django.db.models import Q

from core.models import Commit

log = logging.getLogger(__name__)

INCIDENT_REPLAY_BATCH_CAP = 500
_PAIR_LINE = re.compile(r"^\s*(\d+)\s+([0-9a-fA-F]{40})\s*$")


@dataclass
class IncidentReplayLine:
    repoid: int
    commit_sha: str
    line_number: int


@dataclass
class IncidentReplayParseError:
    line_number: int
    message: str


@dataclass
class IncidentReplayPreview:
    parsed_pairs: list[IncidentReplayLine] = field(default_factory=list)
    parse_errors: list[IncidentReplayParseError] = field(default_factory=list)
    duplicate_count: int = 0
    resolved: list[tuple[IncidentReplayLine, Commit]] = field(default_factory=list)
    missing: list[IncidentReplayLine] = field(default_factory=list)
    truncated: bool = False


def parse_incident_replay_input(
    raw: str,
) -> tuple[list[IncidentReplayLine], list[IncidentReplayParseError], int]:
    pairs: list[IncidentReplayLine] = []
    errors: list[IncidentReplayParseError] = []
    seen: set[tuple[int, str]] = set()
    duplicate_count = 0

    for line_number, line in enumerate(raw.splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        match = _PAIR_LINE.match(stripped)
        if not match:
            errors.append(
                IncidentReplayParseError(
                    line_number=line_number,
                    message="Expected '<repoid> <40-char SHA>'",
                )
            )
            continue
        repoid = int(match.group(1))
        commit_sha = match.group(2).lower()
        key = (repoid, commit_sha)
        if key in seen:
            duplicate_count += 1
            continue
        seen.add(key)
        pairs.append(
            IncidentReplayLine(
                repoid=repoid, commit_sha=commit_sha, line_number=line_number
            )
        )

    return pairs, errors, duplicate_count


def build_incident_replay_preview(raw: str) -> IncidentReplayPreview:
    pairs, errors, duplicate_count = parse_incident_replay_input(raw)
    preview = IncidentReplayPreview(
        parse_errors=errors,
        duplicate_count=duplicate_count,
    )
    if errors:
        return preview

    if len(pairs) > INCIDENT_REPLAY_BATCH_CAP:
        preview.truncated = True
        pairs = pairs[:INCIDENT_REPLAY_BATCH_CAP]

    preview.parsed_pairs = pairs
    if not pairs:
        return preview

    repoids = {pair.repoid for pair in pairs}
    sha_by_repoid: dict[int, set[str]] = {}
    for pair in pairs:
        sha_by_repoid.setdefault(pair.repoid, set()).add(pair.commit_sha)

    commit_filter = Q()
    for repoid, shas in sha_by_repoid.items():
        commit_filter |= Q(repository_id=repoid, commitid__in=list(shas))

    commits = Commit.objects.filter(commit_filter).select_related(
        "repository", "repository__author"
    )
    commit_by_key = {
        (commit.repository_id, commit.commitid.lower()): commit for commit in commits
    }

    for pair in pairs:
        commit = commit_by_key.get((pair.repoid, pair.commit_sha))
        if commit is None:
            preview.missing.append(pair)
        else:
            preview.resolved.append((pair, commit))

    return preview


def format_repo_slug(commit: Commit) -> str:
    repo = commit.repository
    if repo is None:
        return str(commit.repository_id)
    author = repo.author
    if author is None:
        return repo.name
    return f"{author.service}:{author.username}/{repo.name}"
