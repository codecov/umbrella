# Agent Guidelines

Rules for AI coding agents working in this repository.

## Pre-commit Checks

Before creating a PR or making a commit, always run lint and format checks:

```bash
ruff check --fix .
ruff format .
```

If either command produces changes, stage the fixes and include them in the commit.

This project uses `ruff` for both linting and formatting, configured in `pyproject.toml` and enforced via `.pre-commit-config.yaml`.

## Small, Focused PRs

When a task involves multiple logical changes, split them into separate PRs rather than one large PR.

- Each PR should do **one thing**: a single refactor, a single consolidation, a single feature addition.
- If a set of changes naturally stacks (B depends on A), create stacked PRs with the base set to the previous branch.
- Aim for PRs that are **easy to review in one sitting** — a reviewer should be able to understand the intent from the title alone.

### Examples of good splits

- Moving definitions to a new location (bulk import change) = 1 PR
- Consolidating a duplicate enum/constant to its canonical source = 1 PR per enum
- Adding a new method to a model + migrating callers to use it = 2 PRs

### When creating stacked PRs

1. Create a branch per logical change, each based on the previous.
2. Push all branches.
3. Open draft PRs with each PR's base set to the previous branch.
4. Note the dependency in the PR body (e.g. "Stacked on #123").

## Capture Learnings

### Workflow learnings

When you discover a workflow pattern that should be repeated — or a mistake that should be avoided — add it to this file or create a rule in `.cursor/rules/`.

Examples worth capturing:

- A git workflow that worked well (e.g. how to structure stacked PRs)
- A step that was missed and caused rework (e.g. forgetting to run linters)
- A review or PR convention the team expects
- A process for splitting, sequencing, or batching changes

### Technical learnings

When you discover a technical pattern, gotcha, or convention specific to this codebase, add it to this file or create a rule in `.cursor/rules/`.

Examples worth capturing:

- ORM differences between SQLAlchemy and Django models (field names, method names, enum types)
- Import conventions (canonical locations for shared enums, models, constants)
- Compatibility gotchas (e.g. `TextChoices` vs plain `Enum`, `cached_property` vs `property`)
- Test patterns (which factories to use, which fixtures, `@pytest.mark.django_db`)
- Architecture decisions (where shared code lives, what belongs in `libs/shared/` vs `apps/worker/`)

Write learnings as concise, actionable references — something a developer or agent can consult mid-task. Include concrete examples where possible.
