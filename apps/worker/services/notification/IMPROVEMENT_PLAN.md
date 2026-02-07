# Notification System Improvement Plan

## Overview

The coverage notification system (GitHub status checks, PR comments, webhooks, chat
integrations) has been identified as difficult to work with. This document catalogs the
specific issues and proposes concrete improvements.

The bundle analysis notification system (`services/bundle_analysis/notify/`) was built
later and is meaningfully cleaner. Several of the proposals below take direct inspiration
from its architecture.

---

## Part 1: Issues

### 1.1 — God Method in the Celery Task Entry Point

**File:** `tasks/notify.py` — `NotifyTask.run_impl_within_lock` (~320 lines)

This single method handles commit validation, bot resolution, YAML loading, CI status
checking, retry logic, report fetching, pull request resolution, GitLab-specific SHA
handling, and notification dispatch. It has 5+ levels of nesting in places and at least
8 near-identical early-return dictionaries (`{"notified": False, "notifications": None,
"reason": "..."}`).

Other issues in this file:
- `submit_third_party_notifications` takes **13 parameters**, signaling a missing data
  object.
- Dead code: `GENERIC_TA_ERROR_MSG` is defined but unused.
- A bare `assert` is used for commit existence (disabled by `python -O`).
- A `log.info()` call is accidentally wrapped in a tuple via a trailing comma.
- Repetitive breadcrumb-logging boilerplate on every exit path.

### 1.2 — Inheritance Hierarchy Is Deep and Misleading

```
AbstractBaseNotifier
  └── StatusNotifier
        └── ChecksNotifier          # overrides notify() AND send_notification() entirely
              └── ProjectChecksNotifier  (+ MessageMixin + StatusProjectMixin)
```

`ChecksNotifier` extends `StatusNotifier` but **completely overrides** both `notify()`
and `send_notification()` without calling `super()`. It is not a specialization — it is
an alternative implementation disguised as a subclass. The only reuse is a handful of
utility methods (`can_we_set_this_status`, `required_builds`, etc.).

Consequence: to trace a single `PatchChecksNotifier.notify()` call, a developer must
read **6 files and 5 classes** in MRO order, remembering which methods are inherited
and which are shadowed.

### 1.3 — Massive Code Duplication Between Status and Checks Notifiers

The flag-coverage logic in `StatusNotifier.notify()` (`status/base.py:146-243`) is
nearly copy-pasted into `ChecksNotifier.notify()` (`checks/base.py:92-268`). The only
differences are the payload keys they mutate (`payload["message"]` vs.
`payload["output"]["summary"]`). A bug fix in one copy is easily missed in the other.

Additional duplications:
- `get_notifier_filters()` is byte-for-byte identical in both classes.
- `_get_threshold()` is independently defined in both `StatusPatchMixin` and
  `StatusProjectMixin` with identical implementations.
- `TorngitClientError` / `TorngitError` catch blocks appear in 5+ separate locations
  with slightly different handling each time.

### 1.4 — Mixins Use an Implicit, Untyped Contract

`StatusPatchMixin`, `StatusChangesMixin`, `StatusProjectMixin`, and `MessageMixin` all
freely access `self.notifier_yaml_settings`, `self.current_yaml`, `self.context`,
`self.repository`, etc. — none of which they define. There is no `Protocol` or ABC that
formalizes what the consuming class must provide. This makes the mixins:

- Impossible to understand in isolation
- Impossible to type-check statically
- Fragile to refactor (no compile-time safety net)

### 1.5 — NotificationService Has Too Many Responsibilities

**File:** `services/notification/__init__.py`

This single class handles:
- Plan/tier entitlement gating
- GitHub app installation inspection
- YAML configuration parsing
- Notifier instantiation and composition
- Notification execution and error handling
- Result persistence
- Status aggregation from flags and components

The 7-argument notifier constructor call is copy-pasted **6 times**. Adding a new
constructor parameter requires updating all 6 call sites.

Plan-checking logic is duplicated between `_should_use_status_notifier` and
`_should_use_checks_notifier`, with manual memoization via a mutable `self.plan`
attribute that creates implicit call-order dependencies.

### 1.6 — No Shared Data Context Between Notifiers

Each notifier independently re-extracts everything it needs from `ComparisonProxy`.
When the comment notifier, three status notifiers, and three checks notifiers all run
for the same commit, they each independently compute filtered comparisons, diffs, and
changes. There is no mechanism to share intermediate results.

The bundle analysis system solves this with `fields_of_interest` +
`initialize_from_context()`, where each notification context inherits already-loaded
fields from the previous one.

### 1.7 — Comment Notifier Condition System Is Over-Engineered

**File:** `notifiers/comment/conditions.py`

`HasEnoughRequiredChanges` is a god-condition containing 7 static methods, its own
caching system, bitwise flag evaluation, and a backwards-compatibility shim for
booleans. The bitmask-based OR-group evaluation (`condition_group &
individual_condition.value`) is non-obvious and produces no useful log output when
debugging.

All condition `check_condition` methods are `@staticmethod` that receive `notifier` as a
parameter — effectively instance methods disguised as static methods, preventing normal
method dispatch and making IDE navigation harder.

### 1.8 — Message Building Is Imperative and Hard to Trace

**File:** `notifiers/mixins/message/__init__.py`

`MessageMixin.create_message()` is 100+ lines that builds a PR comment by appending
strings to a list via a `write = message.append` closure. Section writers mutate the
list by side effect. The `upper`/`middle`/`lower` section trichotomy is not documented
in this file — you must trace into `get_message_layout` to understand it.

Business logic (plan tier checks, feature flags, cross-pollination marketing messages)
is embedded directly in what should be a formatting concern. Inline HTML, emoji
shortcodes, and full GitHub asset URLs are hardcoded in Python strings.

### 1.9 — Inconsistent and Fragile Error Handling

- `tasks/notify.py` uses a bare `assert` for commit existence and catches `Exception`
  broadly just to log a breadcrumb.
- `StatusNotifier.notify()` catches `TorngitClientError` at two different levels in the
  call stack with different result payloads.
- `CommentNotifier` catches `TorngitClientError` and `TorngitObjectNotFoundError` in
  behavior methods, but `TorngitServerFailureError` is caught separately in `notify()`.
- There is no unified error type for "this notification cannot proceed" — some failures
  are early returns with `NotificationResult`, some are exceptions, and some are dict
  returns with `"reason"` strings.

### 1.10 — `async_to_sync` Is Sprinkled Throughout

The conversion between async and sync (`async_to_sync(repository_service.set_commit_status)`,
`async_to_sync(repository_service.create_check_run)`, etc.) is done at each individual
call site across multiple files. If the worker ever migrates to async, every single call
must be found and changed. There is no centralized adapter.

### 1.11 — Tests Are Extensive but Brittle

The test suite is ~12,800 lines across 16 files with 318+ test methods. However:

- **187 snapshot assertions** that break on any message format change.
- Exact string matching on payload contents (URLs, color codes, message text).
- A ~900-line `conftest.py` with 12+ specialized comparison fixtures.
- Simple notifiers (Slack, Gitter, HipChat, IRC) have minimal coverage (1-4 tests each).
- Writing a test for a new notifier requires understanding the full fixture graph and
  mocking the entire provider layer.

### 1.12 — Likely Bugs

- `checks/base.py:98` — `comparison.pull is None or ()` is always truthy for non-None
  values due to operator precedence. The `or ()` evaluates as a separate expression.
- `status/base.py` — `get_upgrade_message()` takes `(self)` but `checks/base.py`
  overrides it to take `(self, comparison)`. Incompatible signatures on methods with the
  same name in a parent-child hierarchy.
- `tasks/notify.py` — A `log.info(...)` call is wrapped in a tuple by a trailing comma,
  creating a no-op single-element tuple.

---

## Part 2: Improvement Proposals

### 2.1 — Decompose `NotifyTask.run_impl_within_lock`

**Goal:** Break the 320-line god method into focused, testable steps.

**How:**
1. Extract a `NotificationContext` dataclass that holds `commit`, `current_yaml`,
   `comparison`, `enriched_pull`, `report`, and other shared state. This replaces the
   13-parameter signature of `submit_third_party_notifications`.
2. Extract each phase into its own method: `_validate_commit()`,
   `_resolve_bot_and_yaml()`, `_check_ci_status()`, `_fetch_reports()`,
   `_resolve_pull_request()`, `_dispatch_notifications()`.
3. Replace the 8 repetitive early-return dictionaries with a shared
   `_not_notified(reason: str)` helper or a `NotifyTaskResult` dataclass.
4. Centralize breadcrumb logging into a decorator or context manager so each exit path
   does not need to manually call `_call_upload_breadcrumb_task`.

### 2.2 — Flatten the Notifier Hierarchy with Composition

**Goal:** Eliminate the misleading `ChecksNotifier(StatusNotifier)` inheritance.

**How:**
1. Extract the shared utility methods (`can_we_set_this_status`, `required_builds`,
   `flag_coverage_was_uploaded`, `determine_status_check_behavior_to_apply`,
   `maybe_send_notification`) into a `StatusCheckMixin` or a standalone
   `StatusCheckHelper` service class.
2. Make `StatusNotifier` and `ChecksNotifier` both inherit directly from
   `AbstractBaseNotifier` (siblings, not parent-child).
3. Extract the duplicated flag-coverage logic from both `notify()` methods into a shared
   `_apply_flag_coverage_behavior(comparison, payload, message_key)` function that both
   can call, parameterized by the payload key to mutate.
4. `ChecksWithFallback` remains as-is — it is already compositional.

### 2.3 — Formalize Mixin Contracts with Protocols

**Goal:** Make mixin dependencies explicit and statically checkable.

**How:**
1. Define a `NotifierProtocol` (using `typing.Protocol`) that declares all attributes
   mixins depend on: `notifier_yaml_settings`, `current_yaml`, `context`, `repository`,
   `repository_service`, `decoration_type`, etc.
2. Type-hint each mixin's `self` as `NotifierProtocol` so that IDEs and mypy can verify
   the contract.
3. Remove redundant `context` class-variable declarations from leaf classes (they are
   already set by the mixins and shadowed unnecessarily).

### 2.4 — Separate Concerns in NotificationService

**Goal:** Single responsibility for the orchestrator.

**How:**
1. Extract plan/tier entitlement logic into a `NotificationEntitlementChecker` class
   with a single method `allowed_notification_types(owner, repository) -> set[str]`.
2. Extract notifier instantiation into a `NotifierFactory` with a
   `create_notifiers(yaml, repository, ...) -> list[AbstractBaseNotifier]` method. This
   factory owns the 7-argument constructor call exactly once, via a private
   `_build_notifier(cls, title, yaml_settings, site_settings)` helper.
3. The `NotificationService` becomes a thin orchestrator: get allowed types from the
   entitlement checker, get notifier instances from the factory, run them, persist
   results.

### 2.5 — Introduce a Shared Notification Context (Inspired by Bundle Analysis)

**Goal:** Avoid redundant computation across notifiers and make data contracts explicit.

**How:**
1. Define a `CoverageNotificationContext` dataclass with typed fields for everything
   notifiers need: `comparison`, `filtered_comparison`, `diff`, `changes`,
   `enriched_pull`, `head_report`, `base_report`, `flags`, `components`.
2. Build a `CoverageNotificationContextBuilder` that populates the context once, with
   each step being idempotent (check-before-compute) and raising a typed
   `ContextBuildError` on failure.
3. Pass the populated context to each notifier instead of the raw `ComparisonProxy`.
   Notifiers read from the context instead of re-computing diffs and filtered
   comparisons independently.

### 2.6 — Replace Imperative Message Building with Templates

**Goal:** Make PR comment formatting maintainable and testable.

**How:**
1. Move PR comment rendering to Django/Jinja templates (as the bundle analysis system
   already does). Each section (header, file table, flags, components, footer) becomes a
   template partial.
2. The `MessageMixin` becomes a thin adapter that assembles a template context dict and
   calls `template.render(context)`.
3. Business logic (plan tier checks, feature flags) stays in the notifier or the context
   builder — not in the template layer.
4. This makes message format changes a template edit, not a Python code change, and
   eliminates the `write = message.append` side-effect pattern.

### 2.7 — Unify Error Handling

**Goal:** One consistent pattern for "this notification cannot proceed."

**How:**
1. Define a `NotificationError` hierarchy: `NotificationSkipped(reason)` for expected
   non-sends, `NotificationFailed(reason, retryable)` for failures.
2. Each notifier raises these instead of returning ad-hoc `NotificationResult` objects
   with explanation strings or returning raw dicts.
3. The orchestrator catches these uniformly and converts to `NotificationResult` /
   `CommitNotification` records in one place.
4. Replace the bare `assert` in `NotifyTask` with an explicit
   `raise NotificationFailed("Commit not found")`.

### 2.8 — Centralize Async-to-Sync Bridging

**Goal:** Single place to change when migrating to async.

**How:**
1. Create a `ProviderAdapter` (or extend the existing `repository_service`) that wraps
   all provider calls (`set_commit_status`, `create_check_run`, `update_check_run`,
   `post_comment`, `edit_comment`, `delete_comment`) in sync wrappers.
2. Notifiers call `self.provider.set_commit_status(...)` (sync) instead of
   `async_to_sync(self.repository_service.set_commit_status)(...)`.
3. When the worker goes async, only the adapter changes.

### 2.9 — Fix Known Bugs

1. **`checks/base.py:98`** — Change `comparison.pull is None or ()` to
   `not comparison.pull`.
2. **`checks/base.py` `get_upgrade_message`** — Align the signature with
   `StatusNotifier.get_upgrade_message` or rename one to avoid polymorphic confusion.
3. **`tasks/notify.py`** — Remove the trailing-comma tuple around `log.info()`.
4. **`tasks/notify.py`** — Remove dead `GENERIC_TA_ERROR_MSG` constant.
5. **`checks/base.py`** — Remove the no-op `with nullcontext():` block.
6. **`checks/base.py`** — Remove the duplicated `get_notifier_filters()` override
   (it is identical to the inherited version).

### 2.10 — Improve Test Maintainability

**Goal:** Make tests less brittle and easier to write for new notifiers.

**How:**
1. Replace exact-string payload assertions with semantic checks (e.g., assert the status
   is "success", assert the message contains the coverage percentage — not the entire
   formatted string).
2. Reduce snapshot count by using snapshots only for full integration-level PR comment
   rendering tests, not for unit-level payload checks.
3. Extract a `NotifierTestHelper` base class or fixture that handles common setup
   (comparison creation, provider mocking, plan/tier setup) so new notifier tests can
   focus on behavior, not boilerplate.
4. Add tests for simple notifiers (Slack, Gitter, HipChat) covering error paths, not
   just the happy path.

---

## Recommended Order of Execution

These proposals are ordered by impact-to-effort ratio and dependency:

| Phase | Proposals | Rationale |
|-------|-----------|-----------|
| **1 — Quick wins** | 2.9 (bug fixes), 2.8 (async adapter) | Low risk, immediate improvement |
| **2 — Foundation** | 2.3 (protocols), 2.7 (error hierarchy) | Enables static analysis, unblocks later refactors |
| **3 — Core refactor** | 2.2 (flatten hierarchy), 2.1 (decompose task) | Largest impact on day-to-day developer experience |
| **4 — Architecture** | 2.4 (separate service concerns), 2.5 (shared context) | Structural improvements that pay off over time |
| **5 — Polish** | 2.6 (templates), 2.10 (test improvements) | Best done alongside or after the core refactors |

Each phase is independently shippable. Phase 1 can land as a single PR. Phases 2-3
should be done together to avoid an intermediate state where half the hierarchy uses
protocols and the other half does not.
