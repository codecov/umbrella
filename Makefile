######
# ~~~ Welcome to our Makefile ~~~
# We hope you enjoy your stay!
#
# We use `make` for quite a lot, so targets are defined across different
# `Makefile`s and `include`d here. Maybe that's a sign that we need a different
# system, but old habits die hard.
#
# This `Makefile` exports some generally-applicable `Makefile` variables that
# will apply across all of the `Makefile`s it `include`s, and it also defines
# some "wrapper" targets that will run generic `make` targets with values for
# specific subprojects plugged in:
# - `worker.*` for `apps/worker`
# - `api.*` for `apps/codecov-api`
# - `shared.*` for `libs/shared`
#
# These "wrapper" targets rely on two somewhat obtuse `make` features:
# - a "function" or "macro" of sorts that is expanded before each target to set
#   variables for that target. This is the `define` and `eval`/`call` stuff.
# - pattern rules, which let you match basically a wildcard in a rule name. A %
#   in a rule name matches anything, and then $* in the rule definition expands
#   to whatever was matched. If multiple rules can match, `make` picks the most
#   specific one (i.e. the one with the smallest `$*` value)
#
# If you run `make worker.build`, the wrapper target will set worker-specific
# values and then run `$(MAKE) _build` to run the generic `_build` target with
# worker's propagated through. 
#
# We are interested in making `umbrella` more of a monolith instead of a
# collection of subprojects. If and when we do, the goal is not to need the
# "wrapper" targets anymore; we should be able to set a single set of values for
# the variables used in the wrapper targets, delete the `_` from generic rule
# names like `_build` or `_save.requirements`, and invoke `make` on the same
# target no matter what.
#####

export sha := $(shell git rev-parse --short=7 HEAD)
export full_sha := $(shell git rev-parse HEAD)
export long_sha := ${full_sha}
export merge_sha := $(shell git merge-base HEAD^ origin/main)
export release_version := $(shell cat VERSION)
export VERSION ?= release-${sha}

export build_date ?= $(shell git show -s --date=iso8601-strict --pretty=format:%cd $$sha)
export branch := $(shell git branch | grep \* | cut -f2 -d' ')

# This can be overridden with an environment variable to pull from a GCR registry.
export AR_REPO_PREFIX ?= codecov

# `LC_ALL=C` is added to `sort` to ensure you get the same order across systems.
# Otherwise, a Mac may sort according to en_US.UTF-8 while CI may sort according to C/POSIX.
export SHARED_SHA := $(shell git ls-files libs/shared | LC_ALL=C sort | xargs sha1sum | cut -d ' ' -f 1 | sha1sum | head -c 40)
export DOCKER_REQS_SHA := $(shell sha1sum docker/Dockerfile.requirements | head -c 40)

######
# codecov-api targets
######
API_UV_LOCK_SHA := $(shell sha1sum apps/codecov-api/uv.lock | head -c 40)
API_REQS_TAG := reqs-${API_UV_LOCK_SHA}-${DOCKER_REQS_SHA}-${SHARED_SHA}

define api_rule_prefix
.PHONY: $(1)
$(1): export APP_DIR := apps/codecov-api
$(1): export REQUIREMENTS_TAG := ${API_REQS_TAG}
$(1): export AR_REPO ?= ${AR_REPO_PREFIX}/api
$(1): export DOCKERHUB_REPO ?= codecov/self-hosted-api
$(1): export CI_REQS_REPO ?= codecov/api-ci-requirements
endef

# Any API target starting with `proxy` should be forwarded to
# `apps/codecov-api/Makefile`.
$(eval $(call api_rule_prefix,api.proxy%))
api.proxy%:
	$(MAKE) -C apps/codecov-api proxy$*

# Any API target starting with `shell` should be forwarded to
# `apps/codecov-api/Makefile`.
$(eval $(call api_rule_prefix,api.shell%))
api.shell%:
	$(MAKE) -C apps/codecov-api shell$*

# All other API targets are implemented as generic targets that are `include`d
# from the root `Makefile`.
$(eval $(call api_rule_prefix,api.%))
api.%:
	$(MAKE) _$*

######
# worker targets
######
WORKER_UV_LOCK_SHA := $(shell sha1sum apps/worker/uv.lock | head -c 40)
WORKER_REQS_TAG := reqs-${WORKER_UV_LOCK_SHA}-${DOCKER_REQS_SHA}-${SHARED_SHA}

define worker_rule_prefix
.PHONY: $(1)
$(1): export APP_DIR := apps/worker
$(1): export REQUIREMENTS_TAG := ${WORKER_REQS_TAG}
$(1): export AR_REPO ?= ${AR_REPO_PREFIX}/worker
$(1): export DOCKERHUB_REPO ?= codecov/self-hosted-worker
$(1): export CI_REQS_REPO ?= codecov/worker-ci-requirements
endef

# Any Worker target starting with `shell` should be forwarded to
# `apps/worker/Makefile`.
$(eval $(call worker_rule_prefix,worker.shell%))
worker.shell%:
	$(MAKE) -C apps/worker shell$*

# All other Worker targets are implemented as generic targets that are
# `include`d from the root `Makefile`.
$(eval $(call worker_rule_prefix,worker.%))
worker.%:
	$(MAKE) _$*

######
# shared targets
######
SHARED_UV_LOCK_SHA := $(shell sha1sum libs/shared/uv.lock | head -c 40)
SHARED_REQS_TAG := reqs-${SHARED_UV_LOCK_SHA}-${DOCKER_REQS_SHA}

define shared_rule_prefix
.PHONY: $(1)
$(1): export APP_DIR := libs/shared
$(1): export REQUIREMENTS_TAG := ${SHARED_REQS_TAG}
$(1): export AR_REPO ?= ${AR_REPO_PREFIX}/dev-shared
$(1): export DOCKERHUB_REPO ?= codecov/dev-hosted-shared
$(1): export CI_REQS_REPO ?= codecov/shared-ci-requirements
$(1): export COV_SOURCE := ./shared
endef

# All other Shared targets are implemented as generic targets above. Declare the
# appropriate Makefile variables with this rule prefix function and then invoke
# `make` again on the generic target.
$(eval $(call shared_rule_prefix,shared.%))
shared.%:
	$(MAKE) _$*

######
# Targets for building docker images
######
include docker/Makefile.docker

######
# Targets for running tests in CI
######
include docker/Makefile.ci-tests

######
# Development environment targets
######
include tools/devenv/Makefile.devenv
