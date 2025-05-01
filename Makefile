export sha := $(shell git rev-parse --short=7 HEAD)
export full_sha := $(shell git rev-parse HEAD)
export long_sha := ${full_sha}
export merge_sha := $(shell git merge-base HEAD^ origin/main)
export release_version := `cat VERSION`
export VERSION := release-${sha}

export build_date ?= $(shell git show -s --date=iso8601-strict --pretty=format:%cd $$sha)
export branch := $(shell git branch | grep \* | cut -f2 -d' ')

export DOCKER_BUILDKIT=1

# `LC_ALL=C` is added to `sort` to ensure you get the same order across systems.
# Otherwise, a Mac may sort according to en_US.UTF-8 while CI may sort according to C/POSIX.
export SHARED_SHA := $(shell git ls-files libs/shared | LC_ALL=C sort | xargs sha1sum | cut -d ' ' -f 1 | sha1sum | head -c 40)
export DOCKER_REQS_SHA := $(shell sha1sum docker/Dockerfile.requirements | head -c 40)

# Generic target for building a requirements image. You probably want
# `worker.build.requirements` or `api.build.requirements`.
_build.requirements:
	docker pull ${AR_REPO}:${REQUIREMENTS_TAG} || docker build \
			   --network host \
               -f docker/Dockerfile.requirements . \
               --build-arg APP_DIR=${APP_DIR} \
               -t ${AR_REPO}:${REQUIREMENTS_TAG} \
	       -t ${CI_REQS_REPO}:${REQUIREMENTS_TAG}

######
# codecov-api targets
######
API_UV_LOCK_SHA := $(shell sha1sum apps/codecov-api/uv.lock | head -c 40)
API_REQS_TAG := reqs-${API_UV_LOCK_SHA}-${DOCKER_REQS_SHA}-${SHARED_SHA}

define api_rule_prefix
.PHONY: $(1)
$(1): export APP_DIR := apps/codecov-api
$(1): export REQUIREMENTS_TAG := ${API_REQS_TAG}
$(1): export AR_REPO ?= codecov/api
$(1): export DOCKERHUB_REPO ?= codecov/self-hosted-api
$(1): export CI_REQS_REPO ?= codecov/api-ci-requirements
endef

# umbrella builds a special requirements image for api that installs shared properly.
$(eval $(call api_rule_prefix,api.build.requirements))
api.build.requirements:
	$(MAKE) _build.requirements

# This target calls `make build.requirements` for api so we have to make sure it calls our
# custom `build.requirements` instead.
$(eval $(call api_rule_prefix,api.build))
api.build:
	$(MAKE) api.build.requirements
	$(MAKE) -C apps/codecov-api build.local

# Any other target starting with `api.` should be forwarded to `apps/codecov-api`.
# The `$*` variable is the string caught by the `%` in the rule pattern.
$(eval $(call api_rule_prefix,api.%))
api.%:
	$(MAKE) -C apps/codecov-api $*

######
# worker targets
######
WORKER_UV_LOCK_SHA := $(shell sha1sum apps/worker/uv.lock | head -c 40)
WORKER_REQS_TAG := reqs-${WORKER_UV_LOCK_SHA}-${DOCKER_REQS_SHA}-${SHARED_SHA}

define worker_rule_prefix
.PHONY: $(1)
$(1): export APP_DIR := apps/worker
$(1): export REQUIREMENTS_TAG := ${WORKER_REQS_TAG}
$(1): export AR_REPO ?= codecov/worker
$(1): export DOCKERHUB_REPO ?= codecov/self-hosted-worker
$(1): export CI_REQS_REPO ?= codecov/worker-ci-requirements
endef

# umbrella builds a special requirements image for worker that installs shared properly.
$(eval $(call worker_rule_prefix,worker.build.requirements))
worker.build.requirements:
	$(MAKE) _build.requirements

# This target calls `make build.requirements` for worker so we have to make sure it calls our
# custom `build.requirements` instead.
$(eval $(call worker_rule_prefix,worker.build))
worker.build:
	$(MAKE) worker.build.requirements
	$(MAKE) -C apps/worker build.local

# Any other target starting with `worker.` should be forwarded to `apps/worker`.
# The `$*` variable is the string caught by the `%` in the rule pattern.
$(eval $(call worker_rule_prefix,worker.%))
worker.%:
	$(MAKE) -C apps/worker $*

######
# shared targets
######

# No need to override any of shared's targets. Just run make in `libs/shared`.
.PHONY: shared.%
shared.%:
	$(MAKE) -C libs/shared $*

######
# Development environment targets
######
include tools/devenv/Makefile.devenv
