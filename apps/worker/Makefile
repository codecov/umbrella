sha ?= $(shell git rev-parse --short=7 HEAD)
full_sha ?= $(shell git rev-parse HEAD)
release_version ?= `cat VERSION`
_gcr := ${CODECOV_WORKER_GCR_REPO_BASE}
merge_sha ?= $(shell git merge-base HEAD^ origin/main)
build_date ?= $(shell git show -s --date=iso8601-strict --pretty=format:%cd $$sha)
name ?= worker
branch ?= $(shell git branch | grep \* | cut -f2 -d' ')
gh_access_token := $(shell echo ${GH_ACCESS_TOKEN})
epoch ?= $(shell date +"%s")

AR_REPO ?= codecov/worker
DOCKERHUB_REPO ?= codecov/self-hosted-worker
VERSION ?= release-${sha}
CODECOV_UPLOAD_TOKEN ?= "notset"
CODECOV_STATIC_TOKEN ?= "notset"
CODECOV_URL ?= "https://api.codecov.io"

DEFAULT_REQS_TAG := requirements-v1-$(shell sha1sum uv.lock | cut -d ' ' -f 1)-$(shell sha1sum docker/Dockerfile.requirements | cut -d ' ' -f 1)
REQUIREMENTS_TAG ?= ${DEFAULT_REQS_TAG}

# We allow this to be overridden so that we can run `pytest` from this directory
# but have the junit file use paths relative to a parent directory. This will
# help us move to a monorepo.
PYTEST_ROOTDIR ?= "."

export DOCKER_BUILDKIT=1
export WORKER_DOCKER_REPO=${AR_REPO}
export WORKER_DOCKER_VERSION=${VERSION}
export CODECOV_TOKEN=${CODECOV_UPLOAD_TOKEN}

# Codecov CLI version to use
CODECOV_CLI_VERSION := 0.5.1

test:
	COVERAGE_CORE=sysmon pytest --cov=./ --junitxml=junit.xml -o junit_family=legacy -c pytest.ini --rootdir=${PYTEST_ROOTDIR}

shell:
	docker-compose exec worker bash
