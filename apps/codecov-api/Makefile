sha ?= $(shell git rev-parse --short=7 HEAD)
long_sha ?= $(shell git rev-parse HEAD)
merge_sha ?= $(shell git merge-base HEAD^ origin/main)
release_version ?= `cat VERSION`
build_date ?= $(shell git show -s --date=iso8601-strict --pretty=format:%cd $$sha)
branch ?= $(shell git branch | grep \* | cut -f2 -d' ')
epoch ?= $(shell date +"%s")
AR_REPO ?= codecov/api
DOCKERHUB_REPO ?= codecov/self-hosted-api
REQUIREMENTS_TAG ?= requirements-v1-$(shell sha1sum uv.lock | cut -d ' ' -f 1)-$(shell sha1sum docker/Dockerfile.requirements | cut -d ' ' -f 1)
VERSION ?= release-${sha}
CODECOV_UPLOAD_TOKEN ?= "notset"
CODECOV_STATIC_TOKEN ?= "notset"
CODECOV_URL ?= "https://api.codecov.io"
export DOCKER_BUILDKIT=1
export API_DOCKER_REPO=${AR_REPO}
export API_DOCKER_VERSION=${VERSION}
export CODECOV_TOKEN=${CODECOV_UPLOAD_TOKEN}
API_DOMAIN ?= api
PROXY_NETWORK ?= api_default

DEFAULT_REQS_TAG := requirements-v1-$(shell sha1sum uv.lock | cut -d ' ' -f 1)-$(shell sha1sum docker/Dockerfile.requirements | cut -d ' ' -f 1)
REQUIREMENTS_TAG ?= ${DEFAULT_REQS_TAG}

# We allow this to be overridden so that we can run `pytest` from this directory
# but have the junit file use paths relative to a parent directory. This will
# help us move to a monorepo.
PYTEST_ROOTDIR ?= "."

# Codecov CLI version to use
CODECOV_CLI_VERSION := 9.0.4

check-for-migration-conflicts:
	python manage.py check_for_migration_conflicts

test:
	COVERAGE_CORE=sysmon pytest --cov=./ --junitxml=junit.xml -o junit_family=legacy -c pytest.ini --rootdir=${PYTEST_ROOTDIR}

shell:
	docker compose exec api bash
	
test_env.check-for-migration-conflicts:
	docker compose exec api python manage.py check_for_migration_conflicts

### START Proxy Commands
.PHONY: proxy.build
proxy.build: # Used to build the proxy
proxy.build:
	docker build -f docker/Dockerfile-proxy . -t ${API_DOCKER_REPO}/proxy:latest -t ${API_DOCKER_REPO}/proxy:${release_version}-${sha} \
			--label "org.label-schema.build-date"="$(build_date)" \
			--label "org.label-schema.name"="API Proxy" \
			--label "org.label-schema.vendor"="api" \
			--label "org.label-schema.version"="${release_version}"

.PHONY: proxy.run
proxy.run: # Used to run the proxy
proxy.run:
	make proxy.build
	make proxy.down
	docker run --rm --network ${PROXY_NETWORK} -e FRP_TOKEN=${FRP_TOKEN} -e DOMAIN=${API_DOMAIN} --name api-proxy ${API_DOCKER_REPO}/proxy:latest
	sleep 3
	make proxy.logs
	# You should see "[api] start proxy success"
	# If no logs then proxy failed to start. Check if you are on VPN. If you get a 404, check if you are on VPN

.PHONY: proxy.logs
proxy.logs: # Used to logs the proxy
proxy.logs:
	docker logs api-proxy

.PHONY: proxy.shell
proxy.shell: # Used to shell the proxy
proxy.shell:
	docker exec -it api-proxy sh

.PHONY: proxy.down
proxy.down: # Used to down the proxy
proxy.down:
	docker kill api-proxy || true

### END PROXY Commands
