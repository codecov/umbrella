# Codecov's Docker images

`Makefile.docker` contains `make` targets used by CI to build docker images.
`Makefile.ci-tests` contains `make` targets for setting up and running tests
against those images in CI.

### The "requirements image": `Dockerfile.requirements`

We have a single base image for all of our services which installs various
system dependencies (e.g. `apt-get install libpq-dev`) as well the Python
dependencies in the `pyproject.toml` file in the repository root.

You'll notice `Dockerfile.requirements` has a couple of `FROM` statements to
build more than one image. The first image contains _build dependencies_, such
as the Rust compiler, which are needed to install everything but not actually
needed to run the service. The second image copies only the _runtime dependencies_
over from the first image. This is the image "returned by" `docker build`.

The requirements image is pushed under the name `<GCP repo prefix>/umbrella-reqs`.
Its tag includes the SHA1 hash of its inputs:
- `uv.lock`
- `docker/Dockerfile.requirements`
- `libs/shared/**`

### The "test requirements image": `Dockerfile.test-requirements`

On top of the base "requirements image", this image installs development
dependencies like linters and such which don't belong in our production
deployments.

The test requirements image is pushed under the name
`<GCP repo prefix>/umbrella-test-reqs`. Its tag matches the SHA of the base
requirements image.

### The "app image": `Dockerfile`

Each separate "subproject" inside umbrella (worker, shared, api) has its own
"app image" which plugs the appropriate working directory and entrypoint command
into a shared `Dockerfile`.

We use the same `Dockerfile` to build a few different "flavors" of each app:
self-hosted, local, or production (a.k.a. "cloud"). All three flavors are pretty
trivial variants on the same base, so we build all three and then choose which
one to "return" based on a build arg with the final line: `FROM ${BUILD_ENV}`.

The base image for each "app image" is passed in as the `REQUIREMENTS_IMAGE`
build argument. Production images will use the `Dockerfile.requirements` image
as a base while tests and local development will use `Dockerfile.test-requirements`.

When built against `Dockerfile.requirements`, these images are pushed with names like:
- `<GCP repo prefix>/codecov/worker`
- `<GCP repo prefix>/codecov/api`
- `<GCP repo prefix>/codecov/dev-shared`

When built against `Dockerfile.test-requirements`, add `test-` before the image name.

Tags include `:latest` or things like `:release-<short-commit-sha>`.
