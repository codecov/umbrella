# Codecov monorepo

`umbrella` is Codecov's monorepo. It was created by absorbing a few of our repositories into subdirectories in this repository:
- [`codecov/worker`](https://github.com/codecov/worker) in `apps/worker`
- [`codecov/codecov-api`](https://github.com/codecov/codecov-api) in `apps/codecov-api`
- [`codecov/shared`](https://github.com/codecov/shared) in `libs/shared`

There is also a `tools` directory for development tools that we use to develop Codecov.

### Absorbing a new repo

`tools/absorb-repo` contains a script that absorbs a repository while preserving its history. See the README in that directory for more information.

# Running Codecov locally

These instructions assume you're using macOS (or at least a POSIX-y platform) but in principle you can get them working anywhere.

`docker-compose.yml` sets up a complete local instance of Codecov that you can access at http://localhost:8080. The `worker` and `api` services bind-mount the host's copy of the repository into the container to make development easier:
- Changes you make outside the containers will trigger a hot-reloader inside the containers automatically
  - Both services will hot-reload when you make changes in their directories as well as in `libs/shared`
- Things like coverage data or junit files that are produced when you run tests inside the containers are accessible to you outside the containers

`Makefile` (via `tools/devenv/Makefile.devenv` and `tools/devenv/Makefile.test`) exposes `make` targets that take care of setting up and updating your local Codecov instance.

### Prereqs

- [Docker Desktop](https://docs.docker.com/desktop/).
  - If using a Mac, ensure that `General > Use Rosetta for x86/amd64 emulation on Apple Silicon` is turned on in the settings or the timescale container will fail to start.
- `make`
- (Optional) `codecovcli` (install with `pip install codecov-cli`)

### `tools/devenv/config/codecov.yml`
Codecov needs API keys and secrets to run, and we obviously aren't putting ours on GitHub. We expect you to put those secrets at `tools/devenv/config/codecov.yml`. This is sometimes called the "install yaml".

If you work at Codecov/Sentry, you should have access to the "Codecov Engineering" 1Password vault; copy the contents of the "umbrella secrets" item to `tools/devenv/config/codecov.yml`.

If you don't work here, try copying [our example self-hosted configuration](https://github.com/codecov/self-hosted/blob/main/config/codecov.yml) or following our [self-hosted configuration guide](https://docs.codecov.com/docs/configuration).

### Ready to go
From the repository root:
```
# Build `umbrella` containers, pull other containers, start docker compose
$ make devenv

# Rebuild and restart `umbrella` containers. Necessary if dependencies change.
# Run this after pulling if you haven't pulled in a while.
$ make devenv.refresh

# Shut everything down.
$ make devenv.stop

# Start everything, skipping the build step.
$ make devenv.start
```

Access your local instance of Codecov at http://localhost:8080.

You can view logs with `docker compose logs api` / `docker compose logs worker` or in the Docker Desktop UI.

### Running tests locally

```
# Runs worker tests and emits coverage and test result data in `apps/worker`
$ make devenv.test.worker

# Runs codecov-api tests and emits coverage and test result data in `apps/codecov-api`
$ make devenv.test.api

# Runs shared tests and emits coverage and test result data in `libs/shared/tests`
$ make devenv.test.shared
```

These `make` targets capture a few magic incantations involved in running our tests but they don't allow you to specify specific test files you want to run. A shell function or custom script may be nicer.

### Submitting coverage and test results locally

Using `codecovcli` with your local Codecov instance can be a pain, so `tools/devenv/scripts/codecovcli-helper.sh` exists to make it a little easier.

The first time you want to upload something for a respository, you'll need to get its local upload token for your local Codecov instance. Navigate to http://localhost:8080/gh/codecov/umbrella/new (or the same page for a different repository) and copy the upload token on that page. Add a line to `~/.local-codecov-tokens` like the following:
```
github.com:codecov/umbrella=5889cc1e-35b3-41b8-9e02-d441ec80d463
```

From then on, you can invoke `./tools/devenv/scripts/codecovcli-helper.sh` more-or-less how you'd invoke `codecovcli`:
```

# Upload worker coverage report with workerunit flag
$ ./tools/devenv/scripts/codecovcli-helper.sh upload-process --report-type coverage -f apps/worker/unit.coverage.xml -F workerunit

# Upload api test results with apiunit flag
$ ./tools/devenv/scripts/codecovcli-helper.sh upload-process --report-type test_results -f apps/codecov-api/unit.junit.xml -F apiunit
```

For convenience, some `make` targets have been created that will upload the coverage and junit files produced by the `make` targest for tests:
```
$ make devenv.upload.worker
$ make devenv.upload.api
$ make devenv.upload.shared
```

### Extras
- You can browse your local MinIO (archive service) at http://localhost:9000/ with the username `codecov-default-key` and password `codecov-default-secret`
- You can browse your local Redis by pointing the [Redis CLI](https://redis.io/docs/latest/develop/tools/cli/) or [Redis Insight GUI](https://redis.io/insight/) at 127.0.0.1:6379
- You can browse your local Postgres database with the connection string `postgres://postgres@localhost:5432/postgres` in your preferred database browser
- You can browse your local Timescale database with the connection string `postgres://postgres@localhost:5433/postgres` in your preferred database browser
