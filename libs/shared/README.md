# shared

Shared is a place for code that is common to multiple python services within `codecov`.

## How does shared get into production

`shared` is a package of its own, so it needs to be installed as a dependency on the services that might use it.

The current services using `shared` are `worker` and `codecov-api`.

## Getting started

To get started, ensure that you have:

1. Docker installed on your machine
2. Run
```
docker compose up
```

## Running tests

In order to run tests from within your docker container, run:

```
make test
```

To run a specific test file, run for example:
```
make test-path TEST_PATH=tests/unit/bundle_analysis/test_bundle_analysis.py
```

## Running migrations

If you make changes to the models in `shared/django_apps/` you will need to create migrations to reflect those changes in the database.

Make sure the shared container is running and shell into it
```bash
$ docker compose up
$ docker compose exec -it shared /bin/bash
```

Now you can create a migration (from within the container)

```bash
$ cd shared/django_apps/
$ python manage.py pgmakemigrations
```

To learn more about migrations visit [Django Docs](https://docs.djangoproject.com/en/5.0/topics/migrations/)

## Managing shared dependencies

As a normal python package, `shared` can include dependencies of its own.

Updating them should be done in the `pyproject.toml` file.

Remember to add dependencies as loosely as possible. Only make sure to include what the minimum version is, and only include a maximum version if you do know that higher versions will break.

Remember that multiple packages, on different contexts of their own requirements, will have to install this. So keeping the requirements loose allow them to avoid version clashes and eases upgrades whenever they need to.
