# syntax=docker/dockerfile:1.4
ARG REQUIREMENTS_IMAGE
ARG BUILD_ENV=self-hosted
ARG BERGLAS_VERSION=2.0.6

ARG WORKDIR=/app
ARG ENTRYPOINT
ARG DJANGO_SETTINGS_PARENT

FROM us-docker.pkg.dev/berglas/berglas/berglas:$BERGLAS_VERSION as berglas

FROM $REQUIREMENTS_IMAGE as app


# Add a non-root user to run as. Generally refer to it via its UID to avoid
# weird name/UID mismatch quirks.
RUN adduser --system codecov --uid 1001 --home /home/codecov
COPY --chown=1001:1001 --chmod=744 . /app
USER 1001

# Generate static assets for Django
WORKDIR /app/apps/codecov-api
RUN python manage.py collectstatic --no-input

ARG RELEASE_VERSION
ENV RELEASE_VERSION=$RELEASE_VERSION

ARG WORKDIR
WORKDIR $WORKDIR

ARG ENTRYPOINT
ENV ENTRYPOINT=$ENTRYPOINT
ENTRYPOINT $ENTRYPOINT

### Local development image
FROM app as local
ARG DJANGO_SETTINGS_PARENT
ENV RUN_ENV="DEV"
ENV DJANGO_SETTINGS_MODULE="$DJANGO_SETTINGS_PARENT.settings_dev"

### Production image
FROM app as cloud
ARG DJANGO_SETTINGS_PARENT
ENV RUN_ENV="PRODUCTION"
ENV DJANGO_SETTINGS_MODULE="$DJANGO_SETTINGS_PARENT.settings_prod"
COPY --chmod=755 --from=berglas /bin/berglas /usr/local/bin/berglas

### Self-hosted image
FROM app as self-hosted

ARG DJANGO_SETTINGS_PARENT
ENV RUN_ENV="ENTERPRISE"
ENV CODECOV_API_PORT=8000
ENV DJANGO_SETTINGS_MODULE="$DJANGO_SETTINGS_PARENT.settings_enterprise"

### Self-hosted runtime image
FROM self-hosted as self-hosted-runtime
USER root
ARG EXTERNAL_DEPS_FOLDER=./external_deps
RUN mkdir $EXTERNAL_DEPS_FOLDER
RUN pip install --target $EXTERNAL_DEPS_FOLDER psycopg2-binary tlslite-ng
RUN chown 1001:1001 $EXTERNAL_DEPS_FOLDER
USER 1001

# "Output" image
FROM ${BUILD_ENV}
