# syntax=docker/dockerfile:1.4
ARG REQUIREMENTS_IMAGE
ARG BUILD_ENV=self-hosted
ARG BERGLAS_VERSION=2.0.6

ARG WORKDIR=/app
ARG ENTRYPOINT

FROM us-docker.pkg.dev/berglas/berglas/berglas:$BERGLAS_VERSION as berglas

FROM $REQUIREMENTS_IMAGE as app

COPY --chown=codecov:application --chmod=744 . /app
USER codecov

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
ENV RUN_ENV="DEV"

### Production image
FROM app as cloud
ENV RUN_ENV="PRODUCTION"
COPY --chmod=755 --from=berglas /bin/berglas /usr/local/bin/berglas

### Self-hosted image
FROM app as self-hosted
ENV RUN_ENV="ENTERPRISE"
ENV CODECOV_API_PORT=8000

# "Output" image
FROM ${BUILD_ENV}
