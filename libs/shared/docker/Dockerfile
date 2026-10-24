# syntax=docker/dockerfile:1.4
ARG REQUIREMENTS_IMAGE

ARG PYTHON_IMAGE=python:3.13.17-slim-bookworm@sha256:e32be0e55814a08977501ca863271d9bd02443710db8f2afaf8372e2c4508530

FROM $REQUIREMENTS_IMAGE as app

# Change the working directory to the `app/libs/shared` directory
COPY . /app/libs/shared
WORKDIR /app/libs/shared

CMD ["uv", "run", "bash"]
