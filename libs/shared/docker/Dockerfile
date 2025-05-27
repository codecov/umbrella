# syntax=docker/dockerfile:1.4
ARG REQUIREMENTS_IMAGE

ARG PYTHON_IMAGE=ghcr.io/astral-sh/uv:python3.13-bookworm-slim

FROM $REQUIREMENTS_IMAGE as app

# Change the working directory to the `app/libs/shared` directory
COPY . /app/libs/shared
WORKDIR /app/libs/shared

CMD ["uv", "run", "bash"]
