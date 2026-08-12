# syntax=docker/dockerfile:1.4
ARG REQUIREMENTS_IMAGE

ARG PYTHON_IMAGE=ghcr.io/astral-sh/uv:python3.13-bookworm-slim@sha256:531f855bda2c73cd6ef67d56b733b357cea384185b3022bd09f05e002cd144ca

FROM $REQUIREMENTS_IMAGE as app

# Change the working directory to the `app/libs/shared` directory
COPY . /app/libs/shared
WORKDIR /app/libs/shared

CMD ["uv", "run", "bash"]
