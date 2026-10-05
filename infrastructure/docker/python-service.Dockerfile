# syntax=docker/dockerfile:1.7
# ---------------------------------------------------------------------------
# One Dockerfile for every Python workspace member.
#
#   docker build -f infrastructure/docker/python-service.Dockerfile \
#     --build-arg PACKAGE=stockintel-market-producer .
#
# The runtime image contains only a virtualenv with PACKAGE's dependency
# closure (installed non-editable) - no source tree, no dev tools, no uv.
# When a new workspace member is added, add its pyproject.toml to step 1.
# ---------------------------------------------------------------------------
ARG PYTHON_IMAGE=python:3.12-slim-bookworm
ARG UV_IMAGE=ghcr.io/astral-sh/uv:0.8.17

FROM ${UV_IMAGE} AS uv

FROM ${PYTHON_IMAGE} AS builder
ARG PACKAGE
ARG UV_EXTRAS=""
COPY --from=uv /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/opt/venv
WORKDIR /src

# 1) Third-party dependencies only; this layer is cached until uv.lock or a
#    member's pyproject.toml changes. uv needs every member's pyproject to
#    resolve the workspace, even members it will not install.
COPY pyproject.toml uv.lock ./
COPY shared/pyproject.toml shared/pyproject.toml
COPY ml/pyproject.toml ml/pyproject.toml
COPY services/market-producer/pyproject.toml services/market-producer/pyproject.toml
COPY services/stream-processor/pyproject.toml services/stream-processor/pyproject.toml
COPY services/sinks/pyproject.toml services/sinks/pyproject.toml
COPY apps/api/pyproject.toml apps/api/pyproject.toml
RUN --mount=type=cache,target=/root/.cache/uv \
    test -n "${PACKAGE}" && \
    uv sync --frozen --no-dev --no-install-workspace --package "${PACKAGE}" ${UV_EXTRAS}

# 2) Workspace sources, installed non-editable (only PACKAGE's closure).
COPY shared/ shared/
COPY ml/ ml/
COPY services/ services/
COPY apps/api/ apps/api/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable --package "${PACKAGE}" ${UV_EXTRAS}

FROM ${PYTHON_IMAGE} AS runtime
ARG PACKAGE
LABEL org.opencontainers.image.title="${PACKAGE}"
ENV PATH="/opt/venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
RUN groupadd --system --gid 10001 app \
 && useradd --system --uid 10001 --gid app --no-create-home --shell /usr/sbin/nologin app
COPY --from=builder --chown=root:root /opt/venv /opt/venv
USER 10001:10001
WORKDIR /tmp
