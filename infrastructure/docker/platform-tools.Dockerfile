# syntax=docker/dockerfile:1.7
# ---------------------------------------------------------------------------
# platform-tools: one-shot jobs that act on shared infrastructure
#   * topic provisioning  -> python -m shared.kafka.provision
#   * DB migrations       -> python -m shared.db.migrate upgrade head
# Built from the `shared` workspace package only, so it stays small.
# ---------------------------------------------------------------------------
ARG PYTHON_IMAGE=python:3.12-slim-bookworm
ARG UV_IMAGE=ghcr.io/astral-sh/uv:0.8.17

FROM ${UV_IMAGE} AS uv

FROM ${PYTHON_IMAGE} AS builder
COPY --from=uv /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/opt/venv
WORKDIR /src

# 1) third-party dependencies only: cached until the lockfile changes
COPY pyproject.toml uv.lock ./
COPY shared/pyproject.toml shared/pyproject.toml
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-workspace \
        --package stockintel-shared --extra kafka --extra db

# 2) our code, installed non-editable into the venv
COPY shared/ shared/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable \
        --package stockintel-shared --extra kafka --extra db

FROM ${PYTHON_IMAGE} AS runtime
LABEL org.opencontainers.image.title="stockintel-platform-tools"
ENV PATH="/opt/venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
RUN groupadd --system --gid 10001 app \
 && useradd --system --uid 10001 --gid app --no-create-home --shell /usr/sbin/nologin app
COPY --from=builder --chown=root:root /opt/venv /opt/venv
USER 10001:10001
WORKDIR /tmp
CMD ["python", "-m", "shared.kafka.provision", "--help"]
