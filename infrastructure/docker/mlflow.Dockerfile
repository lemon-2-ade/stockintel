# syntax=docker/dockerfile:1.7
# MLflow tracking server + model registry.
# The upstream image lacks a PostgreSQL driver, so we build a minimal one with
# a pinned MLflow version (the same version the training code will use).
ARG PYTHON_IMAGE=python:3.12-slim-bookworm
FROM ${PYTHON_IMAGE}

ARG MLFLOW_VERSION=3.16.1
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    MLFLOW_DISABLE_AGENT_HINT=1

RUN pip install "mlflow==${MLFLOW_VERSION}" "psycopg2-binary==2.9.13" \
 && groupadd --system --gid 10001 mlflow \
 && useradd --system --uid 10001 --gid mlflow --home-dir /mlflow --create-home mlflow \
 && mkdir -p /mlflow/artifacts && chown -R mlflow:mlflow /mlflow

USER 10001:10001
WORKDIR /mlflow
EXPOSE 5000
# Backend store URI, artifact destination and allowed hosts come from
# MLFLOW_* environment variables (see docker-compose.yml), so no credential is
# baked into the image or visible in `ps`.
CMD ["mlflow", "server", "--serve-artifacts", "--host", "0.0.0.0", "--port", "5000"]
