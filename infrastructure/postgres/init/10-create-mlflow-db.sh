#!/usr/bin/env bash
# Runs once, on first initialisation of the Postgres data volume.
# MLflow gets its own role + database: its schema is managed by MLflow's own
# migrations and must never be touched by ours (and vice versa).
set -euo pipefail

: "${MLFLOW_DB_PASSWORD:?MLFLOW_DB_PASSWORD must be set}"

psql -v ON_ERROR_STOP=1 -v mlflow_pw="${MLFLOW_DB_PASSWORD}" \
     --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}" <<'EOSQL'
CREATE ROLE mlflow LOGIN PASSWORD :'mlflow_pw';
CREATE DATABASE mlflow OWNER mlflow;
REVOKE ALL ON DATABASE mlflow FROM PUBLIC;
EOSQL
