# ============================================================================
# Stock Intelligence Platform - developer entry points.  `make help` lists them.
# ============================================================================
SHELL := /usr/bin/env bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

COMPOSE ?= docker compose
UV ?= uv
ENV_FILE := .env
# Load .env for host-side commands when it exists (tests, migrations).
UV_RUN := $(UV) run --frozen $(if $(wildcard $(ENV_FILE)),--env-file $(ENV_FILE),)

INFRA_SERVICES := kafka postgres redis mlflow prometheus grafana

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z0-9_-]+:.*##/ {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# --- setup ------------------------------------------------------------------
.PHONY: install env
install: ## Install Python toolchain + deps (uv) and git hooks
	$(UV) sync --frozen
	$(UV) run --frozen pre-commit install

env: ## Create .env from .env.example (never overwrites)
	@if [ -f $(ENV_FILE) ]; then echo "$(ENV_FILE) already exists"; \
	else cp .env.example $(ENV_FILE) && echo "created $(ENV_FILE) - change the passwords"; fi

# --- local stack --------------------------------------------------------------
.PHONY: dev infra-up down clean ps logs compose-config topics migrate kafka-ui
dev: infra-up ## Start the full local stack (grows as phases add services)

infra-up: $(ENV_FILE) ## Build + start infrastructure, run migrations & topic provisioning
	$(COMPOSE) build db-migrate
	$(COMPOSE) up -d --build --wait $(INFRA_SERVICES)
	$(COMPOSE) run --rm db-migrate
	$(COMPOSE) run --rm kafka-init

down: ## Stop containers (keep volumes)
	$(COMPOSE) --profile tools down

clean: ## Stop containers AND delete all volumes (destroys local data)
	$(COMPOSE) --profile tools down -v --remove-orphans

ps: ## Show service status
	$(COMPOSE) ps

logs: ## Tail logs (make logs s=kafka)
	$(COMPOSE) logs -f --tail=100 $(s)

compose-config: ## Validate docker-compose.yml
	$(COMPOSE) --env-file .env.example config --quiet && echo "compose file OK"

topics: ## (Re)provision Kafka topics from shared/kafka/topics.py
	$(COMPOSE) run --rm kafka-init

migrate: ## Apply database migrations
	$(COMPOSE) run --rm db-migrate

kafka-ui: ## Start Kafka UI on http://localhost:8081
	$(COMPOSE) --profile tools up -d kafka-ui

$(ENV_FILE):
	@echo "missing $(ENV_FILE): run 'make env' first" && exit 1

# --- data -------------------------------------------------------------------
.PHONY: data data-update-lock calibrate
data: ## Download + verify the pinned historical dataset into data/raw (read-only)
	$(UV) run --frozen python -m stockml.data.acquire

data-update-lock: ## Re-record dataset checksums after deliberately bumping the revision
	$(UV) run --frozen python -m stockml.data.acquire --update-lock

calibrate: data ## Re-estimate simulator parameters from the historical snapshot
	$(UV) run --frozen python -m stockml.data.calibrate

# --- quality ------------------------------------------------------------------
.PHONY: fmt lint typecheck test test-integration cov check
fmt: ## Auto-format and fix lint
	$(UV) run --frozen ruff format .
	$(UV) run --frozen ruff check --fix .

lint: ## Lint + format check
	$(UV) run --frozen ruff check .
	$(UV) run --frozen ruff format --check .

typecheck: ## Static type checking (mypy --strict)
	$(UV) run --frozen mypy

test: ## Unit tests (no infrastructure needed)
	$(UV) run --frozen pytest

test-integration: ## Integration tests against the running stack (make infra-up first)
	$(UV_RUN) pytest -m integration tests/integration

cov: ## Unit tests with coverage report
	$(UV) run --frozen pytest --cov --cov-report=term-missing

check: lint typecheck test ## Everything CI runs on a pull request
