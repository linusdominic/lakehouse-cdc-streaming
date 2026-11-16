SHELL := /bin/bash
.DEFAULT_GOAL := help
COMPOSE := docker compose
export DBT_PROFILES_DIR := $(CURDIR)/dbt

.PHONY: help up down demo verify verify-negative chaos maintenance dbt dbt-docs report test test-spark lint fmt logs ps trino

help: ## List targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-14s %s\n", $$1, $$2}'

.env:
	cp .env.example .env

up: .env ## Start the stack, seed Postgres, register the Debezium connector
	uv sync --all-groups --quiet
	$(COMPOSE) up -d --build --wait
	uv run cdc seed
	uv run cdc register-connector
	@echo "Stack is up. Next: make demo"

demo: ## Workload with injected faults, reconcile, compact, reconcile again, dbt build
	./scripts/demo.sh

verify: ## Drain the pipeline, then reconcile Postgres against silver (exit 1 on mismatch)
	uv run cdc drain
	uv run cdc verify

verify-negative: ## Corrupt silver on purpose, show verify fails, roll back
	./scripts/verify_negative.sh

chaos: ## Kill the streaming job mid-stream (three ways), replay Kafka offsets, reconcile
	./scripts/chaos.sh

maintenance: ## Compaction, snapshot expiry, orphan-file removal (pauses the stream)
	./scripts/maintenance.sh

dbt: ## Build and test the gold layer
	uv run dbt build --project-dir dbt

dbt-docs: ## Generate dbt docs into dbt/target
	uv run dbt docs generate --project-dir dbt

report: ## Consumer lag, freshness and batch statistics
	uv run cdc report

test: ## Unit tests (pure Python, no containers)
	uv run pytest

test-spark: ## Spark tests against a local Iceberg catalog, inside the Spark image
	$(COMPOSE) --profile jobs run --rm --no-deps spark-test

lint: ## ruff check + format check
	uv run ruff check .
	uv run ruff format --check .

fmt: ## Apply ruff fixes and formatting
	uv run ruff check --fix .
	uv run ruff format .

logs: ## Tail the streaming job
	$(COMPOSE) logs -f streaming

ps: ## Container status and memory
	$(COMPOSE) ps
	@docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}' | grep -E 'NAME|lakehouse-cdc'

trino: ## Open a Trino SQL shell on the lake catalog
	$(COMPOSE) exec trino trino --catalog lake

down: ## Stop everything and delete all volumes and run artifacts
	$(COMPOSE) --profile jobs down -v --remove-orphans
	rm -f artifacts/*.json artifacts/*.jsonl artifacts/*.csv artifacts/*.log
