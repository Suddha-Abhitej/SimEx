# Local stack shortcuts. On Windows without make, use scripts/dev.ps1 (same targets).
.PHONY: up down logs ps smoke ingest book

COMPOSE ?= docker compose
PYTHON ?= python

up:  ## Start the stack and wait until every service is healthy
	$(COMPOSE) up -d --wait

down:  ## Stop the stack (data volumes are kept; add -v to delete them)
	$(COMPOSE) down

logs:  ## Follow logs; narrow with: make logs s=kafka
	$(COMPOSE) logs -f --tail=100 $(s)

ps:  ## Show services and health
	$(COMPOSE) ps

smoke:  ## Check Kafka, Postgres and Valkey from the host
	$(PYTHON) scripts/smoke_test.py

ingest:  ## Run the market-data ingestor (Gemini) against the local stack (metrics on :9101/metrics)
	$(PYTHON) -m simex.ingest

book:  ## Run the order book service: md.level2 to Valkey (metrics on :9102/metrics)
	$(PYTHON) -m simex.book
