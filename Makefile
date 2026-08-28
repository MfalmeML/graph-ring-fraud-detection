.PHONY: help install test docker-up docker-down shadow train

PYTHON := .venv/Scripts/python.exe
COMPOSE := docker compose -f deployment/docker-compose.yml

help:
	@echo "Available targets:"
	@echo "  install      Install Python dependencies"
	@echo "  test         Run integration tests"
	@echo "  docker-up    Start all services with Docker Compose"
	@echo "  docker-down  Stop all services"
	@echo "  shadow       Run shadow mode with 1% sample"
	@echo "  train        Train fusion model from labels"

install:
	$(PYTHON) -m pip install -r requirements.txt

test:
	$(COMPOSE) up -d --wait neo4j redis kafka zookeeper
	$(PYTHON) -m pytest tests/ -v --tb=short

docker-up:
	docker build -f deployment/Dockerfile.base -t graph-ring-fraud-base:latest .
	$(COMPOSE) up -d

docker-down:
	$(COMPOSE) down

shadow:
	$(COMPOSE) up shadow-mode

train:
	bash scripts/train_fusion.sh
