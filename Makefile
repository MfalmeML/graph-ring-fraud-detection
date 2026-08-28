.PHONY: help install test docker-up docker-down shadow train

help:
	@echo "Available targets:"
	@echo "  install      Install Python dependencies"
	@echo "  test         Run integration tests"
	@echo "  docker-up    Start all services with Docker Compose"
	@echo "  docker-down  Stop all services"
	@echo "  shadow       Run shadow mode with 1% sample"
	@echo "  train        Train fusion model from labels"

install:
	pip install -r requirements.txt

test:
	pytest tests/ -v --tb=short

docker-up:
	docker compose -f deployment/docker-compose.yml up -d

docker-down:
	docker compose -f deployment/docker-compose.yml down

shadow:
	docker compose -f deployment/docker-compose.yml up shadow-mode

train:
	bash scripts/train_fusion.sh
