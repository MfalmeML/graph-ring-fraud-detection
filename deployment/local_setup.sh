#!/bin/bash

set -e

echo "=== Local Graph Ring Fraud Detection Setup ==="

# Check prerequisites
command -v python3 >/dev/null 2>&1 || { echo "Python3 required"; exit 1; }
command -v docker >/dev/null 2>&1 || { echo "Docker required"; exit 1; }
command -v docker-compose >/dev/null 2>&1 || { echo "docker-compose required"; exit 1; }

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# Create directories
mkdir -p logs models data/neo4j data/redis data/postgres

# Set environment variables for local development
export NEO4J_URI="bolt://localhost:7687"
export NEO4J_USER="neo4j"
export NEO4J_PASSWORD="password"
export REDIS_HOST="localhost"
export REDIS_PORT="6379"
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export MODEL_DIR="./models"
export LOG_LEVEL="INFO"

echo "Environment configured"

# Start services with docker-compose
echo "Starting services..."
docker-compose -f deployment/docker-compose.yml up -d

# Wait for services to be ready
echo "Waiting for services to be ready..."
sleep 10

# Check Neo4j
echo -n "Neo4j: "
curl -s -o /dev/null -w "%{http_code}\n" -u neo4j:password http://localhost:7474 || echo "FAIL"

# Check Redis
echo -n "Redis: "
redis-cli -h localhost ping || echo "FAIL"

# Check Kafka
echo -n "Kafka: "
kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1 && echo "OK" || echo "FAIL"

# Create Kafka topic if not exists
kafka-topics --bootstrap-server localhost:9092 --create --topic transactions --partitions 3 --replication-factor 1 --if-not-exists
kafka-topics --bootstrap-server localhost:9092 --create --topic shadow_decisions --partitions 1 --replication-factor 1 --if-not-exists

echo ""
echo "=== Setup Complete ==="
echo ""
echo "Services running:"
echo "  Neo4j:      http://localhost:7474 (user: neo4j, password: password)"
echo "  Redis:      localhost:6379"
echo "  Kafka:      localhost:9092"
echo "  API:        http://localhost:8000"
echo "  Investigator: http://localhost:8002"
echo "  Monitoring: http://localhost:8003"
echo ""
echo "To stop services: docker-compose -f deployment/docker-compose.yml down"
echo "To run tests: pytest tests/ -v"
echo "To generate test data: python scripts/generate_test_data.py"