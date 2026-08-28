#!/bin/bash

set -e

echo "=== Running Local Graph Fraud Detection (No Kafka) ==="

# Activate virtual environment
source venv/bin/activate

# Set environment
export NEO4J_URI="bolt://localhost:7687"
export NEO4J_USER="neo4j"
export NEO4J_PASSWORD="password"
export REDIS_HOST="localhost"
export REDIS_PORT="6379"
export USE_LOCAL_QUEUE="true"

# Start graph builder in background
echo "Starting graph builder..."
python -c "
from neo4j import GraphDatabase
from src.ingestion.local_consumer import LocalGraphBuilder, message_queue

driver = GraphDatabase.driver('bolt://localhost:7687', auth=('neo4j', 'password'))
builder = LocalGraphBuilder(driver, message_queue)
builder.start()
import time
time.sleep(2)
print('Graph builder running')
" &

# Start API
echo "Starting API..."
uvicorn src.serving.api:app --host 0.0.0.0 --port 8000 &

echo ""
echo "=== Services Running ==="
echo "  API: http://localhost:8000"
echo "  Neo4j: http://localhost:7474"
echo ""
echo "To generate test ring: python scripts/generate_local_ring.py"
echo "To stop: kill %1 %2"