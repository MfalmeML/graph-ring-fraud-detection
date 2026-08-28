#!/bin/bash

echo "=== Running TTL Cleanup ==="

python -c "
from neo4j import GraphDatabase
from src.graph_builder.ttl_manager import TTLManager
import os

driver = GraphDatabase.driver(
    os.environ.get('NEO4J_URI', 'bolt://localhost:7687'),
    auth=(os.environ.get('NEO4J_USER', 'neo4j'), os.environ.get('NEO4J_PASSWORD', 'password'))
)

with driver.session() as session:
    manager = TTLManager(session)
    result = manager.run_cleanup()
    print(f'Cleanup completed: {result}')
driver.close()
"