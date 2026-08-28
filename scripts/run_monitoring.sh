#!/bin/bash

echo "=== Running Drift Detection ==="

curl -X POST http://localhost:8003/monitoring/drift/check

echo ""
echo "=== Performance Report ==="

curl http://localhost:8003/monitoring/performance

echo ""
echo "=== Drift Summary ==="

curl http://localhost:8003/monitoring/drift