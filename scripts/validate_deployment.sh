#!/bin/bash

printf '%s\n' "=== Validating Graph Ring Fraud Detection Deployment ==="

# Check Neo4j
printf '%s' "Neo4j: "
neo4j_status=$(curl -sS -o /dev/null -w "%{http_code}" -u neo4j:password http://localhost:7474 2>/dev/null) || neo4j_status="FAIL"
printf '%s\n' "$neo4j_status"

# Check Redis
printf '%s' "Redis: "
if redis-cli -h localhost ping 2>/dev/null | grep -q '^PONG$'; then
    printf '%s\n' "OK"
else
    printf '%s\n' "FAIL"
fi

# Check Kafka
printf '%s' "Kafka: "
if kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
    printf '%s\n' "OK"
else
    printf '%s\n' "FAIL"
fi

# Check Ring Score API
printf '%s' "Ring Score API: "
ring_score_status=$(curl -sS -o /dev/null -w "%{http_code}" http://localhost:8000/docs 2>/dev/null) || ring_score_status="FAIL"
printf '%s\n' "$ring_score_status"

# Check Investigator API
printf '%s' "Investigator API: "
investigator_status=$(curl -sS -o /dev/null -w "%{http_code}" http://localhost:8002/docs 2>/dev/null) || investigator_status="FAIL"
printf '%s\n' "$investigator_status"

# Test ring score lookup
printf '%s' "Ring Score Lookup (test account): "
ring_score_response=$(curl -sS -X GET "http://localhost:8000/ring-score/test_account_123" -H "accept: application/json" 2>/dev/null) || ring_score_response=""
if [[ "$ring_score_response" == *"ring_score"* ]]; then
    printf '%s\n' "OK"
else
    printf '%s\n' "FAIL (response: $ring_score_response)"
fi

# Test investigator endpoints
printf '%s' "Pending Rings: "
pending_rings_response=$(curl -sS -X GET "http://localhost:8002/rings/pending" -H "accept: application/json" 2>/dev/null) || pending_rings_response=""
if [[ "$pending_rings_response" == "[]" ]] || [[ "$pending_rings_response" == *"ring_id"* ]]; then
    printf '%s\n' "OK"
else
    printf '%s\n' "FAIL (response: $pending_rings_response)"
fi

printf '%s\n' "=== Validation Complete ==="
