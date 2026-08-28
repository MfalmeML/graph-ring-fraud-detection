#!/bin/bash

set -e

echo "=== Offline Local Setup (No Cloud) ==="

# Check prerequisites
command -v python3 >/dev/null 2>&1 || { echo "Python3 required"; exit 1; }
command -v java >/dev/null 2>&1 || { echo "Java required for Neo4j"; exit 1; }

# Create directories
mkdir -p services logs models data/neo4j data/redis

# Download Neo4j if not present
if [ ! -f services/neo4j-community-5.14.0-unix.tar.gz ]; then
    echo "Downloading Neo4j (offline install)..."
    wget -O services/neo4j-community-5.14.0-unix.tar.gz https://dist.neo4j.org/neo4j-community-5.14.0-unix.tar.gz
fi

# Extract Neo4j
if [ ! -d services/neo4j ]; then
    echo "Extracting Neo4j..."
    tar -xzf services/neo4j-community-5.14.0-unix.tar.gz -C services/
    mv services/neo4j-community-5.14.0 services/neo4j
fi

# Configure Neo4j
cat > services/neo4j/conf/neo4j.conf << EOF
dbms.default_listen_address=0.0.0.0
dbms.connector.bolt.enabled=true
dbms.connector.bolt.listen_address=0.0.0.0:7687
dbms.connector.http.enabled=true
dbms.connector.http.listen_address=0.0.0.0:7474
dbms.security.auth_enabled=false
dbms.memory.heap.initial_size=512m
dbms.memory.heap.max_size=1024m
dbms.memory.pagecache.size=512m
dbms.logs.debug.enabled=false
EOF

# Start Neo4j
echo "Starting Neo4j..."
cd services/neo4j
./bin/neo4j start
cd ../..

# Install Redis locally if not present
if ! command -v redis-server >/dev/null 2>&1; then
    echo "Redis not found. Installing from source..."
    wget -O services/redis-7.2.4.tar.gz https://download.redis.io/releases/redis-7.2.4.tar.gz
    tar -xzf services/redis-7.2.4.tar.gz -C services/
    cd services/redis-7.2.4
    make
    cd ../..
fi

# Start Redis
echo "Starting Redis..."
services/redis-7.2.4/src/redis-server --port 6379 --daemonize yes --appendonly yes --dir ./data/redis

# Python virtual environment
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# Create simple Kafka replacement using Python
cat > services/kafka_simulator.py << 'EOF'
import json
import threading
import time
from collections import defaultdict, deque
from typing import List, Dict, Any, Callable

class LocalMessageQueue:
    def __init__(self):
        self.topics = defaultdict(deque)
        self.subscribers = defaultdict(list)
        self.lock = threading.Lock()
    
    def produce(self, topic: str, message: Dict[str, Any]):
        with self.lock:
            self.topics[topic].append(message)
            for callback in self.subscribers.get(topic, []):
                callback(message)
    
    def consume(self, topic: str, callback: Callable):
        with self.lock:
            self.subscribers[topic].append(callback)
            # Replay existing messages
            for msg in list(self.topics[topic]):
                callback(msg)
    
    def get_messages(self, topic: str, limit: int = 100) -> List[Dict]:
        with self.lock:
            return list(self.topics[topic])[-limit:]

queue = LocalMessageQueue()

def produce_transaction(account_id, device_id, ip_address, merchant_id, card_id):
    import time
    import random
    from datetime import datetime
    msg = {
        "transaction_id": f"tx_{int(time.time())}_{random.randint(1000,9999)}",
        "account_id": account_id,
        "device_id": device_id,
        "ip_address": ip_address,
        "merchant_id": merchant_id,
        "card_id": card_id,
        "timestamp": datetime.utcnow().isoformat(),
        "amount": round(random.uniform(10, 500), 2),
        "tabular_fraud_probability": random.uniform(0.01, 0.95)
    }
    queue.produce("transactions", msg)
    return msg
EOF

echo ""
echo "=== Offline Setup Complete ==="
echo ""
echo "Services running:"
echo "  Neo4j: http://localhost:7474 (auth disabled)"
echo "  Redis: localhost:6379"
echo "  Message Queue: Python in-memory (no Kafka required)"
echo ""
echo "To start API: uvicorn src.serving.api:app --host 0.0.0.0 --port 8000"
echo "To generate test data: python -c 'from services.kafka_simulator import produce_transaction; produce_transaction(\"acc_test\", \"dev_test\", \"192.168.1.1\", \"mer_test\", \"card_test\")'"