import json
import threading
import time
from typing import Optional, Callable, List, Dict, Any
from datetime import datetime
from collections import deque

from src.ingestion.event_parser import parse_transaction
from src.graph_builder.models import GraphNode, GraphEdge

class LocalMessageQueue:
    def __init__(self):
        self.topics = {}
        self.subscribers = {}
        self.lock = threading.Lock()
    
    def subscribe(self, topic: str, callback: Callable):
        with self.lock:
            if topic not in self.subscribers:
                self.subscribers[topic] = []
            self.subscribers[topic].append(callback)
    
    def publish(self, topic: str, message: Dict[str, Any]):
        with self.lock:
            if topic not in self.topics:
                self.topics[topic] = deque(maxlen=10000)
            self.topics[topic].append(message)
            
            for callback in self.subscribers.get(topic, []):
                try:
                    callback(message)
                except Exception as e:
                    print(f"Callback error: {e}")
    
    def get_messages(self, topic: str, limit: int = 100) -> List[Dict]:
        with self.lock:
            if topic not in self.topics:
                return []
            return list(self.topics[topic])[-limit:]

class LocalGraphBuilder:
    def __init__(self, neo4j_driver, queue: LocalMessageQueue, topic: str = "transactions"):
        self.driver = neo4j_driver
        self.queue = queue
        self.topic = topic
        self.running = False
        self.thread = None
    
    def start(self):
        self.running = True
        self.queue.subscribe(self.topic, self._process_message)
        print(f"LocalGraphBuilder listening on topic: {self.topic}")
    
    def _process_message(self, event: Dict[str, Any]):
        try:
            nodes, edges = parse_transaction(event)
            if not nodes and not edges:
                return
            
            with self.driver.session() as session:
                self._write_nodes(session, nodes)
                self._write_edges(session, edges)
        except Exception as e:
            print(f"Error processing message: {e}")
    
    def _write_nodes(self, session, nodes: List[GraphNode]):
        for node in nodes:
            session.run(
                f"""
                MERGE (n:{node.type.value} {{id: $id}})
                SET n.updated_at = $updated_at,
                    n.created_at = COALESCE(n.created_at, $created_at)
                """,
                id=node.id,
                updated_at=node.updated_at.isoformat(),
                created_at=node.created_at.isoformat()
            )
    
    def _write_edges(self, session, edges: List[GraphEdge]):
        for edge in edges:
            session.run(
                f"""
                MATCH (source {{id: $source_id}})
                MATCH (target {{id: $target_id}})
                MERGE (source)-[r:{edge.type.value}]->(target)
                SET r.created_at = COALESCE(r.created_at, $created_at),
                    r.timestamp = $timestamp
                """,
                source_id=edge.source_id,
                target_id=edge.target_id,
                created_at=edge.created_at.isoformat(),
                timestamp=edge.timestamp.isoformat()
            )
    
    def stop(self):
        self.running = False

# Global queue instance
message_queue = LocalMessageQueue()

def produce_transaction(
    account_id: str,
    device_id: str,
    ip_address: str,
    merchant_id: str,
    card_id: str,
    amount: float = 100.0
) -> Dict[str, Any]:
    import random
    import time
    event = {
        "transaction_id": f"tx_{int(time.time())}_{random.randint(1000,9999)}",
        "account_id": account_id,
        "device_id": device_id,
        "ip_address": ip_address,
        "merchant_id": merchant_id,
        "card_id": card_id,
        "timestamp": datetime.utcnow().isoformat(),
        "amount": amount,
        "tabular_fraud_probability": random.uniform(0.01, 0.95)
    }
    message_queue.publish("transactions", event)
    return event