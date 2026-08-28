import json
import os
from typing import Optional
from kafka import KafkaConsumer
from neo4j import GraphDatabase, Session
from .event_parser import parse_transaction
from src.graph_builder.batch_loader import BatchGraphLoader
from src.graph_builder.entity_resolution import EntityResolver
import logging

logger = logging.getLogger(__name__)

class TransactionConsumer:
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        neo4j_uri: str,
        neo4j_user: str,
        neo4j_password: str,
        group_id: str = "graph-builder"
    ):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode("utf-8"))
        )
        self.driver = GraphDatabase.driver(
            neo4j_uri,
            auth=(neo4j_user, neo4j_password)
        )
        self.entity_resolver = EntityResolver()
        self.batch_size = int(os.getenv("GRAPH_BATCH_SIZE", "100"))
    
    def run(self):
        nodes = []
        edges = []
        try:
            for message in self.consumer:
                event = message.value
                try:
                    event = self.entity_resolver.resolve_transaction(event)
                    event_nodes, event_edges = parse_transaction(event)
                except ValueError as exc:
                    logger.warning("Skipping invalid transaction: %s", exc)
                    continue

                nodes.extend(event_nodes)
                edges.extend(event_edges)
                if len(nodes) >= self.batch_size:
                    self._flush(nodes, edges)
                    nodes.clear()
                    edges.clear()
        finally:
            if nodes or edges:
                self._flush(nodes, edges)
            self.driver.close()

    def _flush(self, nodes, edges):
        with self.driver.session() as session:
            loader = BatchGraphLoader(session, batch_size=self.batch_size)
            loader.load_nodes(nodes)
            loader.load_edges(edges)
    
    def _write_nodes(self, session: Session, nodes):
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
    
    def _write_edges(self, session: Session, edges):
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


if __name__ == "__main__":
    consumer = TransactionConsumer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        topic=os.getenv("KAFKA_TOPIC", "transactions"),
        neo4j_uri=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        neo4j_user=os.getenv("NEO4J_USER", "neo4j"),
        neo4j_password=os.getenv("NEO4J_PASSWORD", "password")
    )
    consumer.run()