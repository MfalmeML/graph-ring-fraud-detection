import json
from typing import Optional
from kafka import KafkaConsumer
from neo4j import GraphDatabase, Session
from .event_parser import parse_transaction

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
    
    def run(self):
        try:
            for message in self.consumer:
                event = message.value
                nodes, edges = parse_transaction(event)
                with self.driver.session() as session:
                    self._write_nodes(session, nodes)
                    self._write_edges(session, edges)
        finally:
            self.driver.close()
    
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