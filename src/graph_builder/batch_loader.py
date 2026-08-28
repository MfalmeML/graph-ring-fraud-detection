from typing import List, Dict, Any
from neo4j import Session
from src.graph_builder.models import GraphNode, GraphEdge
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BatchGraphLoader:
    def __init__(self, session: Session, batch_size: int = 1000):
        self.session = session
        self.batch_size = batch_size
    
    def load_nodes(self, nodes: List[GraphNode]) -> int:
        if not nodes:
            return 0
        
        total = 0
        for i in range(0, len(nodes), self.batch_size):
            batch = nodes[i:i + self.batch_size]
            query = """
            UNWIND $nodes AS node
            MERGE (n {id: node.id})
            SET n:{node.type}, 
                n.updated_at = node.updated_at,
                n.created_at = COALESCE(n.created_at, node.created_at)
            """
            result = self.session.run(
                query,
                nodes=[{
                    "id": n.id,
                    "type": n.type.value,
                    "updated_at": n.updated_at.isoformat(),
                    "created_at": n.created_at.isoformat()
                } for n in batch]
            )
            total += len(batch)
        logger.info(f"Loaded {total} nodes")
        return total
    
    def load_edges(self, edges: List[GraphEdge]) -> int:
        if not edges:
            return 0
        
        total = 0
        for i in range(0, len(edges), self.batch_size):
            batch = edges[i:i + self.batch_size]
            query = """
            UNWIND $edges AS edge
            MATCH (source {id: edge.source_id})
            MATCH (target {id: edge.target_id})
            MERGE (source)-[r:{edge.type}]->(target)
            SET r.created_at = COALESCE(r.created_at, edge.created_at),
                r.timestamp = edge.timestamp
            """
            result = self.session.run(
                query,
                edges=[{
                    "source_id": e.source_id,
                    "target_id": e.target_id,
                    "type": e.type.value,
                    "created_at": e.created_at.isoformat(),
                    "timestamp": e.timestamp.isoformat() if e.timestamp else None
                } for e in batch]
            )
            total += len(batch)
        logger.info(f"Loaded {total} edges")
        return total