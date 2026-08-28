from collections import defaultdict
from typing import List
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
        nodes_by_type = defaultdict(list)
        for node in nodes:
            nodes_by_type[node.type.value].append(node)

        for node_type, typed_nodes in nodes_by_type.items():
            for i in range(0, len(typed_nodes), self.batch_size):
                batch = typed_nodes[i:i + self.batch_size]
                query = f"""
                UNWIND $nodes AS node
                MERGE (n:{node_type} {{id: node.id}})
                SET n.updated_at = node.updated_at,
                    n.created_at = COALESCE(n.created_at, node.created_at)
                """
                self.session.run(
                    query,
                    nodes=[{
                        "id": n.id,
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
        edges_by_type = defaultdict(list)
        for edge in edges:
            edges_by_type[edge.type.value].append(edge)

        for edge_type, typed_edges in edges_by_type.items():
            for i in range(0, len(typed_edges), self.batch_size):
                batch = typed_edges[i:i + self.batch_size]
                query = f"""
                UNWIND $edges AS edge
                MATCH (source {{id: edge.source_id}})
                MATCH (target {{id: edge.target_id}})
                MERGE (source)-[r:{edge_type}]->(target)
                SET r.created_at = COALESCE(r.created_at, edge.created_at),
                    r.timestamp = edge.timestamp
                """
                self.session.run(
                    query,
                    edges=[{
                        "source_id": e.source_id,
                        "target_id": e.target_id,
                        "created_at": e.created_at.isoformat(),
                        "timestamp": e.timestamp.isoformat() if e.timestamp else None
                    } for e in batch]
                )
                total += len(batch)
        logger.info(f"Loaded {total} edges")
        return total