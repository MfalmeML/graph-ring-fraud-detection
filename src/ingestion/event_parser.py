from datetime import datetime
from typing import Tuple, List
from src.graph_builder.schema import NodeType, EdgeType
from src.graph_builder.models import GraphNode, GraphEdge

def parse_transaction(event: dict) -> Tuple[List[GraphNode], List[GraphEdge]]:
    nodes = []
    edges = []
    timestamp = datetime.fromisoformat(event.get("timestamp", datetime.utcnow().isoformat()))
    
    account_id = event.get("account_id")
    device_id = event.get("device_id")
    ip_address = event.get("ip_address")
    merchant_id = event.get("merchant_id")
    card_id = event.get("card_id")
    
    if account_id:
        nodes.append(GraphNode(id=account_id, type=NodeType.ACCOUNT))
    if device_id:
        nodes.append(GraphNode(id=device_id, type=NodeType.DEVICE))
    if ip_address:
        nodes.append(GraphNode(id=ip_address, type=NodeType.IP))
    if merchant_id:
        nodes.append(GraphNode(id=merchant_id, type=NodeType.MERCHANT))
    if card_id:
        nodes.append(GraphNode(id=card_id, type=NodeType.CARD))
    
    if account_id and device_id:
        edges.append(GraphEdge(
            source_id=account_id,
            target_id=device_id,
            type=EdgeType.USED,
            timestamp=timestamp
        ))
    if account_id and ip_address:
        edges.append(GraphEdge(
            source_id=account_id,
            target_id=ip_address,
            type=EdgeType.USED,
            timestamp=timestamp
        ))
    if account_id and merchant_id:
        edges.append(GraphEdge(
            source_id=account_id,
            target_id=merchant_id,
            type=EdgeType.TRANSACTED_WITH,
            timestamp=timestamp
        ))
    if account_id and card_id:
        edges.append(GraphEdge(
            source_id=account_id,
            target_id=card_id,
            type=EdgeType.OWNS,
            timestamp=timestamp
        ))
    if device_id and ip_address:
        edges.append(GraphEdge(
            source_id=device_id,
            target_id=ip_address,
            type=EdgeType.SEEN_AT,
            timestamp=timestamp
        ))
    
    return nodes, edges