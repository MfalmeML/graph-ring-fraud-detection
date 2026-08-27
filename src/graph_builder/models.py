from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any
from .schema import NodeType, EdgeType

@dataclass
class GraphNode:
    id: str
    type: NodeType
    properties: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)

@dataclass
class GraphEdge:
    source_id: str
    target_id: str
    type: EdgeType
    properties: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    timestamp: Optional[datetime] = None