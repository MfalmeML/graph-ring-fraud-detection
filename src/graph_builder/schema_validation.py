import re
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from src.graph_builder.schema import NodeType, EdgeType

class SchemaValidator:
    def __init__(self):
        self.node_validators = {
            NodeType.ACCOUNT: self._validate_account,
            NodeType.DEVICE: self._validate_device,
            NodeType.IP: self._validate_ip,
            NodeType.MERCHANT: self._validate_merchant,
            NodeType.CARD: self._validate_card
        }
        self.edge_validators = {
            EdgeType.USED: self._validate_used,
            EdgeType.SEEN_AT: self._validate_seen_at,
            EdgeType.TRANSACTED_WITH: self._validate_transacted_with,
            EdgeType.OWNS: self._validate_owns
        }
    
    def validate_node(self, node_type: NodeType, properties: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        validator = self.node_validators.get(node_type)
        if not validator:
            return False, f"Unknown node type: {node_type}"
        return validator(properties)
    
    def validate_edge(self, edge_type: EdgeType, properties: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        validator = self.edge_validators.get(edge_type)
        if not validator:
            return False, f"Unknown edge type: {edge_type}"
        return validator(properties)
    
    def _validate_account(self, props: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        required = ["id"]
        for field in required:
            if field not in props:
                return False, f"Missing required field: {field}"
        if not props["id"] or len(props["id"]) < 3:
            return False, "Account ID must be at least 3 characters"
        return True, None
    
    def _validate_device(self, props: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        if "id" not in props:
            return False, "Missing required field: id"
        if not props["id"]:
            return False, "Device ID cannot be empty"
        return True, None
    
    def _validate_ip(self, props: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        if "id" not in props:
            return False, "Missing required field: id"
        ip_pattern = r'^(\d{1,3}\.){3}\d{1,3}$'
        if not re.match(ip_pattern, props["id"]):
            return False, f"Invalid IP address format: {props['id']}"
        return True, None
    
    def _validate_merchant(self, props: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        if "id" not in props:
            return False, "Missing required field: id"
        if not props["id"]:
            return False, "Merchant ID cannot be empty"
        return True, None
    
    def _validate_card(self, props: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        if "id" not in props:
            return False, "Missing required field: id"
        if not props["id"]:
            return False, "Card ID cannot be empty"
        return True, None
    
    def _validate_used(self, props: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        return True, None
    
    def _validate_seen_at(self, props: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        return True, None
    
    def _validate_transacted_with(self, props: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        return True, None
    
    def _validate_owns(self, props: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        return True, None

class EntityResolution:
    def __init__(self):
        self.ip_subnet_cache = {}
    
    def resolve_ip_subnet(self, ip_address: str, prefix_length: int = 24) -> str:
        parts = ip_address.split('.')
        if len(parts) == 4:
            subnet_parts = parts[:prefix_length // 8]
            if prefix_length % 8 > 0:
                subnet_parts.append(str(int(parts[prefix_length // 8]) & (0xFF << (8 - prefix_length % 8))))
            return '.'.join(subnet_parts)
        return ip_address
    
    def resolve_device_fingerprint(self, device_fingerprint: str) -> str:
        if not device_fingerprint:
            return "unknown_device"
        
        normalized = device_fingerprint.strip().lower()
        normalized = re.sub(r'[^a-zA-Z0-9_]', '_', normalized)
        
        if len(normalized) > 64:
            normalized = normalized[:64]
        
        return normalized
    
    def resolve_identity(self, account_id: str, device_id: str, ip_address: str) -> str:
        identity_hash = f"{account_id}|{device_id}|{ip_address}"
        import hashlib
        return hashlib.md5(identity_hash.encode()).hexdigest()