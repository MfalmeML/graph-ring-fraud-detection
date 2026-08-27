import redis
import json
from typing import Dict, Any, Optional
from neo4j import Session
from src.features.structural import StructuralFeatureCalculator
from src.features.temporal import TemporalFeatureCalculator

class FeatureCache:
    def __init__(
        self,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_db: int = 0,
        ttl_seconds: int = 300
    ):
        self.redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            decode_responses=True
        )
        self.ttl = ttl_seconds
    
    def compute_and_cache_account_features(
        self,
        account_id: str,
        neo4j_session: Session
    ) -> Dict[str, Any]:
        structural = StructuralFeatureCalculator(neo4j_session)
        temporal = TemporalFeatureCalculator(neo4j_session)
        
        features = {
            "device_account_count": structural.device_account_count(account_id),
            "ip_account_count": structural.ip_account_count(account_id),
            "merchant_account_diversity": structural.merchant_account_diversity(account_id),
            "triangle_count": structural.triangle_count(account_id),
            "clustering_coefficient": structural.clustering_coefficient(account_id),
            "connected_component_size": structural.connected_component_size(account_id),
            "shared_entity_count": structural.shared_entity_count(account_id),
            "new_edges_last_1h": temporal.new_edges_last_hour(account_id),
            "new_edges_last_24h": temporal.new_edges_last_day(account_id),
            "edge_formation_burstiness": temporal.edge_formation_burstiness(account_id)
        }
        
        key = f"account_features:{account_id}"
        self.redis_client.setex(
            key,
            self.ttl,
            json.dumps(features)
        )
        return features
    
    def get_features(self, account_id: str) -> Optional[Dict[str, Any]]:
        key = f"account_features:{account_id}"
        data = self.redis_client.get(key)
        if data:
            return json.loads(data)
        return None