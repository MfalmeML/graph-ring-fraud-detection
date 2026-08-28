from typing import Dict, Any, Optional
from neo4j import Session
from .feature_cache import FeatureCache

class RingScoreCalculator:
    def __init__(
        self,
        feature_cache: FeatureCache,
        neo4j_session: Session,
        unsupervised_score_weight: float = 0.7,
        structural_score_weight: float = 0.3
    ):
        self.cache = feature_cache
        self.session = neo4j_session
        self.unsupervised_weight = unsupervised_score_weight
        self.structural_weight = structural_score_weight
    
    def compute_ring_score(self, account_id: str) -> float:
        features = self.cache.get_features(account_id)
        if not features:
            features = self.cache.compute_and_cache_account_features(
                account_id,
                self.session
            )
        
        structural_score = self._structural_component(features)
        unsupervised_score = self._unsupervised_component(account_id)
        
        return (
            self.unsupervised_weight * unsupervised_score +
            self.structural_weight * structural_score
        )
    
    def _structural_component(self, features: Dict[str, Any]) -> float:
        scores = [
            min(features.get("device_account_count", 0) / 10.0, 1.0),
            min(features.get("ip_account_count", 0) / 10.0, 1.0),
            min(features.get("merchant_account_diversity", 0) / 20.0, 1.0),
            min(features.get("triangle_count", 0) / 5.0, 1.0),
            features.get("clustering_coefficient", 0.0),
            min(features.get("connected_component_size", 0) / 50.0, 1.0),
            min(features.get("new_edges_last_1h", 0) / 10.0, 1.0)
        ]
        return sum(scores) / len(scores)
    
    def _unsupervised_component(self, account_id: str) -> float:
        result = self.session.run(
            """
            MATCH (a:Account {id: $account_id})
            OPTIONAL MATCH (a)-[:USED|TRANSACTED_WITH|OWNS]-(entity)<-[:USED|TRANSACTED_WITH|OWNS]-(other:Account)
            WITH a, collect(DISTINCT other) AS connected
            WITH a, size(connected) AS degree
            OPTIONAL MATCH (a)-[:USED|TRANSACTED_WITH|OWNS]-(e1)<-[:USED|TRANSACTED_WITH|OWNS]-(other1:Account)
            OPTIONAL MATCH (other1)-[:USED|TRANSACTED_WITH|OWNS]-(e2)<-[:USED|TRANSACTED_WITH|OWNS]-(other2:Account)
            WHERE other2 IN connected AND other1 <> other2
                 WITH degree, count(DISTINCT [other1.id, other2.id]) AS pair_count
            RETURN CASE WHEN degree > 1 THEN 
                     pair_count / (degree * (degree - 1))
                   ELSE 0.0 END AS density
            """,
            account_id=account_id
        )
        record = result.single()
        return record["density"] if record else 0.0