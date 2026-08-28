import torch
import json
from typing import Dict, List, Optional
from neo4j import Session
from redis import Redis
from src.embeddings.train_graphsage import GraphSAGETrainer
from src.embeddings.supervised_classifier import SupervisedRingClassifier

class EmbeddingInference:
    def __init__(
        self,
        neo4j_session: Session,
        redis_client: Redis,
        model_dir: str = "/app/models",
        embedding_dim: int = 64
    ):
        self.session = neo4j_session
        self.redis = redis_client
        self.model_dir = model_dir
        self.embedding_dim = embedding_dim
        self.graphsage = GraphSAGETrainer(embedding_dim=embedding_dim)
        self.classifier = SupervisedRingClassifier(embedding_dim=embedding_dim)
        
        try:
            self.graphsage.load_model(
                f"{model_dir}/graphsage.pt",
                in_dim=4
            )
            self.classifier.load(f"{model_dir}/ring_classifier.pt")
            self.loaded = True
        except FileNotFoundError:
            self.loaded = False
    
    def get_embedding(self, account_id: str) -> Optional[List[float]]:
        if not self.loaded:
            return None
        
        cached = self.redis.get(f"embedding:{account_id}")
        if cached:
            return json.loads(cached)
        
        result = self.session.run(
            """
            MATCH (a:Account {id: $account_id})
            OPTIONAL MATCH (a)-[:USED]-(d:Device)
            OPTIONAL MATCH (a)-[:USED]-(i:IP)
            OPTIONAL MATCH (a)-[:TRANSACTED_WITH]-(m:Merchant)
            OPTIONAL MATCH (a)-[:OWNS]-(c:Card)
            RETURN count(DISTINCT d) AS device_count,
                   count(DISTINCT i) AS ip_count,
                   count(DISTINCT m) AS merchant_count,
                   count(DISTINCT c) AS card_count
            """,
            account_id=account_id
        )
        record = result.single()
        if not record:
            return None
        
        x = torch.tensor([[
            record["device_count"],
            record["ip_count"],
            record["merchant_count"],
            record["card_count"]
        ]], dtype=torch.float)
        
        with torch.no_grad():
            embedding = self.graphsage.model(x, torch.empty(0, 2))
        
        embedding_list = embedding.numpy().tolist()[0]
        self.redis.setex(f"embedding:{account_id}", 3600, json.dumps(embedding_list))
        
        return embedding_list
    
    def predict_ring_membership(self, account_id: str) -> Optional[float]:
        if not self.loaded:
            return None
        
        embedding = self.get_embedding(account_id)
        if embedding is None:
            return None
        
        return self.classifier.predict(embedding)