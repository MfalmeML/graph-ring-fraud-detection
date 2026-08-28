import json
import logging
import time
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from neo4j import GraphDatabase
from redis import Redis
import numpy as np
from sklearn.model_selection import train_test_split

from src.embeddings.train_graphsage import GraphSAGETrainer
from src.embeddings.supervised_classifier import SupervisedRingClassifier
from src.fusion.learned_fusion import LearnedFusion
from src.community.storage import CommunityStorage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TrainingPipeline:
    def __init__(
        self,
        neo4j_uri: str,
        neo4j_user: str,
        neo4j_password: str,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        embedding_dim: int = 64,
        model_dir: str = "/app/models"
    ):
        self.driver = GraphDatabase.driver(
            neo4j_uri,
            auth=(neo4j_user, neo4j_password)
        )
        self.redis_client = Redis(
            host=redis_host,
            port=redis_port,
            decode_responses=True
        )
        self.embedding_dim = embedding_dim
        self.model_dir = model_dir
        self.graphsage_trainer = GraphSAGETrainer(
            embedding_dim=embedding_dim,
            num_layers=2,
            epochs=100
        )
        self.classifier = SupervisedRingClassifier(
            embedding_dim=embedding_dim,
            epochs=50
        )
        self.fusion = LearnedFusion(
            redis_host=redis_host,
            redis_port=redis_port
        )
    
    def get_labeled_accounts(self, min_positive: int = 10) -> Tuple[Dict[str, int], List[str]]:
        with self.driver.session() as session:
            result = session.run(
                """
                MATCH (r:RingCandidate)
                WHERE r.status IN ['CONFIRMED', 'REJECTED']
                OPTIONAL MATCH (r)<-[:BELONGS_TO_RING]-(members:Account)
                WITH r, collect(members.id) AS member_ids
                UNWIND member_ids AS account_id
                RETURN account_id,
                       CASE WHEN r.status = 'CONFIRMED' THEN 1 ELSE 0 END AS label
                """
            )
            labels = {record["account_id"]: record["label"] for record in result}
        
        positive = [acc for acc, label in labels.items() if label == 1]
        negative = [acc for acc, label in labels.items() if label == 0]
        
        logger.info(f"Found {len(positive)} confirmed, {len(negative)} rejected accounts")
        
        if len(positive) < min_positive:
            logger.warning(f"Only {len(positive)} positive labels. Need {min_positive} for training.")
            return labels, []
        
        return labels, positive + negative
    
    def get_tabular_scores(self, account_ids: List[str]) -> Dict[str, float]:
        scores = {}
        for account_id in account_ids:
            cached = self.redis_client.get(f"tabular_score:{account_id}")
            if cached:
                scores[account_id] = float(cached)
            else:
                scores[account_id] = 0.5
        return scores
    
    def get_ring_scores(self, account_ids: List[str]) -> Dict[str, float]:
        scores = {}
        for account_id in account_ids:
            cached = self.redis_client.get(f"ring_score:{account_id}")
            if cached:
                data = json.loads(cached)
                scores[account_id] = data.get("ring_score", 0.0)
            else:
                scores[account_id] = 0.0
        return scores
    
    def run_graphsage_training(self, account_ids: List[str]) -> Dict[str, List[float]]:
        logger.info("Building graph for GraphSAGE training...")
        
        with self.driver.session() as session:
            G = self.graphsage_trainer.build_graph_from_neo4j(session, account_ids)
            
            labels_result = session.run(
                """
                MATCH (a:Account)
                WHERE a.id IN $account_ids
                OPTIONAL MATCH (a)-[:BELONGS_TO_RING]->(r:RingCandidate {status: 'CONFIRMED'})
                RETURN a.id AS id,
                       CASE WHEN r IS NOT NULL THEN 1 ELSE 0 END AS label
                """,
                account_ids=account_ids
            )
            labels = {record["id"]: record["label"] for record in labels_result}
        
        data, node_list = self.graphsage_trainer.prepare_data(G, labels)
        self.graphsage_trainer.train(data)
        
        embeddings = self.graphsage_trainer.generate_embeddings(data, node_list)
        
        self.graphsage_trainer.save_model(f"{self.model_dir}/graphsage.pt")
        
        return embeddings
    
    def run_classifier_training(self, embeddings: Dict[str, List[float]], labels: Dict[str, int]) -> Dict[str, float]:
        logger.info("Training ring classifier...")
        
        results = self.classifier.train(embeddings, labels)
        
        self.classifier.save(f"{self.model_dir}/ring_classifier.pt")
        
        return results
    
    def run_fusion_training(
        self,
        tabular_scores: Dict[str, float],
        ring_scores: Dict[str, float],
        labels: Dict[str, int]
    ) -> Dict[str, float]:
        logger.info("Training learned fusion model...")
        
        results = self.fusion.train(tabular_scores, ring_scores, labels)
        
        comparison = self.fusion.compare_with_override_rule(
            tabular_scores,
            ring_scores,
            labels
        )
        
        logger.info(f"Fusion comparison: {comparison}")
        
        self.fusion.save(f"{self.model_dir}/fusion_model.pt")
        
        return {**results, **comparison}
    
    def run_full_pipeline(self) -> Dict[str, float]:
        labels, account_ids = self.get_labeled_accounts(min_positive=10)
        
        if not account_ids:
            return {"status": "insufficient_labels", "positive_count": sum(labels.values())}
        
        logger.info(f"Training with {len(account_ids)} accounts")
        
        embeddings = self.run_graphsage_training(account_ids)
        
        classifier_results = self.run_classifier_training(embeddings, labels)
        
        tabular_scores = self.get_tabular_scores(account_ids)
        ring_scores = self.get_ring_scores(account_ids)
        
        fusion_results = self.run_fusion_training(tabular_scores, ring_scores, labels)
        
        self._cache_embeddings(embeddings)
        self._update_models_metadata(classifier_results, fusion_results)
        
        return {
            "status": "success",
            "classifier": classifier_results,
            "fusion": fusion_results,
            "total_accounts": len(account_ids),
            "positive_count": sum(labels.values()),
            "negative_count": len(account_ids) - sum(labels.values())
        }
    
    def _cache_embeddings(self, embeddings: Dict[str, List[float]]):
        for account_id, emb in list(embeddings.items())[:1000]:
            self.redis_client.setex(
                f"embedding:{account_id}",
                86400,
                json.dumps(emb)
            )
    
    def _update_models_metadata(self, classifier_results: Dict, fusion_results: Dict):
        metadata = {
            "last_trained": datetime.utcnow().isoformat(),
            "embedding_dim": self.embedding_dim,
            "classifier_auc": classifier_results.get("test_auc", 0.0),
            "classifier_auprc": classifier_results.get("test_auprc", 0.0),
            "fusion_auc": fusion_results.get("learned_auc", 0.0),
            "fusion_lift": fusion_results.get("lift_auc", 0.0),
            "model_version": int(time.time())
        }
        self.redis_client.set("models_metadata", json.dumps(metadata))
    
    def close(self):
        self.driver.close()

if __name__ == "__main__":
    import sys
    import os
    
    pipeline = TrainingPipeline(
        neo4j_uri=os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
        neo4j_user=os.environ.get("NEO4J_USER", "neo4j"),
        neo4j_password=os.environ.get("NEO4J_PASSWORD", "password"),
        redis_host=os.environ.get("REDIS_HOST", "localhost"),
        redis_port=int(os.environ.get("REDIS_PORT", 6379)),
        model_dir=os.environ.get("MODEL_DIR", "/app/models")
    )
    
    try:
        results = pipeline.run_full_pipeline()
        print(json.dumps(results, indent=2))
    finally:
        pipeline.close()