import json
import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional
from kafka import KafkaConsumer, KafkaProducer
from neo4j import GraphDatabase
from redis import Redis
import os

from src.serving.ring_score import RingScoreCalculator
from src.serving.feature_cache import FeatureCache
from src.fusion.learned_fusion import LearnedFusion
from src.config.decision_policy import DecisionPolicy

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ShadowModeRunner:
    def __init__(
        self,
        neo4j_uri: str,
        neo4j_user: str,
        neo4j_password: str,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        kafka_bootstrap: str = "localhost:9092",
        input_topic: str = "transactions",
        output_topic: str = "shadow_decisions",
        model_path: str = "/app/models/fusion_model.pt"
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
        self.consumer = KafkaConsumer(
            input_topic,
            bootstrap_servers=kafka_bootstrap,
            group_id="shadow-mode",
            value_deserializer=lambda m: json.loads(m.decode("utf-8"))
        )
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        self.feature_cache = FeatureCache(
            redis_host=redis_host,
            redis_port=redis_port
        )
        self.policy = DecisionPolicy()
        self.fusion = LearnedFusion(
            redis_host=redis_host,
            redis_port=redis_port
        )
        try:
            self.fusion.load(model_path)
            self.fusion_available = True
        except FileNotFoundError:
            self.fusion_available = False
            logger.warning("Fusion model not found. Using manual override rule.")
    
    def run(self, sample_rate: float = 1.0):
        logger.info(f"Shadow mode started. Sample rate: {sample_rate}")
        
        for message in self.consumer:
            if sample_rate < 1.0 and random.random() > sample_rate:
                continue
            
            event = message.value
            account_id = event.get("account_id")
            tabular_prob = event.get("tabular_fraud_probability", 0.5)
            
            if not account_id:
                continue
            
            try:
                result = self._evaluate_account(account_id, tabular_prob)
                self._log_result(event, result)
            except Exception as e:
                logger.error(f"Shadow evaluation failed for {account_id}: {e}")
    
    def _evaluate_account(
        self,
        account_id: str,
        tabular_prob: float
    ) -> Dict[str, Any]:
        with self.driver.session() as session:
            calculator = RingScoreCalculator(
                feature_cache=self.feature_cache,
                neo4j_session=session
            )
            ring_score = calculator.compute_ring_score(account_id)
            
            confirmed_members = self._get_confirmed_members(session, account_id)
            
            if self.fusion_available:
                combined_score = self.fusion.predict(tabular_prob, ring_score)
                decision = self._decision_from_score(combined_score)
            else:
                decision, combined_score = self.policy.decide(
                    account_id=account_id,
                    ring_score=ring_score,
                    confirmed_ring_members=confirmed_members,
                    tabular_fraud_probability=tabular_prob
                )
                if isinstance(combined_score, tuple):
                    combined_score = combined_score[1]
            
            return {
                "account_id": account_id,
                "ring_score": ring_score,
                "tabular_prob": tabular_prob,
                "combined_score": combined_score,
                "decision": decision.value if hasattr(decision, 'value') else str(decision),
                "confirmed_members": confirmed_members,
                "fusion_used": self.fusion_available,
                "timestamp": datetime.utcnow().isoformat()
            }
    
    def _get_confirmed_members(self, session, account_id: str) -> int:
        result = session.run(
            """
            MATCH (a:Account {id: $account_id})-[:BELONGS_TO_RING]->(r:RingCandidate {status: 'CONFIRMED'})
            MATCH (r)<-[:BELONGS_TO_RING]-(members:Account)
            RETURN count(DISTINCT members) AS count
            """,
            account_id=account_id
        )
        record = result.single()
        return record["count"] if record else 0
    
    def _decision_from_score(self, score: float) -> str:
        if score > 0.90:
            return "DECLINE"
        elif 0.50 < score <= 0.90:
            return "CHALLENGE"
        else:
            return "APPROVE"
    
    def _log_result(self, event: Dict[str, Any], result: Dict[str, Any]):
        log_entry = {
            "transaction_id": event.get("transaction_id"),
            "account_id": result["account_id"],
            "shadow_decision": result["decision"],
            "production_decision": event.get("actual_decision"),
            "ring_score": result["ring_score"],
            "combined_score": result["combined_score"],
            "tabular_prob": result["tabular_prob"],
            "confirmed_members": result["confirmed_members"],
            "fusion_used": result["fusion_used"],
            "evaluated_at": result["timestamp"],
            "shadow_mode": True
        }
        self.producer.send("shadow_decisions", log_entry)
        self.producer.flush()
    
    def get_statistics(self, limit: int = 1000) -> Dict[str, Any]:
        consumer = KafkaConsumer(
            "shadow_decisions",
            bootstrap_servers=self.consumer.config['bootstrap_servers'],
            group_id="shadow-stats",
            value_deserializer=lambda m: json.loads(m.decode("utf-8"))
        )
        
        decisions = []
        for _ in range(limit):
            msg = next(consumer, None)
            if not msg:
                break
            decisions.append(msg.value)
        
        consumer.close()
        
        if not decisions:
            return {"total": 0, "message": "No shadow decisions found"}
        
        decisions_by_type = {}
        for d in decisions:
            shadow = d.get("shadow_decision", "UNKNOWN")
            decisions_by_type[shadow] = decisions_by_type.get(shadow, 0) + 1
        
        production_mismatch = sum(
            1 for d in decisions
            if d.get("shadow_decision") != d.get("production_decision")
        )
        
        return {
            "total": len(decisions),
            "decision_distribution": decisions_by_type,
            "production_mismatch_count": production_mismatch,
            "production_mismatch_rate": production_mismatch / len(decisions) if decisions else 0,
            "last_evaluated": decisions[-1].get("evaluated_at") if decisions else None
        }
    
    def close(self):
        self.driver.close()
        self.consumer.close()
        self.producer.close()

if __name__ == "__main__":
    import sys
    import random
    
    runner = ShadowModeRunner(
        neo4j_uri=os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
        neo4j_user=os.environ.get("NEO4J_USER", "neo4j"),
        neo4j_password=os.environ.get("NEO4J_PASSWORD", "password"),
        redis_host=os.environ.get("REDIS_HOST", "localhost"),
        redis_port=int(os.environ.get("REDIS_PORT", 6379)),
        kafka_bootstrap=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        input_topic=os.environ.get("SHADOW_INPUT_TOPIC", "transactions"),
        output_topic=os.environ.get("SHADOW_OUTPUT_TOPIC", "shadow_decisions"),
        model_path=os.environ.get("FUSION_MODEL_PATH", "/app/models/fusion_model.pt")
    )
    
    sample_rate = float(os.environ.get("SHADOW_SAMPLE_RATE", "0.1"))
    
    try:
        runner.run(sample_rate=sample_rate)
    except KeyboardInterrupt:
        logger.info("Shadow mode interrupted")
    finally:
        runner.close()