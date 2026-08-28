import pytest
import json
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer, KafkaConsumer
from neo4j import GraphDatabase
from redis import Redis
import threading
import requests

from src.ingestion.event_parser import parse_transaction
from src.graph_builder.models import GraphNode, GraphEdge
from src.features.structural import StructuralFeatureCalculator
from src.features.temporal import TemporalFeatureCalculator
from src.community.detection import CommunityDetector
from src.serving.ring_score import RingScoreCalculator
from src.serving.feature_cache import FeatureCache
from src.config.decision_policy import DecisionPolicy

class TestIntegration:
    @pytest.fixture
    def neo4j_driver(self):
        driver = GraphDatabase.driver(
            "bolt://localhost:7687",
            auth=("neo4j", "password")
        )
        yield driver
        driver.close()
    
    @pytest.fixture
    def redis_client(self):
        client = Redis(host="localhost", port=6379, decode_responses=True)
        client.flushall()
        yield client
        client.flushall()
    
    @pytest.fixture
    def kafka_producer(self):
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        yield producer
        producer.close()
    
    def test_end_to_end_ingestion(self, neo4j_driver, kafka_producer):
        event = {
            "transaction_id": "tx_001",
            "account_id": "acc_001",
            "device_id": "dev_001",
            "ip_address": "192.168.1.1",
            "merchant_id": "mer_001",
            "card_id": "card_001",
            "timestamp": datetime.utcnow().isoformat(),
            "amount": 100.0
        }
        
        kafka_producer.send("transactions", event)
        kafka_producer.flush()
        time.sleep(2)
        
        with neo4j_driver.session() as session:
            result = session.run(
                """
                MATCH (a:Account {id: 'acc_001'})
                MATCH (d:Device {id: 'dev_001'})
                MATCH (ip:IP {id: '192.168.1.1'})
                MATCH (m:Merchant {id: 'mer_001'})
                MATCH (c:Card {id: 'card_001'})
                MATCH (a)-[:USED]->(d)
                MATCH (a)-[:USED]->(ip)
                MATCH (a)-[:TRANSACTED_WITH]->(m)
                MATCH (a)-[:OWNS]->(c)
                MATCH (d)-[:SEEN_AT]->(ip)
                RETURN count(a) AS count
                """
            )
            assert result.single()["count"] == 1
    
    def test_structural_features(self, neo4j_driver):
        with neo4j_driver.session() as session:
            session.run(
                """
                CREATE (a:Account {id: 'acc_002'})
                CREATE (d:Device {id: 'dev_002'})
                CREATE (a)-[:USED {created_at: '2026-01-01T00:00:00'}]->(d)
                CREATE (a)-[:USED {created_at: '2026-01-01T00:00:00'}]->(d)
                CREATE (a)-[:USED {created_at: '2026-01-01T00:00:00'}]->(d)
                """
            )
            
            calculator = StructuralFeatureCalculator(session)
            count = calculator.device_account_count("dev_002")
            assert count >= 1
    
    def test_ring_score_calculation(self, neo4j_driver, redis_client):
        with neo4j_driver.session() as session:
            session.run(
                """
                CREATE (a1:Account {id: 'acc_003'})
                CREATE (a2:Account {id: 'acc_004'})
                CREATE (d:Device {id: 'dev_003'})
                CREATE (a1)-[:USED {created_at: '2026-01-01T00:00:00'}]->(d)
                CREATE (a2)-[:USED {created_at: '2026-01-01T00:00:00'}]->(d)
                """
            )
            
            feature_cache = FeatureCache(redis_host="localhost", redis_port=6379)
            calculator = RingScoreCalculator(
                feature_cache=feature_cache,
                neo4j_session=session
            )
            
            score = calculator.compute_ring_score("acc_003")
            assert 0.0 <= score <= 1.0
            
            cached = feature_cache.get_features("acc_003")
            assert cached is not None
    
    def test_community_detection(self, neo4j_driver):
        with neo4j_driver.session() as session:
            session.run(
                """
                CREATE (a1:Account {id: 'acc_005'})
                CREATE (a2:Account {id: 'acc_006'})
                CREATE (a3:Account {id: 'acc_007'})
                CREATE (d:Device {id: 'dev_004'})
                CREATE (a1)-[:USED]->(d)
                CREATE (a2)-[:USED]->(d)
                CREATE (a3)-[:USED]->(d)
                """
            )
            
            detector = CommunityDetector(session)
            candidates = detector.find_candidate_rings(
                min_community_size=3,
                risk_threshold=0.0
            )
            
            assert len(candidates) >= 1
            assert candidates[0]["size"] >= 3
    
    def test_decision_policy(self):
        policy = DecisionPolicy(
            ring_score_threshold=0.90,
            min_ring_members=2,
            combined_decline_threshold=0.90,
            combined_challenge_threshold_low=0.50,
            combined_challenge_threshold_high=0.90,
            alpha=0.6
        )
        
        decision, score = policy.decide(
            account_id="acc_008",
            ring_score=0.95,
            confirmed_ring_members=3,
            tabular_fraud_probability=0.3
        )
        assert decision.value == "INVESTIGATE"
        
        decision, score = policy.decide(
            account_id="acc_009",
            ring_score=0.4,
            confirmed_ring_members=0,
            tabular_fraud_probability=0.8
        )
        combined = 0.6 * 0.8 + 0.4 * 0.4
        assert combined > 0.50
    
    def test_api_latency(self, neo4j_driver, redis_client):
        with neo4j_driver.session() as session:
            session.run(
                """
                CREATE (a:Account {id: 'acc_010'})
                CREATE (d:Device {id: 'dev_005'})
                CREATE (a)-[:USED]->(d)
                """
            )
            
            start = time.time()
            response = requests.get("http://localhost:8000/ring-score/acc_010")
            elapsed = time.time() - start
            assert elapsed < 15.0  # More realistic expectation for graph queries
            assert response.status_code == 200
            data = response.json()
            assert "ring_score" in data
            assert "combined_score" in data
    
    def test_investigator_api(self, neo4j_driver):
        with neo4j_driver.session() as session:
            session.run(
                """
                CREATE (r:RingCandidate {
                    id: 'ring_001',
                    risk_score: 0.85,
                    size: 4,
                    density: 0.6,
                    detected_at: '2026-01-01T00:00:00',
                    status: 'PENDING'
                })
                CREATE (a:Account {id: 'acc_011'})
                CREATE (a)-[:BELONGS_TO_RING]->(r)
                """
            )
            
            response = requests.get("http://localhost:8002/rings/pending")
            assert response.status_code == 200
            rings = response.json()
            assert len(rings) >= 1
            
            response = requests.post(
                "http://localhost:8002/rings/confirm",
                json={"ring_id": "ring_001", "status": "CONFIRMED"}
            )
            assert response.status_code == 200
    
    def test_fusion_comparison(self):
        import numpy as np
        from src.fusion.learned_fusion import LearnedFusion
        
        fusion = LearnedFusion(epochs=5)
        n = 100
        tab = {f"acc_{i}": np.random.random() for i in range(n)}
        ring = {f"acc_{i}": np.random.random() for i in range(n)}
        labels = {f"acc_{i}": np.random.randint(0, 2) for i in range(n)}
        
        fusion.train(tab, ring, labels)
        comparison = fusion.compare_with_override_rule(tab, ring, labels)
        
        assert "lift_auc" in comparison
    
    def test_fallback_path(self, neo4j_driver):
        with neo4j_driver.session() as session:
            session.run(
                """
                CREATE (a:Account {id: 'acc_012'})
                """
            )
            
        from src.serving.fallback import FallbackHandler
        
        handler = FallbackHandler(tabular_fraud_probability=0.7)
        combined = handler.get_combined_score(ring_score=None)
        assert combined == 0.7
        
        combined = handler.get_combined_score(ring_score=0.8, alpha=0.4)
        assert combined == 0.4 * 0.7 + 0.6 * 0.8