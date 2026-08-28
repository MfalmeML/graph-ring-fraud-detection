import os
import time
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel
from neo4j import GraphDatabase
from redis import Redis
from prometheus_client import generate_latest
from src.serving.ring_score import RingScoreCalculator
from src.serving.feature_cache import FeatureCache
from src.embeddings.inference import EmbeddingInference
from src.serving.circuit_breaker import CircuitBreaker, CircuitBreakerDecorator, CircuitState
from src.serving.metrics import metrics, timing_decorator
from src.config.decision_policy import DecisionPolicy
from src.fusion.learned_fusion import LearnedFusion

app = FastAPI()

class RingScoreResponse(BaseModel):
    account_id: str
    ring_score: float
    cached: bool
    embedding: Optional[List[float]] = None
    membership_prob: Optional[float] = None
    combined_score: Optional[float] = None
    confirmed_members: Optional[int] = None

class ApiConfig:
    def __init__(self):
        self.neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.neo4j_user = os.getenv("NEO4J_USER", "neo4j")
        self.neo4j_password = os.getenv("NEO4J_PASSWORD", "password")
        self.redis_host = os.getenv("REDIS_HOST", "localhost")
        self.redis_port = int(os.getenv("REDIS_PORT", "6379"))

config = ApiConfig()
neo4j_driver = GraphDatabase.driver(
    config.neo4j_uri,
    auth=(config.neo4j_user, config.neo4j_password)
)
redis_client = Redis(
    host=config.redis_host,
    port=config.redis_port,
    decode_responses=True
)

feature_cache = FeatureCache(
    redis_host=config.redis_host,
    redis_port=config.redis_port
)

embedding_session = neo4j_driver.session()
embedding_inference = EmbeddingInference(
    neo4j_session=embedding_session,
    redis_client=redis_client,
    model_dir="/app/models"
)

neo4j_circuit = CircuitBreaker(
    name="neo4j",
    redis_client=redis_client,
    failure_threshold=3,
    timeout_seconds=30
)

decision_policy = DecisionPolicy()
fusion_model = None
fusion_available = False

try:
    fusion_model = LearnedFusion(
        redis_host=config.redis_host,
        redis_port=config.redis_port
    )
    fusion_model.load("/app/models/fusion_model.pt")
    fusion_available = True
except Exception:
    fusion_available = False


@CircuitBreakerDecorator(neo4j_circuit, fallback=lambda account_id: {"ring_score": 0.0, "combined_score": 0.5})
def get_ring_score_safe(account_id: str):
    with neo4j_driver.session() as session:
        calculator = RingScoreCalculator(
            feature_cache=feature_cache,
            neo4j_session=session
        )
        ring_score = calculator.compute_ring_score(account_id)
        
        result = session.run(
            """
            MATCH (a:Account {id: $account_id})-[:BELONGS_TO_RING]->(r:RingCandidate {status: 'CONFIRMED'})
            MATCH (r)<-[:BELONGS_TO_RING]-(members:Account)
            RETURN count(DISTINCT members) AS count
            """,
            account_id=account_id
        )
        record = result.single()
        confirmed_members = record["count"] if record else 0
        
        tabular_prob = 0.5
        
        if fusion_available:
            combined = fusion_model.predict(tabular_prob, ring_score)
        else:
            decision, combined = decision_policy.decide(
                account_id=account_id,
                ring_score=ring_score,
                confirmed_ring_members=confirmed_members,
                tabular_fraud_probability=tabular_prob
            )
            if isinstance(combined, tuple):
                combined = combined[1]
        
        return {"ring_score": ring_score, "combined_score": combined, "confirmed_members": confirmed_members}

@app.get("/ring-score/{account_id}", response_model=RingScoreResponse)
def get_ring_score(account_id: str):
    start_time = time.time()
    status = "success"
    
    try:
        cache_key = f"ring_score:{account_id}"
        cached = redis_client.get(cache_key)
        
        if cached:
            cached_data = get_ring_score_safe(account_id)
            metrics.record_request("cached")
            metrics.record_ring_score(float(cached))
            return RingScoreResponse(
                account_id=account_id,
                ring_score=float(cached),
                cached=True,
                combined_score=cached_data.get("combined_score"),
                confirmed_members=cached_data.get("confirmed_members")
            )
        
        result = get_ring_score_safe(account_id)
        redis_client.setex(cache_key, 60, str(result["ring_score"]))
        
        embedding = None
        membership_prob = None
        try:
            embedding = embedding_inference.get_embedding(account_id)
            membership_prob = (
                embedding_inference.predict_ring_membership(account_id)
                if embedding
                else None
            )
        except Exception:
            pass
        
        metrics.record_request("success")
        metrics.record_ring_score(result["ring_score"])
        
        return RingScoreResponse(
            account_id=account_id,
            ring_score=result["ring_score"],
            cached=False,
            embedding=embedding,
            membership_prob=membership_prob,
            combined_score=result["combined_score"],
            confirmed_members=result["confirmed_members"]
        )
    except Exception as e:
        status = "error"
        metrics.record_request("error")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        duration = time.time() - start_time
        metrics.record_latency(duration)
        metrics.update_circuit_state("neo4j", neo4j_circuit.get_status().get("state", 0))


@app.get("/circuit/status")
def get_circuit_status():
    return {"neo4j": neo4j_circuit.get_status()}


@app.post("/circuit/reset")
def reset_circuit():
    neo4j_circuit._set_state(CircuitState.CLOSED)
    neo4j_circuit._reset_failures()
    return {"status": "reset"}


@app.get("/metrics")
def get_metrics():
    return Response(content=generate_latest(), media_type="text/plain")

@app.on_event("shutdown")
def shutdown():
    embedding_session.close()
    neo4j_driver.close()