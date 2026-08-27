from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from neo4j import GraphDatabase
from redis import Redis
from src.serving.ring_score import RingScoreCalculator
from src.serving.feature_cache import FeatureCache

app = FastAPI()

class RingScoreResponse(BaseModel):
    account_id: str
    ring_score: float
    cached: bool

class ApiConfig:
    def __init__(self):
        self.neo4j_uri = "bolt://localhost:7687"
        self.neo4j_user = "neo4j"
        self.neo4j_password = "password"
        self.redis_host = "localhost"
        self.redis_port = 6379

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

@app.get("/ring-score/{account_id}", response_model=RingScoreResponse)
def get_ring_score(account_id: str):
    cache_key = f"ring_score:{account_id}"
    cached = redis_client.get(cache_key)
    
    if cached:
        return RingScoreResponse(
            account_id=account_id,
            ring_score=float(cached),
            cached=True
        )
    
    try:
        with neo4j_driver.session() as session:
            calculator = RingScoreCalculator(
                feature_cache=feature_cache,
                neo4j_session=session
            )
            score = calculator.compute_ring_score(account_id)
            redis_client.setex(cache_key, 60, str(score))
            return RingScoreResponse(
                account_id=account_id,
                ring_score=score,
                cached=False
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.on_event("shutdown")
def shutdown():
    neo4j_driver.close()