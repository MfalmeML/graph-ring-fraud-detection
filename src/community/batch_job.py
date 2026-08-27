from typing import List, Dict, Any
from neo4j import GraphDatabase
from redis import Redis
import json
from datetime import datetime
import logging
from src.community.detection import CommunityDetector
from src.community.storage import CommunityStorage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CommunityBatchJob:
    def __init__(
        self,
        neo4j_uri: str,
        neo4j_user: str,
        neo4j_password: str,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        min_community_size: int = 3,
        risk_threshold: float = 0.5
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
        self.min_size = min_community_size
        self.risk_threshold = risk_threshold
        self.job_id = f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    
    def run(self) -> Dict[str, Any]:
        logger.info(f"Starting batch job {self.job_id}")
        start_time = datetime.utcnow()
        
        with self.driver.session() as session:
            detector = CommunityDetector(session)
            storage = CommunityStorage(session)
            
            candidates = detector.find_candidate_rings(
                min_community_size=self.min_size,
                risk_threshold=self.risk_threshold
            )
            
            logger.info(f"Found {len(candidates)} candidate rings")
            
            stored_rings = []
            for ring in candidates:
                ring_id = storage.store_candidate_ring(ring)
                stored_rings.append({
                    "ring_id": ring_id,
                    "size": ring["size"],
                    "risk_score": ring["risk_score"]
                })
            
            self._update_ring_cache(candidates)
            label_counts = storage.get_label_counts()
            logger.info("Label totals after batch %s: %s", self.job_id, label_counts)
        
        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()
        
        result = {
            "job_id": self.job_id,
            "started_at": start_time.isoformat(),
            "completed_at": end_time.isoformat(),
            "duration_seconds": duration,
            "candidate_count": len(candidates),
            "label_counts": label_counts,
            "stored_rings": stored_rings
        }
        
        self.redis_client.setex(
            f"batch_job:{self.job_id}",
            86400,
            json.dumps(result)
        )
        self.redis_client.set("last_batch_result", json.dumps(result))
        
        logger.info(f"Completed batch job {self.job_id} in {duration:.2f}s")
        return result
    
    def _update_ring_cache(self, candidates: List[Dict[str, Any]]):
        for ring in candidates:
            for account_id in ring["account_ids"]:
                cache_key = f"ring_membership:{account_id}"
                current = self.redis_client.get(cache_key)
                if current:
                    data = json.loads(current)
                else:
                    data = {"ring_ids": []}
                
                ring_id = ring["community_id"]
                if ring_id not in data["ring_ids"]:
                    data["ring_ids"].append(ring_id)
                    data["highest_risk"] = max(
                        data.get("highest_risk", 0.0),
                        ring["risk_score"]
                    )
                    self.redis_client.setex(
                        cache_key,
                        3600,
                        json.dumps(data)
                    )
    
    def close(self):
        self.driver.close()

if __name__ == "__main__":
    import sys
    job = CommunityBatchJob(
        neo4j_uri=sys.argv[1] if len(sys.argv) > 1 else "bolt://localhost:7687",
        neo4j_user=sys.argv[2] if len(sys.argv) > 2 else "neo4j",
        neo4j_password=sys.argv[3] if len(sys.argv) > 3 else "password"
    )
    try:
        job.run()
    finally:
        job.close()