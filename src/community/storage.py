from typing import List, Dict, Any
from neo4j import Session
from datetime import datetime

class CommunityStorage:
    def __init__(self, session: Session):
        self.session = session
    
    def store_candidate_ring(self, ring: Dict[str, Any]) -> str:
        ring_id = ring["community_id"]
        result = self.session.run(
            """
            MERGE (r:RingCandidate {id: $ring_id})
            SET r.risk_score = $risk_score,
                r.size = $size,
                r.edge_count = $edge_count,
                r.density = $density,
                r.detected_at = $detected_at,
                r.status = 'PENDING'
            RETURN r.id AS id
            """,
            ring_id=ring_id,
            risk_score=ring["risk_score"],
            size=ring["size"],
            edge_count=ring["edge_count"],
            density=ring["density"],
            detected_at=datetime.utcnow().isoformat()
        )
        
        for account_id in ring["account_ids"]:
            self.session.run(
                """
                MATCH (a:Account {id: $account_id})
                MATCH (r:RingCandidate {id: $ring_id})
                MERGE (a)-[:BELONGS_TO_RING]->(r)
                """,
                account_id=account_id,
                ring_id=ring_id
            )
        
        return result.single()["id"]
    
    def update_ring_status(self, ring_id: str, status: str, confirmed_by: str = "investigator"):
        self.session.run(
            """
            MATCH (r:RingCandidate {id: $ring_id})
            SET r.status = $status,
                r.confirmed_by = $confirmed_by,
                r.confirmed_at = $confirmed_at
            """,
            ring_id=ring_id,
            status=status,
            confirmed_by=confirmed_by,
            confirmed_at=datetime.utcnow().isoformat()
        )
    
    def get_confirmed_ring_members(self, account_id: str) -> int:
        result = self.session.run(
            """
            MATCH (a:Account {id: $account_id})-[:BELONGS_TO_RING]->(r:RingCandidate {status: 'CONFIRMED'})
            MATCH (r)<-[:BELONGS_TO_RING]-(members:Account)
            RETURN count(DISTINCT members) AS member_count
            """,
            account_id=account_id
        )
        return result.single()["member_count"]