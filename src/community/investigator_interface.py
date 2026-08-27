from typing import List, Dict, Any, Optional
from neo4j import Session
from src.community.storage import CommunityStorage

class InvestigatorInterface:
    def __init__(self, session: Session):
        self.session = session
        self.storage = CommunityStorage(session)
    
    def get_pending_rings(self, limit: int = 50) -> List[Dict[str, Any]]:
        result = self.session.run(
            """
            MATCH (r:RingCandidate {status: 'PENDING'})
            OPTIONAL MATCH (r)<-[:BELONGS_TO_RING]-(members:Account)
            WITH r, collect(members.id) AS member_ids
            ORDER BY r.risk_score DESC
            LIMIT $limit
            RETURN r.id AS ring_id,
                   r.risk_score AS risk_score,
                   r.size AS size,
                   r.density AS density,
                   r.detected_at AS detected_at,
                   member_ids
            """,
            limit=limit
        )
        return [dict(record) for record in result]
    
    def confirm_ring(self, ring_id: str, investigator_id: str = "investigator") -> bool:
        try:
            self.storage.update_ring_status(ring_id, "CONFIRMED", investigator_id)
            return True
        except Exception:
            return False
    
    def reject_ring(self, ring_id: str, investigator_id: str = "investigator") -> bool:
        try:
            self.storage.update_ring_status(ring_id, "REJECTED", investigator_id)
            return True
        except Exception:
            return False
    
    def get_ring_details(self, ring_id: str) -> Optional[Dict[str, Any]]:
        result = self.session.run(
            """
            MATCH (r:RingCandidate {id: $ring_id})
            OPTIONAL MATCH (r)<-[:BELONGS_TO_RING]-(members:Account)
            OPTIONAL MATCH (members)-[rel:USED|TRANSACTED_WITH|OWNS]-(entity)
            WITH r, collect(DISTINCT members.id) AS member_ids, collect(DISTINCT entity.id) AS shared_entities
            RETURN r.id AS ring_id,
                   r.risk_score AS risk_score,
                   r.size AS size,
                   r.density AS density,
                   r.detected_at AS detected_at,
                   r.status AS status,
                   member_ids,
                   shared_entities
            """,
            ring_id=ring_id
        )
        record = result.single()
        return dict(record) if record else None