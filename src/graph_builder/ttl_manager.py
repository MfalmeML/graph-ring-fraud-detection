from neo4j import Session
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TTLManager:
    def __init__(
        self,
        session: Session,
        edge_ttl_days: int = 90,
        node_ttl_days: int = 365,
        orphan_ttl_days: int = 30
    ):
        self.session = session
        self.edge_ttl = edge_ttl_days
        self.node_ttl = node_ttl_days
        self.orphan_ttl = orphan_ttl_days
    
    def expire_stale_edges(self) -> int:
        cutoff = datetime.utcnow() - timedelta(days=self.edge_ttl)
        result = self.session.run(
            """
            MATCH (a)-[r:USED|SEEN_AT|TRANSACTED_WITH|OWNS]->()
            WHERE r.created_at < $cutoff
            DELETE r
            RETURN count(r) AS deleted
            """,
            cutoff=cutoff.isoformat()
        )
        count = result.single()["deleted"]
        logger.info(f"Deleted {count} stale edges older than {self.edge_ttl} days")
        return count
    
    def expire_orphan_nodes(self) -> int:
        cutoff = datetime.utcnow() - timedelta(days=self.orphan_ttl)
        result = self.session.run(
            """
            MATCH (n:Account|Device|IP|Merchant|Card)
            WHERE NOT (n)--()
              AND n.created_at < $cutoff
            DELETE n
            RETURN count(n) AS deleted
            """,
            cutoff=cutoff.isoformat()
        )
        count = result.single()["deleted"]
        logger.info(f"Deleted {count} orphan nodes older than {self.orphan_ttl} days")
        return count
    
    def expire_low_signal_nodes(self) -> int:
        result = self.session.run(
            """
            MATCH (a:Account)
            WHERE a.updated_at < datetime() - duration({days: 60})
              AND NOT EXISTS((a)-[:TRANSACTED_WITH]-())
            DETACH DELETE a
            RETURN count(a) AS deleted
            """
        )
        count = result.single()["deleted"]
        logger.info(f"Deleted {count} low-signal accounts")
        return count
    
    def run_cleanup(self) -> dict:
        edges_deleted = self.expire_stale_edges()
        orphans_deleted = self.expire_orphan_nodes()
        low_signal_deleted = self.expire_low_signal_nodes()
        
        return {
            "edges_deleted": edges_deleted,
            "orphans_deleted": orphans_deleted,
            "low_signal_deleted": low_signal_deleted,
            "total_deleted": edges_deleted + orphans_deleted + low_signal_deleted
        }