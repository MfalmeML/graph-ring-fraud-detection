from neo4j import Session
from datetime import datetime, timedelta

class TemporalFeatureCalculator:
    def __init__(self, session: Session):
        self.session = session
    
    def new_edges_last_hour(self, account_id: str) -> int:
        cutoff = datetime.utcnow() - timedelta(hours=1)
        result = self.session.run(
            """
            MATCH (a:Account {id: $account_id})-[r:USED|TRANSACTED_WITH|OWNS]-()
            WHERE r.created_at > $cutoff
            RETURN count(r) AS count
            """,
            account_id=account_id,
            cutoff=cutoff.isoformat()
        )
        return result.single()["count"]
    
    def new_edges_last_day(self, account_id: str) -> int:
        cutoff = datetime.utcnow() - timedelta(days=1)
        result = self.session.run(
            """
            MATCH (a:Account {id: $account_id})-[r:USED|TRANSACTED_WITH|OWNS]-()
            WHERE r.created_at > $cutoff
            RETURN count(r) AS count
            """,
            account_id=account_id,
            cutoff=cutoff.isoformat()
        )
        return result.single()["count"]
    
    def edge_formation_burstiness(self, account_id: str, window_minutes: int = 10) -> float:
        result = self.session.run(
            """
            MATCH (a:Account {id: $account_id})-[r:USED|TRANSACTED_WITH|OWNS]-()
            WITH r.created_at AS times
            ORDER BY times
            WITH collect(times) AS timestamps
            WITH timestamps, size(timestamps) AS n
            WHERE n > 1
            WITH timestamps[0] AS first_time, timestamps[n-1] AS last_time, n
            WITH (last_time - first_time).seconds / 60.0 AS span, n
            WHERE span > 0
            RETURN (n / span) / ( (n * (n - 1)) / (2 * span * span) ) AS burstiness
            """,
            account_id=account_id,
            window_minutes=window_minutes
        )
        record = result.single()
        return record["burstiness"] if record else 0.0