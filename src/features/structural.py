from typing import List, Dict, Any
from neo4j import Session

class StructuralFeatureCalculator:
    def __init__(self, session: Session):
        self.session = session
    
    def device_account_count(self, device_id: str) -> int:
        result = self.session.run(
            """
            MATCH (d:Device {id: $device_id})<-[:USED]-(a:Account)
            RETURN count(a) AS count
            """,
            device_id=device_id
        )
        return result.single()["count"]
    
    def ip_account_count(self, ip_address: str) -> int:
        result = self.session.run(
            """
            MATCH (ip:IP {id: $ip_address})<-[:USED]-(a:Account)
            RETURN count(a) AS count
            """,
            ip_address=ip_address
        )
        return result.single()["count"]
    
    def merchant_account_diversity(self, merchant_id: str, days: int = 7) -> int:
        result = self.session.run(
            """
            MATCH (m:Merchant {id: $merchant_id})<-[:TRANSACTED_WITH]-(a:Account)
            WHERE a.created_at > datetime() - duration({days: $days})
            RETURN count(DISTINCT a) AS count
            """,
            merchant_id=merchant_id,
            days=days
        )
        return result.single()["count"]
    
    def triangle_count(self, account_id: str) -> int:
        result = self.session.run(
            """
            MATCH (a:Account {id: $account_id})-[r1:USED|TRANSACTED_WITH|OWNS]-(x)
            MATCH (x)-[r2:USED|TRANSACTED_WITH|OWNS]-(y)
            MATCH (y)-[r3:USED|TRANSACTED_WITH|OWNS]-(a)
            RETURN count(DISTINCT [x.id, y.id]) / 2 AS triangles
            """,
            account_id=account_id
        )
        return result.single()["triangles"]
    
    def clustering_coefficient(self, account_id: str) -> float:
        result = self.session.run(
            """
            MATCH (a:Account {id: $account_id})
            OPTIONAL MATCH (a)-[:USED|TRANSACTED_WITH|OWNS]-(neighbor)
            WITH a, collect(DISTINCT neighbor) AS neighbors
            WITH a, neighbors, size(neighbors) AS degree
            OPTIONAL MATCH (a)-[:USED|TRANSACTED_WITH|OWNS]-(n1)
            WHERE n1 IN neighbors
            OPTIONAL MATCH (n1)-[:USED|TRANSACTED_WITH|OWNS]-(n2)
            WHERE n2 IN neighbors AND n1 <> n2
                 WITH degree, count(DISTINCT [n1.id, n2.id]) AS pair_count
            RETURN CASE WHEN degree > 1 
                     THEN pair_count / (degree * (degree - 1))
                   ELSE 0.0 END AS coefficient
            """,
            account_id=account_id
        )
        return result.single()["coefficient"]
    
    def connected_component_size(self, account_id: str) -> int:
        result = self.session.run(
            """
            MATCH (a:Account {id: $account_id})
            MATCH (a)-[:USED|TRANSACTED_WITH|OWNS*0..10]-(connected:Account)
            RETURN count(DISTINCT connected) AS componentSize
            """,
            account_id=account_id
        )
        record = result.single()
        return record["componentSize"] if record else 0
    
    def shared_entity_count(self, account_id: str) -> int:
        result = self.session.run(
            """
            MATCH (a:Account {id: $account_id})-[r:USED|TRANSACTED_WITH|OWNS]-(entity)
            RETURN count(DISTINCT entity) AS count
            """,
            account_id=account_id
        )
        return result.single()["count"]