from typing import List, Dict, Any, Set, Tuple
from neo4j import Session
import networkx as nx
from networkx.algorithms.community import louvain_communities
from networkx.algorithms.components import connected_components

class CommunityDetector:
    def __init__(self, session: Session):
        self.session = session
    
    def build_graph(self, account_ids: List[str]) -> nx.Graph:
        G = nx.Graph()
        for account_id in account_ids:
            G.add_node(account_id, type="Account")
        
        result = self.session.run(
            """
            MATCH (a1:Account)-[:USED|TRANSACTED_WITH|OWNS]-(entity)-[:USED|TRANSACTED_WITH|OWNS]-(a2:Account)
            WHERE a1.id IN $account_ids AND a2.id IN $account_ids AND a1.id <> a2.id
            RETURN a1.id AS source, a2.id AS target, count(*) AS weight
            """,
            account_ids=account_ids
        )
        for record in result:
            G.add_edge(
                record["source"],
                record["target"],
                weight=record["weight"]
            )
        return G
    
    def detect_louvain_communities(self, G: nx.Graph) -> List[Set[str]]:
        if G.number_of_nodes() == 0:
            return []
        return list(louvain_communities(G, seed=42))
    
    def detect_connected_components(self, G: nx.Graph) -> List[Set[str]]:
        return list(connected_components(G))
    
    def compute_community_risk(self, community: Set[str], G: nx.Graph) -> float:
        if len(community) < 2:
            return 0.0
        
        result = self.session.run(
            """
            MATCH (a:Account)
            WHERE a.id IN $account_ids
            OPTIONAL MATCH (a)-[:USED|TRANSACTED_WITH|OWNS]-(entity)<-[:USED|TRANSACTED_WITH|OWNS]-(other:Account)
            WHERE other.id IN $account_ids AND other.id <> a.id
            WITH a, collect(DISTINCT other) AS neighbors
            WITH a, size(neighbors) AS degree, neighbors
            OPTIONAL MATCH (a)-[:USED|TRANSACTED_WITH|OWNS]-(e1)<-[:USED|TRANSACTED_WITH|OWNS]-(n1)
            OPTIONAL MATCH (n1)-[:USED|TRANSACTED_WITH|OWNS]-(e2)<-[:USED|TRANSACTED_WITH|OWNS]-(n2)
            WHERE n1.id IN $account_ids AND n2.id IN $account_ids AND n1.id <> n2.id AND n2.id IN neighbors
            WITH a, degree, neighbors, count(DISTINCT [n1.id, n2.id]) AS triangles
            RETURN avg(CASE WHEN degree > 1 THEN triangles / (degree * (degree - 1)) ELSE 0.0 END) AS avg_clustering,
                   max(degree) AS max_degree,
                   avg(degree) AS avg_degree
            """,
            account_ids=list(community)
        )
        record = result.single()
        if not record:
            return 0.0
        
        density = (2 * G.number_of_edges()) / (G.number_of_nodes() * (G.number_of_nodes() - 1)) if G.number_of_nodes() > 1 else 0.0
        avg_clustering = record["avg_clustering"] or 0.0
        max_degree = record["max_degree"] or 0
        avg_degree = record["avg_degree"] or 0.0
        
        risk = 0.4 * density + 0.3 * avg_clustering + 0.2 * min(max_degree / 10.0, 1.0) + 0.1 * min(avg_degree / 5.0, 1.0)
        return min(risk, 1.0)
    
    def find_candidate_rings(
        self,
        min_community_size: int = 3,
        risk_threshold: float = 0.5
    ) -> List[Dict[str, Any]]:
        result = self.session.run(
            """
            MATCH (a:Account)
            RETURN collect(a.id) AS all_accounts
            """
        )
        all_accounts = result.single()["all_accounts"]
        
        G = self.build_graph(all_accounts)
        communities = self.detect_connected_components(G)
        
        candidates = []
        for comm in communities:
            if len(comm) >= min_community_size:
                subgraph = G.subgraph(comm)
                risk = self.compute_community_risk(comm, G)
                if risk >= risk_threshold:
                    candidates.append({
                        "community_id": f"ring_{hash(frozenset(comm)) & 0xFFFFFFFF}",
                        "account_ids": list(comm),
                        "size": len(comm),
                        "risk_score": risk,
                        "edge_count": subgraph.number_of_edges(),
                        "density": (2 * subgraph.number_of_edges()) / (len(comm) * (len(comm) - 1)) if len(comm) > 1 else 0.0
                    })
        
        candidates.sort(key=lambda x: x["risk_score"], reverse=True)
        return candidates