import torch
import torch.nn.functional as F
from torch_geometric.nn import SAGEConv, global_mean_pool
from torch_geometric.data import Data, DataLoader
from torch_geometric.utils import from_networkx
import networkx as nx
import numpy as np
from typing import List, Dict, Any, Tuple, Optional
from neo4j import Session
from redis import Redis
import json
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GraphSAGETrainer:
    def __init__(
        self,
        embedding_dim: int = 64,
        num_layers: int = 2,
        learning_rate: float = 0.01,
        epochs: int = 100
    ):
        self.embedding_dim = embedding_dim
        self.num_layers = num_layers
        self.lr = learning_rate
        self.epochs = epochs
        self.model = None
        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    
    def build_graph_from_neo4j(self, session: Session, account_ids: List[str]) -> nx.Graph:
        G = nx.Graph()
        
        for account_id in account_ids:
            G.add_node(account_id, label='account')
        
        result = session.run(
            """
            MATCH (a1:Account)-[r:USED|TRANSACTED_WITH|OWNS]-(entity)-[r2:USED|TRANSACTED_WITH|OWNS]-(a2:Account)
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
        
        result = session.run(
            """
            MATCH (a:Account)
            WHERE a.id IN $account_ids
            OPTIONAL MATCH (a)-[:USED]-(d:Device)
            OPTIONAL MATCH (a)-[:USED]-(i:IP)
            OPTIONAL MATCH (a)-[:TRANSACTED_WITH]-(m:Merchant)
            OPTIONAL MATCH (a)-[:OWNS]-(c:Card)
            RETURN a.id AS id,
                   count(DISTINCT d) AS device_count,
                   count(DISTINCT i) AS ip_count,
                   count(DISTINCT m) AS merchant_count,
                   count(DISTINCT c) AS card_count
            """,
            account_ids=account_ids
        )
        features = {record["id"]: [
            record["device_count"],
            record["ip_count"],
            record["merchant_count"],
            record["card_count"]
        ] for record in result}
        
        for node_id, feat in features.items():
            G.nodes[node_id]['x'] = feat
        
        return G
    
    def prepare_data(
        self,
        G: nx.Graph,
        labels: Dict[str, int]
    ) -> Tuple[Data, List[str]]:
        pyg_data = from_networkx(G)
        
        node_list = list(G.nodes())
        x_list = []
        for node in node_list:
            feat = G.nodes[node].get('x', [0, 0, 0, 0])
            x_list.append(feat)
        
        pyg_data.x = torch.tensor(x_list, dtype=torch.float)
        pyg_data.y = torch.tensor([labels.get(node, 0) for node in node_list], dtype=torch.long)
        
        train_mask = torch.zeros(len(node_list), dtype=torch.bool)
        test_mask = torch.zeros(len(node_list), dtype=torch.bool)
        
        node_indices = list(range(len(node_list)))
        np.random.shuffle(node_indices)
        split_idx = int(0.8 * len(node_indices))
        train_indices = node_indices[:split_idx]
        test_indices = node_indices[split_idx:]
        
        for idx in train_indices:
            if labels.get(node_list[idx], -1) >= 0:
                train_mask[idx] = True
        for idx in test_indices:
            if labels.get(node_list[idx], -1) >= 0:
                test_mask[idx] = True
        
        pyg_data.train_mask = train_mask
        pyg_data.test_mask = test_mask
        
        return pyg_data, node_list
    
    def train(self, train_data: Data, val_data: Optional[Data] = None):
        class GraphSAGE(torch.nn.Module):
            def __init__(self, in_dim, hidden_dim, out_dim, num_layers):
                super().__init__()
                self.convs = torch.nn.ModuleList()
                self.convs.append(SAGEConv(in_dim, hidden_dim))
                for _ in range(num_layers - 2):
                    self.convs.append(SAGEConv(hidden_dim, hidden_dim))
                self.convs.append(SAGEConv(hidden_dim, out_dim))
                self.bns = torch.nn.ModuleList()
                for _ in range(num_layers - 1):
                    self.bns.append(torch.nn.BatchNorm1d(hidden_dim))
            
            def forward(self, x, edge_index):
                for i, conv in enumerate(self.convs):
                    x = conv(x, edge_index)
                    if i < len(self.convs) - 1:
                        x = F.relu(x)
                        x = self.bns[i](x)
                        x = F.dropout(x, p=0.2, training=self.training)
                return x
        
        in_dim = train_data.x.shape[1]
        self.model = GraphSAGE(
            in_dim=in_dim,
            hidden_dim=self.embedding_dim,
            out_dim=self.embedding_dim,
            num_layers=self.num_layers
        ).to(self.device)
        
        optimizer = torch.optim.Adam(self.model.parameters(), lr=self.lr)
        criterion = torch.nn.CrossEntropyLoss()
        
        train_data = train_data.to(self.device)
        
        for epoch in range(self.epochs):
            self.model.train()
            optimizer.zero_grad()
            out = self.model(train_data.x, train_data.edge_index)
            
            loss = criterion(out[train_data.train_mask], train_data.y[train_data.train_mask])
            loss.backward()
            optimizer.step()
            
            if epoch % 10 == 0:
                self.model.eval()
                with torch.no_grad():
                    pred = out.argmax(dim=1)
                    acc = (pred[train_data.train_mask] == train_data.y[train_data.train_mask]).float().mean()
                    logger.info(f"Epoch {epoch}: Loss {loss.item():.4f}, Train Acc {acc:.4f}")
    
    def generate_embeddings(self, data: Data, node_list: List[str]) -> Dict[str, List[float]]:
        if self.model is None:
            raise ValueError("Model not trained. Call train() first.")
        
        self.model.eval()
        data = data.to(self.device)
        with torch.no_grad():
            embeddings = self.model(data.x, data.edge_index)
        
        embedding_dict = {}
        for i, node_id in enumerate(node_list):
            embedding_dict[node_id] = embeddings[i].cpu().numpy().tolist()
        
        return embedding_dict
    
    def save_model(self, path: str):
        if self.model is None:
            raise ValueError("No model to save")
        torch.save(self.model.state_dict(), path)
        logger.info(f"Model saved to {path}")
    
    def load_model(self, path: str, in_dim: int):
        class GraphSAGE(torch.nn.Module):
            def __init__(self, in_dim, hidden_dim, out_dim, num_layers):
                super().__init__()
                self.convs = torch.nn.ModuleList()
                self.convs.append(SAGEConv(in_dim, hidden_dim))
                for _ in range(num_layers - 2):
                    self.convs.append(SAGEConv(hidden_dim, hidden_dim))
                self.convs.append(SAGEConv(hidden_dim, out_dim))
                self.bns = torch.nn.ModuleList()
                for _ in range(num_layers - 1):
                    self.bns.append(torch.nn.BatchNorm1d(hidden_dim))
            
            def forward(self, x, edge_index):
                for i, conv in enumerate(self.convs):
                    x = conv(x, edge_index)
                    if i < len(self.convs) - 1:
                        x = F.relu(x)
                        x = self.bns[i](x)
                        x = F.dropout(x, p=0.2, training=self.training)
                return x
        
        self.model = GraphSAGE(
            in_dim=in_dim,
            hidden_dim=self.embedding_dim,
            out_dim=self.embedding_dim,
            num_layers=self.num_layers
        ).to(self.device)
        self.model.load_state_dict(torch.load(path))
        self.model.eval()
        logger.info(f"Model loaded from {path}")