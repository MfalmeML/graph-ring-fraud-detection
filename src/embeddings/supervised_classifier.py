import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.nn import SAGEConv, global_mean_pool
from typing import Dict, List, Tuple, Optional
import numpy as np
from sklearn.metrics import roc_auc_score, average_precision_score
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RingClassifier(nn.Module):
    def __init__(self, embedding_dim: int = 64, hidden_dim: int = 32):
        super().__init__()
        self.fc1 = nn.Linear(embedding_dim, hidden_dim)
        self.fc2 = nn.Linear(hidden_dim, 1)
        self.dropout = nn.Dropout(0.3)
    
    def forward(self, x):
        x = F.relu(self.fc1(x))
        x = self.dropout(x)
        x = torch.sigmoid(self.fc2(x))
        return x.squeeze(-1)

class SupervisedRingClassifier:
    def __init__(
        self,
        embedding_dim: int = 64,
        learning_rate: float = 0.001,
        epochs: int = 50
    ):
        self.embedding_dim = embedding_dim
        self.lr = learning_rate
        self.epochs = epochs
        self.classifier = None
        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    
    def train(
        self,
        embeddings: Dict[str, List[float]],
        labels: Dict[str, int],
        train_split: float = 0.8
    ) -> Dict[str, float]:
        account_ids = list(embeddings.keys())
        np.random.shuffle(account_ids)
        split_idx = int(train_split * len(account_ids))
        train_ids = account_ids[:split_idx]
        test_ids = account_ids[split_idx:]
        
        X_train = torch.tensor([embeddings[a] for a in train_ids], dtype=torch.float)
        y_train = torch.tensor([labels[a] for a in train_ids], dtype=torch.float)
        X_test = torch.tensor([embeddings[a] for a in test_ids], dtype=torch.float)
        y_test = torch.tensor([labels[a] for a in test_ids], dtype=torch.float)
        
        X_train = X_train.to(self.device)
        y_train = y_train.to(self.device)
        X_test = X_test.to(self.device)
        y_test = y_test.to(self.device)
        
        self.classifier = RingClassifier(
            embedding_dim=self.embedding_dim,
            hidden_dim=max(8, self.embedding_dim // 2)
        ).to(self.device)
        
        optimizer = torch.optim.Adam(self.classifier.parameters(), lr=self.lr)
        criterion = nn.BCELoss()
        
        for epoch in range(self.epochs):
            self.classifier.train()
            optimizer.zero_grad()
            pred = self.classifier(X_train)
            loss = criterion(pred, y_train)
            loss.backward()
            optimizer.step()
            
            if epoch % 10 == 0:
                self.classifier.eval()
                with torch.no_grad():
                    train_pred = self.classifier(X_train).cpu().numpy()
                    test_pred = self.classifier(X_test).cpu().numpy()
                    y_train_np = y_train.cpu().numpy()
                    y_test_np = y_test.cpu().numpy()
                    
                    train_auc = roc_auc_score(y_train_np, train_pred) if len(np.unique(y_train_np)) > 1 else 0.5
                    test_auc = roc_auc_score(y_test_np, test_pred) if len(np.unique(y_test_np)) > 1 else 0.5
                    logger.info(f"Epoch {epoch}: Loss {loss.item():.4f}, Train AUC {train_auc:.4f}, Test AUC {test_auc:.4f}")
        
        self.classifier.eval()
        with torch.no_grad():
            test_pred = self.classifier(X_test).cpu().numpy()
            y_test_np = y_test.cpu().numpy()
            auc = roc_auc_score(y_test_np, test_pred) if len(np.unique(y_test_np)) > 1 else 0.5
            auprc = average_precision_score(y_test_np, test_pred)
            
        return {"test_auc": auc, "test_auprc": auprc}
    
    def predict(self, embedding: List[float]) -> float:
        if self.classifier is None:
            raise ValueError("Classifier not trained. Call train() first.")
        
        x = torch.tensor([embedding], dtype=torch.float).to(self.device)
        self.classifier.eval()
        with torch.no_grad():
            return float(self.classifier(x).cpu().numpy()[0])
    
    def save(self, path: str):
        if self.classifier is None:
            raise ValueError("No classifier to save")
        torch.save(self.classifier.state_dict(), path)
        logger.info(f"Classifier saved to {path}")
    
    def load(self, path: str):
        self.classifier = RingClassifier(
            embedding_dim=self.embedding_dim,
            hidden_dim=max(8, self.embedding_dim // 2)
        ).to(self.device)
        self.classifier.load_state_dict(torch.load(path))
        self.classifier.eval()
        logger.info(f"Classifier loaded from {path}")