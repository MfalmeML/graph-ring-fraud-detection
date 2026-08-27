import torch
import torch.nn as nn
import torch.nn.functional as F
from typing import Dict, List, Tuple, Optional
import numpy as np
from sklearn.metrics import roc_auc_score, average_precision_score
import logging
import json
from redis import Redis
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FusionModel(nn.Module):
    def __init__(self, hidden_dim: int = 16):
        super().__init__()
        self.fc1 = nn.Linear(2, hidden_dim)
        self.fc2 = nn.Linear(hidden_dim, 1)
        self.dropout = nn.Dropout(0.2)
    
    def forward(self, tabular_prob: torch.Tensor, ring_score: torch.Tensor) -> torch.Tensor:
        x = torch.stack([tabular_prob, ring_score], dim=-1)
        x = F.relu(self.fc1(x))
        x = self.dropout(x)
        x = torch.sigmoid(self.fc2(x))
        return x.squeeze(-1)

class LearnedFusion:
    def __init__(
        self,
        learning_rate: float = 0.001,
        epochs: int = 50,
        validation_split: float = 0.2,
        redis_host: str = "localhost",
        redis_port: int = 6379
    ):
        self.lr = learning_rate
        self.epochs = epochs
        self.val_split = validation_split
        self.model = None
        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        self.redis_client = Redis(
            host=redis_host,
            port=redis_port,
            decode_responses=True
        )
    
    def train(
        self,
        tabular_scores: Dict[str, float],
        ring_scores: Dict[str, float],
        labels: Dict[str, int]
    ) -> Dict[str, float]:
        account_ids = list(labels.keys())
        np.random.shuffle(account_ids)
        
        split_idx = int((1 - self.val_split) * len(account_ids))
        train_ids = account_ids[:split_idx]
        val_ids = account_ids[split_idx:]
        
        X_train_tab = torch.tensor([tabular_scores.get(a, 0.5) for a in train_ids], dtype=torch.float)
        X_train_ring = torch.tensor([ring_scores.get(a, 0.0) for a in train_ids], dtype=torch.float)
        y_train = torch.tensor([labels[a] for a in train_ids], dtype=torch.float)
        
        X_val_tab = torch.tensor([tabular_scores.get(a, 0.5) for a in val_ids], dtype=torch.float)
        X_val_ring = torch.tensor([ring_scores.get(a, 0.0) for a in val_ids], dtype=torch.float)
        y_val = torch.tensor([labels[a] for a in val_ids], dtype=torch.float)
        
        X_train_tab = X_train_tab.to(self.device)
        X_train_ring = X_train_ring.to(self.device)
        y_train = y_train.to(self.device)
        X_val_tab = X_val_tab.to(self.device)
        X_val_ring = X_val_ring.to(self.device)
        y_val = y_val.to(self.device)
        
        self.model = FusionModel(hidden_dim=16).to(self.device)
        optimizer = torch.optim.Adam(self.model.parameters(), lr=self.lr)
        criterion = nn.BCELoss()
        
        best_val_auc = 0.0
        best_state = None
        
        for epoch in range(self.epochs):
            self.model.train()
            optimizer.zero_grad()
            pred = self.model(X_train_tab, X_train_ring)
            loss = criterion(pred, y_train)
            loss.backward()
            optimizer.step()
            
            if epoch % 10 == 0:
                self.model.eval()
                with torch.no_grad():
                    val_pred = self.model(X_val_tab, X_val_ring).cpu().numpy()
                    y_val_np = y_val.cpu().numpy()
                    train_pred = self.model(X_train_tab, X_train_ring).cpu().numpy()
                    y_train_np = y_train.cpu().numpy()
                    
                    train_auc = roc_auc_score(y_train_np, train_pred) if len(np.unique(y_train_np)) > 1 else 0.5
                    val_auc = roc_auc_score(y_val_np, val_pred) if len(np.unique(y_val_np)) > 1 else 0.5
                    val_auprc = average_precision_score(y_val_np, val_pred)
                    
                    logger.info(f"Epoch {epoch}: Loss {loss.item():.4f}, Train AUC {train_auc:.4f}, Val AUC {val_auc:.4f}")
                    
                    if val_auc > best_val_auc:
                        best_val_auc = val_auc
                        best_state = self.model.state_dict().copy()
        
        if best_state:
            self.model.load_state_dict(best_state)
        
        self.model.eval()
        with torch.no_grad():
            val_pred = self.model(X_val_tab, X_val_ring).cpu().numpy()
            y_val_np = y_val.cpu().numpy()
            final_auc = roc_auc_score(y_val_np, val_pred) if len(np.unique(y_val_np)) > 1 else 0.5
            final_auprc = average_precision_score(y_val_np, val_pred)
        
        self._update_fusion_cache()
        
        return {
            "train_auc": train_auc,
            "val_auc": final_auc,
            "val_auprc": final_auprc,
            "best_val_auc": best_val_auc
        }
    
    def predict(self, tabular_prob: float, ring_score: float) -> float:
        if self.model is None:
            raise ValueError("Model not trained. Call train() first.")
        
        x_tab = torch.tensor([tabular_prob], dtype=torch.float).to(self.device)
        x_ring = torch.tensor([ring_score], dtype=torch.float).to(self.device)
        self.model.eval()
        with torch.no_grad():
            return float(self.model(x_tab, x_ring).cpu().numpy()[0])
    
    def save(self, path: str):
        if self.model is None:
            raise ValueError("No model to save")
        torch.save(self.model.state_dict(), path)
        logger.info(f"Fusion model saved to {path}")
        self._update_fusion_cache()
    
    def load(self, path: str):
        self.model = FusionModel(hidden_dim=16).to(self.device)
        self.model.load_state_dict(torch.load(path))
        self.model.eval()
        logger.info(f"Fusion model loaded from {path}")
    
    def _update_fusion_cache(self):
        if self.model is None:
            return
        model_bytes = torch.jit.trace(self.model, (torch.rand(1), torch.rand(1))).code
        self.redis_client.set("fusion_model_state", model_bytes)
        self.redis_client.set("fusion_model_updated_at", datetime.utcnow().isoformat())
    
    def compare_with_override_rule(
        self,
        tabular_scores: Dict[str, float],
        ring_scores: Dict[str, float],
        labels: Dict[str, int],
        alpha: float = 0.6,
        override_threshold: float = 0.90,
        min_ring_members: int = 2
    ) -> Dict[str, float]:
        account_ids = list(labels.keys())
        
        combined_override = []
        combined_learned = []
        y_true = []
        
        for account_id in account_ids:
            tab = tabular_scores.get(account_id, 0.5)
            ring = ring_scores.get(account_id, 0.0)
            label = labels[account_id]
            
            override_score = alpha * tab + (1 - alpha) * ring
            if ring > override_threshold:
                override_score = max(override_score, 0.9)
            
            learned_score = self.predict(tab, ring) if self.model else override_score
            
            combined_override.append(override_score)
            combined_learned.append(learned_score)
            y_true.append(label)
        
        y_true_np = np.array(y_true)
        override_auc = roc_auc_score(y_true_np, combined_override) if len(np.unique(y_true_np)) > 1 else 0.5
        learned_auc = roc_auc_score(y_true_np, combined_learned) if len(np.unique(y_true_np)) > 1 else 0.5
        override_auprc = average_precision_score(y_true_np, combined_override)
        learned_auprc = average_precision_score(y_true_np, combined_learned)
        
        return {
            "override_auc": override_auc,
            "learned_auc": learned_auc,
            "override_auprc": override_auprc,
            "learned_auprc": learned_auprc,
            "lift_auc": learned_auc - override_auc,
            "lift_auprc": learned_auprc - override_auprc
        }