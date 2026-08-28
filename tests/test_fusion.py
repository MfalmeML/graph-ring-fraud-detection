import pytest
import torch
import numpy as np
from src.fusion.learned_fusion import LearnedFusion, FusionModel

class TestFusion:
    def test_model_forward(self):
        model = FusionModel(hidden_dim=8)
        tab = torch.tensor([0.5, 0.7, 0.3])
        ring = torch.tensor([0.2, 0.9, 0.4])
        output = model(tab, ring)
        assert output.shape == (3,)
        assert torch.all(output >= 0) and torch.all(output <= 1)
    
    def test_fusion_training(self):
        fusion = LearnedFusion(epochs=5)
        n_samples = 100
        tab_scores = {f"acc_{i}": np.random.random() for i in range(n_samples)}
        ring_scores = {f"acc_{i}": np.random.random() for i in range(n_samples)}
        labels = {f"acc_{i}": np.random.randint(0, 2) for i in range(n_samples)}
        
        results = fusion.train(tab_scores, ring_scores, labels)
        assert "val_auc" in results
        assert "val_auprc" in results
        assert 0.5 <= results["val_auc"] <= 1.0
    
    def test_prediction(self):
        fusion = LearnedFusion(epochs=2)
        n_samples = 50
        tab_scores = {f"acc_{i}": np.random.random() for i in range(n_samples)}
        ring_scores = {f"acc_{i}": np.random.random() for i in range(n_samples)}
        labels = {f"acc_{i}": np.random.randint(0, 2) for i in range(n_samples)}
        fusion.train(tab_scores, ring_scores, labels)
        
        pred = fusion.predict(0.5, 0.8)
        assert 0.0 <= pred <= 1.0
    
    def test_compare_override(self):
        fusion = LearnedFusion(epochs=3)
        n_samples = 100
        tab_scores = {f"acc_{i}": np.random.random() for i in range(n_samples)}
        ring_scores = {f"acc_{i}": np.random.random() for i in range(n_samples)}
        labels = {f"acc_{i}": np.random.randint(0, 2) for i in range(n_samples)}
        fusion.train(tab_scores, ring_scores, labels)
        
        comparison = fusion.compare_with_override_rule(tab_scores, ring_scores, labels)
        assert "lift_auc" in comparison