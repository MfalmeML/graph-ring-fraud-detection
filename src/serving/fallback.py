from typing import Optional

class FallbackHandler:
    def __init__(self, tabular_fraud_probability: float):
        self.tabular_fraud_probability = tabular_fraud_probability
    
    def get_combined_score(
        self,
        ring_score: Optional[float],
        alpha: float = 0.6
    ) -> float:
        if ring_score is None:
            return self.tabular_fraud_probability
        return alpha * self.tabular_fraud_probability + (1 - alpha) * ring_score