from typing import Tuple, Optional
from enum import Enum

class Decision(Enum):
    APPROVE = "APPROVE"
    CHALLENGE = "CHALLENGE"
    DECLINE = "DECLINE"
    INVESTIGATE = "INVESTIGATE"

class DecisionPolicy:
    def __init__(
        self,
        ring_score_threshold: float = 0.90,
        min_ring_members: int = 2,
        combined_decline_threshold: float = 0.90,
        combined_challenge_threshold_low: float = 0.50,
        combined_challenge_threshold_high: float = 0.90,
        alpha: float = 0.6
    ):
        self.ring_score_threshold = ring_score_threshold
        self.min_ring_members = min_ring_members
        self.decline_threshold = combined_decline_threshold
        self.challenge_low = combined_challenge_threshold_low
        self.challenge_high = combined_challenge_threshold_high
        self.alpha = alpha
    
    def decide(
        self,
        account_id: str,
        ring_score: float,
        confirmed_ring_members: int,
        tabular_fraud_probability: float
    ) -> Tuple[Decision, float]:
        if ring_score > self.ring_score_threshold and confirmed_ring_members >= self.min_ring_members:
            return Decision.INVESTIGATE, ring_score
        
        combined = self.alpha * tabular_fraud_probability + (1 - self.alpha) * ring_score
        
        if combined > self.decline_threshold:
            return Decision.DECLINE, combined
        elif self.challenge_low < combined <= self.challenge_high:
            return Decision.CHALLENGE, combined
        else:
            return Decision.APPROVE, combined