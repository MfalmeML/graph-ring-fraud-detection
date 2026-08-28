from prometheus_client import Counter, Histogram, Gauge, generate_latest
from typing import Optional
import time

class Metrics:
    def __init__(self):
        self.ring_score_latency = Histogram(
            'ring_score_latency_seconds',
            'Latency of ring score computation',
            buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
        )
        self.ring_score_requests = Counter(
            'ring_score_requests_total',
            'Total ring score requests',
            ['status']
        )
        self.graph_queries = Counter(
            'graph_queries_total',
            'Total graph queries',
            ['operation', 'status']
        )
        self.graph_query_latency = Histogram(
            'graph_query_latency_seconds',
            'Latency of graph queries',
            ['operation'],
            buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0]
        )
        self.cache_hit_rate = Gauge(
            'cache_hit_rate',
            'Cache hit rate'
        )
        self.ring_score_distribution = Histogram(
            'ring_score_distribution',
            'Distribution of ring scores',
            buckets=[0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
        )
        self.circuit_state = Gauge(
            'circuit_state',
            'Circuit breaker state (0=CLOSED, 1=OPEN, 2=HALF_OPEN)',
            ['circuit']
        )
    
    def record_latency(self, duration: float):
        self.ring_score_latency.observe(duration)
    
    def record_request(self, status: str):
        self.ring_score_requests.labels(status=status).inc()
    
    def record_graph_query(self, operation: str, status: str, duration: float):
        self.graph_queries.labels(operation=operation, status=status).inc()
        self.graph_query_latency.labels(operation=operation).observe(duration)
    
    def update_cache_hit_rate(self, rate: float):
        self.cache_hit_rate.set(rate)
    
    def record_ring_score(self, score: float):
        self.ring_score_distribution.observe(score)
    
    def update_circuit_state(self, circuit_name: str, state: int):
        self.circuit_state.labels(circuit=circuit_name).set(state)

metrics = Metrics()

def timing_decorator(operation_name: str):
    def decorator(func):
        def wrapper(*args, **kwargs):
            start = time.time()
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start
                metrics.record_graph_query(operation_name, "success", duration)
                return result
            except Exception as e:
                duration = time.time() - start
                metrics.record_graph_query(operation_name, "error", duration)
                raise e
        return wrapper
    return decorator