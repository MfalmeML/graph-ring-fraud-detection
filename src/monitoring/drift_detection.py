import json
import logging
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from collections import deque
from scipy.stats import ks_2samp, wasserstein_distance
from redis import Redis
from neo4j import Session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DriftDetector:
    def __init__(
        self,
        redis_client: Redis,
        neo4j_session: Session,
        window_size: int = 10000,
        alert_threshold_ks: float = 0.1,
        alert_threshold_js: float = 0.05
    ):
        self.redis = redis_client
        self.session = neo4j_session
        self.window_size = window_size
        self.alert_threshold_ks = alert_threshold_ks
        self.alert_threshold_js = alert_threshold_js
        self.feature_history = {
            "device_account_count": deque(maxlen=window_size),
            "ip_account_count": deque(maxlen=window_size),
            "triangle_count": deque(maxlen=window_size),
            "clustering_coefficient": deque(maxlen=window_size),
            "ring_score": deque(maxlen=window_size)
        }
        self.baseline_features = {}
        self.last_alert_time = {}
    
    def collect_current_distribution(self) -> Dict[str, List[float]]:
        result = self.session.run(
            """
            MATCH (a:Account)
            OPTIONAL MATCH (a)-[:USED]-(d:Device)
            OPTIONAL MATCH (a)-[:USED]-(i:IP)
            OPTIONAL MATCH (a)-[:TRANSACTED_WITH]-(m:Merchant)
            OPTIONAL MATCH (a)-[:OWNS]-(c:Card)
            WITH a, count(DISTINCT d) AS device_count, count(DISTINCT i) AS ip_count,
                 count(DISTINCT m) AS merchant_count, count(DISTINCT c) AS card_count
            OPTIONAL MATCH (a)-[:USED]-(e1)<-[:USED]-(other:Account)
            WITH a, device_count, ip_count, merchant_count, card_count,
                 count(DISTINCT other) AS connections
            OPTIONAL MATCH (a)-[:USED]-(e2)<-[:USED]-(other2:Account)
            WHERE other2 <> a
            WITH a, device_count, ip_count, merchant_count, card_count, connections,
                 count(DISTINCT other2) AS degree
            RETURN a.id AS account_id,
                   device_count AS device_account_count,
                   ip_count AS ip_account_count,
                   connections AS shared_entity_count,
                   CASE WHEN degree > 1 THEN degree ELSE 0 END AS triangle_count,
                   CASE WHEN degree > 1 THEN degree / (degree * degree) ELSE 0.0 END AS clustering_coefficient
            LIMIT 5000
            """
        )
        
        distribution = {
            "device_account_count": [],
            "ip_account_count": [],
            "triangle_count": [],
            "clustering_coefficient": [],
            "shared_entity_count": []
        }
        
        for record in result:
            for key in distribution.keys():
                if key in record:
                    distribution[key].append(float(record[key]))
        
        ring_scores = []
        for account_id in distribution["device_account_count"][:1000]:
            cached = self.redis.get(f"ring_score:{account_id}")
            if cached:
                data = json.loads(cached)
                ring_scores.append(data.get("ring_score", 0.0))
        
        distribution["ring_score"] = ring_scores
        
        return distribution
    
    def update_baseline(self) -> Dict[str, List[float]]:
        distribution = self.collect_current_distribution()
        
        for key, values in distribution.items():
            if len(values) > 100:
                self.baseline_features[key] = {
                    "mean": np.mean(values),
                    "std": np.std(values),
                    "percentiles": np.percentile(values, [5, 25, 50, 75, 95]).tolist(),
                    "count": len(values),
                    "updated_at": datetime.utcnow().isoformat()
                }
                self.redis.set(
                    f"baseline:{key}",
                    json.dumps(self.baseline_features[key])
                )
                self.feature_history[key].clear()
        
        logger.info(f"Baseline updated for {len(distribution)} features")
        return distribution
    
    def load_baseline(self):
        for key in ["device_account_count", "ip_account_count", "triangle_count", 
                    "clustering_coefficient", "ring_score"]:
            data = self.redis.get(f"baseline:{key}")
            if data:
                self.baseline_features[key] = json.loads(data)
            else:
                self.baseline_features[key] = None
    
    def detect_drift(self, current_distribution: Dict[str, List[float]]) -> Dict[str, Dict]:
        self.load_baseline()
        alerts = {}
        
        for feature, current_values in current_distribution.items():
            if len(current_values) < 50:
                continue
            
            baseline = self.baseline_features.get(feature)
            if baseline is None:
                continue
            
            baseline_values = self.feature_history[feature]
            for val in current_values[:100]:
                baseline_values.append(val)
            
            if len(baseline_values) < 100:
                continue
            
            baseline_array = np.array(list(baseline_values))
            current_array = np.array(current_values[:500])
            
            if len(baseline_array) < 50 or len(current_array) < 50:
                continue
            
            try:
                ks_stat, ks_p = ks_2samp(baseline_array, current_array)
                
                baseline_mean = np.mean(baseline_array)
                current_mean = np.mean(current_array)
                mean_shift = abs(current_mean - baseline_mean) / (baseline.get("std", 0.1) + 0.01)
                
                wasserstein = wasserstein_distance(baseline_array, current_array)
                
                if ks_stat > self.alert_threshold_ks or mean_shift > 2.0:
                    alerts[feature] = {
                        "drift_detected": True,
                        "ks_statistic": float(ks_stat),
                        "ks_p_value": float(ks_p),
                        "baseline_mean": float(baseline_mean),
                        "current_mean": float(current_mean),
                        "mean_shift_std": float(mean_shift),
                        "wasserstein_distance": float(wasserstein),
                        "sample_size_baseline": len(baseline_array),
                        "sample_size_current": len(current_array)
                    }
            except Exception as e:
                logger.error(f"Drift detection failed for {feature}: {e}")
        
        for feature, alert in alerts.items():
            if alert["drift_detected"]:
                last_time = self.last_alert_time.get(feature)
                if not last_time or (datetime.utcnow() - last_time) > timedelta(hours=1):
                    logger.warning(f"Drift detected for {feature}: KS={alert['ks_statistic']:.3f}")
                    self.last_alert_time[feature] = datetime.utcnow()
                    self._store_alert(feature, alert)
        
        return alerts
    
    def _store_alert(self, feature: str, alert: Dict):
        alert["feature"] = feature
        alert["detected_at"] = datetime.utcnow().isoformat()
        
        history = self.redis.get(f"drift_alerts:{feature}")
        if history:
            alerts = json.loads(history)
        else:
            alerts = []
        
        alerts.append(alert)
        if len(alerts) > 100:
            alerts = alerts[-100:]
        
        self.redis.setex(f"drift_alerts:{feature}", 2592000, json.dumps(alerts))
    
    def get_drift_summary(self) -> Dict[str, Dict]:
        self.load_baseline()
        summary = {}
        
        for feature, baseline in self.baseline_features.items():
            history = self.redis.get(f"drift_alerts:{feature}")
            alerts = json.loads(history) if history else []
            
            alert_count = len([a for a in alerts if a.get("drift_detected", False)])
            
            summary[feature] = {
                "baseline": baseline,
                "alert_count_last_30d": alert_count,
                "last_alert": alerts[-1] if alerts else None
            }
        
        return summary
    
    def run_monitoring_cycle(self) -> Dict[str, Dict]:
        distribution = self.collect_current_distribution()
        alerts = self.detect_drift(distribution)
        return alerts

class PerformanceMonitor:
    def __init__(self, redis_client: Redis):
        self.redis = redis_client
    
    def record_latency(self, operation: str, latency_ms: float):
        key = f"latency:{operation}"
        history = self.redis.get(key)
        if history:
            data = json.loads(history)
        else:
            data = []
        
        data.append({
            "timestamp": datetime.utcnow().isoformat(),
            "latency_ms": latency_ms
        })
        
        if len(data) > 1000:
            data = data[-1000:]
        
        self.redis.setex(key, 86400, json.dumps(data))
    
    def record_cache_hit(self, hit: bool):
        key = "cache_hit_rate"
        history = self.redis.get(key)
        if history:
            data = json.loads(history)
        else:
            data = {"hits": 0, "misses": 0, "history": []}
        
        if hit:
            data["hits"] += 1
        else:
            data["misses"] += 1
        
        data["history"].append({
            "timestamp": datetime.utcnow().isoformat(),
            "hit": hit
        })
        
        if len(data["history"]) > 1000:
            data["history"] = data["history"][-1000:]
        
        data["rate"] = data["hits"] / (data["hits"] + data["misses"]) if (data["hits"] + data["misses"]) > 0 else 0.5
        
        self.redis.setex(key, 86400, json.dumps(data))
    
    def get_performance_report(self) -> Dict:
        report = {}
        
        for key in ["latency:ring_score_lookup", "latency:feature_compute", "cache_hit_rate"]:
            data = self.redis.get(key)
            if data:
                report[key.split(":")[-1]] = json.loads(data)
        
        return report