import time
import logging
from enum import Enum
from typing import Optional, Callable, Any
from datetime import datetime, timedelta
from redis import Redis
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CircuitState(Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"

class CircuitBreaker:
    def __init__(
        self,
        name: str,
        redis_client: Redis,
        failure_threshold: int = 5,
        timeout_seconds: int = 60,
        half_open_max_requests: int = 3,
        success_threshold: int = 2
    ):
        self.name = name
        self.redis = redis_client
        self.failure_threshold = failure_threshold
        self.timeout_seconds = timeout_seconds
        self.half_open_max_requests = half_open_max_requests
        self.success_threshold = success_threshold
        
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time = None
        self._half_open_requests = 0
        self._half_open_successes = 0
    
    def _get_state(self) -> CircuitState:
        state_data = self.redis.get(f"circuit:{self.name}:state")
        if state_data:
            return CircuitState(state_data)
        return CircuitState.CLOSED
    
    def _set_state(self, state: CircuitState):
        self.redis.setex(
            f"circuit:{self.name}:state",
            3600,
            state.value
        )
        self._state = state
    
    def _get_failure_count(self) -> int:
        count = self.redis.get(f"circuit:{self.name}:failures")
        return int(count) if count else 0
    
    def _increment_failure(self):
        self.redis.incr(f"circuit:{self.name}:failures")
        self.redis.expire(f"circuit:{self.name}:failures", 300)
    
    def _reset_failures(self):
        self.redis.delete(f"circuit:{self.name}:failures")
    
    def _get_last_failure(self) -> Optional[datetime]:
        timestamp = self.redis.get(f"circuit:{self.name}:last_failure")
        if timestamp:
            return datetime.fromisoformat(timestamp)
        return None
    
    def _set_last_failure(self):
        self.redis.setex(
            f"circuit:{self.name}:last_failure",
            3600,
            datetime.utcnow().isoformat()
        )
    
    def _is_timeout_expired(self) -> bool:
        last_failure = self._get_last_failure()
        if not last_failure:
            return True
        return (datetime.utcnow() - last_failure) > timedelta(seconds=self.timeout_seconds)
    
    def _reset_half_open_state(self):
        self.redis.delete(f"circuit:{self.name}:half_open_requests")
        self.redis.delete(f"circuit:{self.name}:half_open_successes")
    
    def _get_half_open_requests(self) -> int:
        count = self.redis.get(f"circuit:{self.name}:half_open_requests")
        return int(count) if count else 0
    
    def _increment_half_open_requests(self) -> int:
        count = self.redis.incr(f"circuit:{self.name}:half_open_requests")
        self.redis.expire(f"circuit:{self.name}:half_open_requests", 60)
        return count
    
    def _get_half_open_successes(self) -> int:
        count = self.redis.get(f"circuit:{self.name}:half_open_successes")
        return int(count) if count else 0
    
    def _increment_half_open_successes(self) -> int:
        count = self.redis.incr(f"circuit:{self.name}:half_open_successes")
        self.redis.expire(f"circuit:{self.name}:half_open_successes", 60)
        return count
    
    def allow_request(self) -> bool:
        state = self._get_state()
        self._state = state
        
        if state == CircuitState.CLOSED:
            return True
        
        elif state == CircuitState.OPEN:
            if self._is_timeout_expired():
                logger.info(f"Circuit {self.name} transitioning to HALF_OPEN")
                self._set_state(CircuitState.HALF_OPEN)
                self._reset_half_open_state()
                return True
            logger.warning(f"Circuit {self.name} is OPEN, request blocked")
            return False
        
        elif state == CircuitState.HALF_OPEN:
            requests = self._get_half_open_requests()
            if requests >= self.half_open_max_requests:
                logger.warning(f"Circuit {self.name} HALF_OPEN max requests reached")
                return False
            self._increment_half_open_requests()
            return True
        
        return True
    
    def record_success(self):
        state = self._get_state()
        
        if state == CircuitState.CLOSED:
            self._reset_failures()
        
        elif state == CircuitState.HALF_OPEN:
            successes = self._increment_half_open_successes()
            if successes >= self.success_threshold:
                logger.info(f"Circuit {self.name} transitioning to CLOSED (success threshold met)")
                self._set_state(CircuitState.CLOSED)
                self._reset_failures()
                self._reset_half_open_state()
    
    def record_failure(self):
        state = self._get_state()
        self._increment_failure()
        self._set_last_failure()
        
        if state == CircuitState.CLOSED:
            failures = self._get_failure_count()
            if failures >= self.failure_threshold:
                logger.warning(f"Circuit {self.name} transitioning to OPEN (failure threshold: {failures})")
                self._set_state(CircuitState.OPEN)
        
        elif state == CircuitState.HALF_OPEN:
            logger.warning(f"Circuit {self.name} half-open request failed, reverting to OPEN")
            self._set_state(CircuitState.OPEN)
            self._reset_half_open_state()
    
    def get_status(self) -> dict:
        return {
            "name": self.name,
            "state": self._get_state().value,
            "failure_count": self._get_failure_count(),
            "last_failure": self._get_last_failure(),
            "timeout_seconds": self.timeout_seconds,
            "half_open_requests": self._get_half_open_requests(),
            "half_open_successes": self._get_half_open_successes()
        }

class CircuitBreakerDecorator:
    def __init__(self, circuit_breaker: CircuitBreaker, fallback: Optional[Callable] = None):
        self.circuit_breaker = circuit_breaker
        self.fallback = fallback
    
    def __call__(self, func: Callable) -> Callable:
        def wrapper(*args, **kwargs) -> Any:
            if not self.circuit_breaker.allow_request():
                if self.fallback:
                    return self.fallback(*args, **kwargs)
                raise Exception(f"Circuit {self.circuit_breaker.name} is OPEN")
            
            try:
                result = func(*args, **kwargs)
                self.circuit_breaker.record_success()
                return result
            except Exception as e:
                self.circuit_breaker.record_failure()
                if self.fallback:
                    return self.fallback(*args, **kwargs)
                raise e
        
        return wrapper