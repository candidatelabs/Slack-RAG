import time
from functools import wraps
from typing import Any, Callable, Optional
import random
from config import APIConfig
from logger import get_logger, api_calls

logger = get_logger(__name__)

class RateLimiter:
    def __init__(self, config: APIConfig):
        self.config = config
        self.calls: list[float] = []
        self.last_reset = time.time()

    def _cleanup_old_calls(self) -> None:
        """Remove calls older than the rate limit period."""
        current_time = time.time()
        self.calls = [t for t in self.calls if current_time - t < self.config.rate_limit_period]

    def _wait_time(self) -> float:
        """Calculate how long to wait before making another call."""
        self._cleanup_old_calls()
        if len(self.calls) >= self.config.rate_limit_calls:
            return self.calls[0] + self.config.rate_limit_period - time.time()
        return 0

    def acquire(self) -> None:
        """Acquire permission to make an API call."""
        wait_time = self._wait_time()
        if wait_time > 0:
            logger.debug("Rate limit reached, waiting", wait_time=wait_time)
            time.sleep(wait_time)
        self.calls.append(time.time())

def rate_limited(config: APIConfig) -> Callable:
    """Decorator for rate limiting API calls with exponential backoff."""
    limiter = RateLimiter(config)

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            retries = 0
            while retries < config.max_retries:
                try:
                    limiter.acquire()
                    result = func(*args, **kwargs)
                    api_calls.labels(api_name=func.__name__, status="success").inc()
                    return result
                except Exception as e:
                    retries += 1
                    if retries == config.max_retries:
                        api_calls.labels(api_name=func.__name__, status="error").inc()
                        logger.error("API call failed after retries", 
                                   function=func.__name__,
                                   error=str(e),
                                   retries=retries)
                        raise
                    
                    # Exponential backoff with jitter
                    wait_time = (config.retry_delay * (2 ** (retries - 1)) + 
                               random.uniform(0, 1))
                    logger.warning("API call failed, retrying",
                                 function=func.__name__,
                                 error=str(e),
                                 retry=retries,
                                 wait_time=wait_time)
                    time.sleep(wait_time)
            return None
        return wrapper
    return decorator 