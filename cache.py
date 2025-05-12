import pickle
import time
from pathlib import Path
from typing import Any, Optional, Callable
from functools import wraps
from logger import get_logger, cache_hits, cache_misses
from config import CacheConfig

logger = get_logger(__name__)

class PersistentCache:
    def __init__(self, config: CacheConfig):
        self.config = config
        self.cache_dir = config.cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache: dict[str, tuple[Any, float]] = {}
        self._load_cache()

    def _load_cache(self) -> None:
        """Load cache from disk."""
        try:
            cache_file = self.cache_dir / "cache.pkl"
            if cache_file.exists():
                with open(cache_file, "rb") as f:
                    self.cache = pickle.load(f)
                logger.info("Cache loaded from disk", size=len(self.cache))
        except Exception as e:
            logger.error("Failed to load cache", error=str(e))
            self.cache = {}

    def _save_cache(self) -> None:
        """Save cache to disk."""
        try:
            cache_file = self.cache_dir / "cache.pkl"
            with open(cache_file, "wb") as f:
                pickle.dump(self.cache, f)
            logger.debug("Cache saved to disk", size=len(self.cache))
        except Exception as e:
            logger.error("Failed to save cache", error=str(e))

    def get(self, key: str) -> Optional[Any]:
        """Get a value from cache if it exists and is not expired."""
        if key in self.cache:
            value, timestamp = self.cache[key]
            if time.time() - timestamp < self.config.ttl:
                cache_hits.inc()
                return value
            else:
                del self.cache[key]
                self._save_cache()
        cache_misses.inc()
        return None

    def set(self, key: str, value: Any) -> None:
        """Set a value in cache with current timestamp."""
        self.cache[key] = (value, time.time())
        if len(self.cache) > self.config.max_size:
            # Remove oldest entries
            sorted_items = sorted(self.cache.items(), key=lambda x: x[1][1])
            self.cache = dict(sorted_items[-self.config.max_size:])
        self._save_cache()

    def delete(self, key: str) -> None:
        """Delete a value from cache."""
        if key in self.cache:
            del self.cache[key]
            self._save_cache()

    def clear(self) -> None:
        """Clear all cache entries."""
        self.cache.clear()
        self._save_cache()

def cached(config: CacheConfig) -> Callable:
    """Decorator for caching function results."""
    cache = PersistentCache(config)

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Create cache key from function name and arguments
            key = f"{func.__name__}:{str(args)}:{str(kwargs)}"
            result = cache.get(key)
            if result is not None:
                return result
            result = func(*args, **kwargs)
            cache.set(key, result)
            return result
        return wrapper
    return decorator 