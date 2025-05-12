import logging
import sys
import time
from functools import wraps
from typing import Any, Callable
import structlog
from prometheus_client import Counter, Histogram, start_http_server

# Metrics
api_calls = Counter('api_calls_total', 'Total API calls', ['api_name', 'status'])
processing_time = Histogram('processing_seconds', 'Time spent processing', ['operation'])
cache_hits = Counter('cache_hits_total', 'Total cache hits')
cache_misses = Counter('cache_misses_total', 'Total cache misses')
db_operations = Counter('db_operations_total', 'Total database operations', ['operation'])

def setup_logging(log_level: str = "INFO") -> None:
    """Configure structured logging."""
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level.upper()),
    )

    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        wrapper_class=structlog.BoundLogger,
        cache_logger_on_first_use=True,
    )

def get_logger(name: str) -> structlog.BoundLogger:
    """Get a structured logger instance."""
    return structlog.get_logger(name)

def log_metrics(func: Callable) -> Callable:
    """Decorator to log metrics for function calls."""
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            status = "success"
            return result
        except Exception as e:
            status = "error"
            raise
        finally:
            duration = time.time() - start_time
            processing_time.labels(operation=func.__name__).observe(duration)
            if hasattr(func, 'api_name'):
                api_calls.labels(api_name=func.api_name, status=status).inc()
    return wrapper

def start_metrics_server(port: int = 8000) -> None:
    """Start the Prometheus metrics server."""
    start_http_server(port)

class MetricsMiddleware:
    """Middleware for collecting metrics."""
    def __init__(self, app: Any):
        self.app = app

    def __call__(self, environ: dict, start_response: Callable) -> Any:
        start_time = time.time()
        try:
            response = self.app(environ, start_response)
            status = "success"
            return response
        except Exception as e:
            status = "error"
            raise
        finally:
            duration = time.time() - start_time
            processing_time.labels(operation="request").observe(duration)
            api_calls.labels(api_name="http", status=status).inc() 