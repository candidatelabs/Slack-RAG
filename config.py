import os
from dataclasses import dataclass
from typing import Optional
from pathlib import Path
import yaml
from dotenv import load_dotenv

@dataclass
class DatabaseConfig:
    url: str
    pool_size: int = 5
    max_overflow: int = 10
    pool_timeout: int = 30
    pool_recycle: int = 3600

@dataclass
class APIConfig:
    slack_token: str
    openai_api_key: str
    anthropic_api_key: str
    rate_limit_calls: int = 50
    rate_limit_period: int = 60
    max_retries: int = 3
    retry_delay: int = 1

@dataclass
class CacheConfig:
    cache_dir: Path
    max_size: int = 1000
    ttl: int = 3600

@dataclass
class AppConfig:
    db: DatabaseConfig
    api: APIConfig
    cache: CacheConfig
    log_level: str = "INFO"
    max_workers: Optional[int] = None
    batch_size: int = 100

def load_config() -> AppConfig:
    load_dotenv()
    print('--- ENVIRONMENT VARIABLES ---')
    for k, v in os.environ.items():
        print(f'{k}={v}')
    print('-----------------------------')
    # Debug print for environment variables
    print('SLACK_TOKEN:', os.getenv('SLACK_TOKEN'))
    print('OPENAI_API_KEY:', os.getenv('OPENAI_API_KEY'))
    print('ANTHROPIC_API_KEY:', os.getenv('ANTHROPIC_API_KEY'))
    
    # Load YAML config if exists
    config_path = Path("config.yaml")
    if config_path.exists():
        with open(config_path) as f:
            config_data = yaml.safe_load(f)
    else:
        config_data = {}

    # Environment variables take precedence
    db_url = os.getenv("DATABASE_URL", "sqlite:///slack_data.db")
    slack_token = os.getenv("SLACK_TOKEN")
    openai_api_key = os.getenv("OPENAI_API_KEY")
    anthropic_api_key = os.getenv("ANTHROPIC_API_KEY")

    if not all([slack_token, openai_api_key, anthropic_api_key]):
        raise ValueError("Missing required API keys in environment variables")

    return AppConfig(
        db=DatabaseConfig(
            url=db_url,
            pool_size=int(os.getenv("DB_POOL_SIZE", "5")),
            max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "10")),
            pool_timeout=int(os.getenv("DB_POOL_TIMEOUT", "30")),
            pool_recycle=int(os.getenv("DB_POOL_RECYCLE", "3600"))
        ),
        api=APIConfig(
            slack_token=slack_token,
            openai_api_key=openai_api_key,
            anthropic_api_key=anthropic_api_key,
            rate_limit_calls=int(os.getenv("RATE_LIMIT_CALLS", "50")),
            rate_limit_period=int(os.getenv("RATE_LIMIT_PERIOD", "60")),
            max_retries=int(os.getenv("MAX_RETRIES", "3")),
            retry_delay=int(os.getenv("RETRY_DELAY", "1"))
        ),
        cache=CacheConfig(
            cache_dir=Path(os.getenv("CACHE_DIR", ".cache")),
            max_size=int(os.getenv("CACHE_MAX_SIZE", "1000")),
            ttl=int(os.getenv("CACHE_TTL", "3600"))
        ),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        max_workers=int(os.getenv("MAX_WORKERS", "0")) or None,
        batch_size=int(os.getenv("BATCH_SIZE", "100"))
    ) 