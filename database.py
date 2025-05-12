from contextlib import contextmanager
from typing import Generator, Any, Optional
import sqlite3
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError
from logger import get_logger, db_operations
from config import DatabaseConfig

logger = get_logger(__name__)

class DatabaseManager:
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.engine = create_engine(
            config.url,
            poolclass=QueuePool,
            pool_size=config.pool_size,
            max_overflow=config.max_overflow,
            pool_timeout=config.pool_timeout,
            pool_recycle=config.pool_recycle
        )
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        
        # Add event listeners for connection pool
        @event.listens_for(self.engine, 'checkout')
        def receive_checkout(dbapi_connection, connection_record, connection_proxy):
            logger.debug("Database connection checked out from pool")

        @event.listens_for(self.engine, 'checkin')
        def receive_checkin(dbapi_connection, connection_record):
            logger.debug("Database connection returned to pool")

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """Get a database session with automatic cleanup."""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            logger.error("Database error", error=str(e))
            raise
        finally:
            session.close()

    def execute_query(self, query: str, params: Optional[tuple] = None) -> Any:
        """Execute a query with metrics and error handling."""
        db_operations.labels(operation="query").inc()
        try:
            with self.get_session() as session:
                result = session.execute(query, params or ())
                return result.fetchall()
        except SQLAlchemyError as e:
            logger.error("Query execution failed", query=query, error=str(e))
            raise

    def execute_many(self, query: str, params_list: list[tuple]) -> None:
        """Execute multiple queries in a batch."""
        db_operations.labels(operation="batch").inc()
        try:
            with self.get_session() as session:
                session.execute(query, params_list)
        except SQLAlchemyError as e:
            logger.error("Batch execution failed", query=query, error=str(e))
            raise

    def create_tables(self) -> None:
        """Create database tables if they don't exist."""
        from .models import Base
        try:
            Base.metadata.create_all(self.engine)
            logger.info("Database tables created successfully")
        except SQLAlchemyError as e:
            logger.error("Failed to create database tables", error=str(e))
            raise

    def get_connection_stats(self) -> dict:
        """Get database connection pool statistics."""
        return {
            "pool_size": self.engine.pool.size(),
            "checkedin": self.engine.pool.checkedin(),
            "overflow": self.engine.pool.overflow(),
            "checkedout": self.engine.pool.checkedout(),
        } 