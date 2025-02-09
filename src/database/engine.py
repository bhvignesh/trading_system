# trading_system/src/database/engine.py

from sqlalchemy import create_engine, exc, event, text
import logging
from sqlalchemy.pool import QueuePool

logger = logging.getLogger(__name__)

def create_db_engine(config):
    """
    Create a DuckDB database engine with connection pooling suitable for
    multi-threaded writes.
    """
    try:
        # Configure engine with explicit pooling settings
        engine = create_engine(
            config.url,
            poolclass=QueuePool,
            pool_size=config.pool_size,
            max_overflow=config.max_overflow,
            pool_timeout=30,
            pool_pre_ping=True,
            isolation_level='AUTOCOMMIT'  # Set default isolation level
        )
        
        @event.listens_for(engine, "connect")
        def set_isolation_level(dbapi_connection, connection_record):
            # Set isolation level at connection creation time
            dbapi_connection.isolation_level = None  # Use autocommit by default
        
        logger.info("DuckDB connection established successfully")
        return engine

    except exc.SQLAlchemyError as e:
        logger.error(f"Failed to establish database connection: {e}")
        raise