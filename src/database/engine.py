# trading_system/src/database/engine.py

from sqlalchemy import create_engine, exc, event, text
import logging
from sqlalchemy.pool import QueuePool

logger = logging.getLogger(__name__)

def create_db_engine(config):
    """
    Create a DuckDB database engine with connection pooling suitable for
    multi-threaded writes.
    
    Args:
        config: An object with attributes:
            - url (str): The DuckDB database URL.
            - pool_size (int): Number of connections in the pool.
            - max_overflow (int): Extra connections allowed beyond the pool size.

    Returns:
        engine: A SQLAlchemy engine instance.
    """
    try:
        # Configure engine with explicit pooling settings
        engine = create_engine(
            config.url,
            poolclass=QueuePool,
            pool_size=config.pool_size,
            max_overflow=config.max_overflow,
            pool_timeout=30,  # Wait up to 30 seconds for a connection
            pool_pre_ping=True,  # Check connection health before using
            isolation_level='AUTOCOMMIT'  # Use autocommit mode for DuckDB
        )
        
        # Optional: Validate connection health
        @event.listens_for(engine, "engine_connect")
        def ping_connection(connection, branch):
            if branch:  # Skip validation for sub-transactions
                return
            try:
                connection.scalar(text("SELECT 1"))
            except exc.DBAPIError as e:
                if e.connection_invalidated:
                    logger.error("Database connection was invalidated.")
                    raise
                else:
                    logger.error("Failed to validate database connection.")
                    raise

        logger.info("DuckDB connection established successfully")
        return engine

    except exc.SQLAlchemyError as e:
        logger.error(f"Failed to establish database connection: {e}")
        raise