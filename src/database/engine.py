# trading_system/src/database/engine.py

from sqlalchemy import create_engine, exc, event, text
import logging
from sqlalchemy.pool import NullPool

logger = logging.getLogger(__name__)

def create_db_engine(config):
    """
    Create a database engine with connection validation.
    
    Args:
        config: An object with the following attributes:
            - url (str): The database URL.
            - pool_size (int): Number of connections in the pool.
            - max_overflow (int): Extra connections allowed beyond the pool size.

    Returns:
        engine: A SQLAlchemy engine instance.

    Raises:
        SQLAlchemyError: If the engine creation fails.
    """
    try:
        engine = create_engine(
            config.url,
            poolclass=NullPool,  # Disable connection pooling
            connect_args={"check_same_thread": False},  # Allow multi-thread access
        )
        
        # Add connection validation to ensure a healthy connection
        @event.listens_for(engine, "engine_connect")
        def ping_connection(connection, branch):
            if branch:  # Skip validation for nested transactions
                return
            try:
                connection.scalar(text("SELECT 1"))  # Execute a lightweight query
            except exc.DBAPIError as e:
                if e.connection_invalidated:  # Handle invalid connections
                    logger.error("Database connection was invalidated.")
                    raise
                else:
                    logger.error("Failed to validate database connection.")
                    raise

        logger.info("Database connection established successfully")
        return engine

    except exc.SQLAlchemyError as e:
        logger.error(f"Failed to establish database connection: {e}")
        raise
