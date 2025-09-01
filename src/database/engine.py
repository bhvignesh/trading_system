# trading_system/src/database/engine.py

import logging
import time

from sqlalchemy import Index, create_engine, event, exc, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool, StaticPool

from .config import DatabaseConfig

logger = logging.getLogger(__name__)

def create_db_engine(config: DatabaseConfig):
    """
    Create and configure a SQLAlchemy engine for the trading system database.

    This function initializes a database engine with configurations tailored for
    either SQLite or PostgreSQL. It sets up connection pooling options, applies
    SQLite-specific optimizations (when applicable), monitors query performance,
    and automatically creates indexes to enhance common query operations.

    Args:
        config (DatabaseConfig): Configuration object containing database URL and pooling parameters.

    Returns:
        Engine: A SQLAlchemy Engine instance configured with the specified settings.

    Raises:
        SQLAlchemyError: If the engine creation fails due to a database connection issue.
    """
    is_sqlite = config.url.startswith("sqlite")
    
    # Start with common, always-valid engine arguments
    engine_kwargs = {
        "echo": config.echo,
    }
    
    # Configure engine parameters based on database type
    if is_sqlite:
        engine_kwargs.update({
            "poolclass": StaticPool,
            "connect_args": {
                "check_same_thread": False,
                "timeout": 60,
                "isolation_level": None  # Enable autocommit mode
            }
        })
    else: # This block is for PostgreSQL or other server-based DBs
        engine_kwargs.update({
            "poolclass": QueuePool,
            "pool_size": config.pool_size,
            "max_overflow": config.max_overflow,
            "pool_timeout": config.pool_timeout, # MOVED arugment
            "pool_recycle": config.pool_recycle, # MOVED arugment
        })
    
    try:
        engine = create_engine(config.url, **engine_kwargs)
        
        if is_sqlite:
            _configure_sqlite_optimizations(engine)
        
        _setup_performance_monitoring(engine)
        _create_indexes(engine)
        
        logger.info("Database engine created with optimizations")
        return engine
        
    except exc.SQLAlchemyError as e:
        logger.error(f"Failed to create database engine: {e}")
        raise

def _configure_sqlite_optimizations(engine):
    """
    Apply SQLite-specific performance enhancements using PRAGMA settings.

    When establishing a connection to a SQLite database, this function executes
    several PRAGMA commands to enable Write-Ahead Logging (WAL) mode, adjust
    synchronous settings, set cache size, and optimize memory usage. These configurations
    are applied to improve concurrency and overall performance.

    Args:
        engine (Engine): The SQLAlchemy engine instance to configure.
    """
    
    @event.listens_for(engine, "connect")
    def set_sqlite_pragma(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        
        # Performance and concurrency optimizations
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL") 
        cursor.execute("PRAGMA cache_size=10000")
        cursor.execute("PRAGMA temp_store=MEMORY")
        cursor.execute("PRAGMA mmap_size=268435456")  # 256MB
        cursor.execute("PRAGMA busy_timeout=60000")
        cursor.execute("PRAGMA foreign_keys=ON")
        
        # Query optimizations
        cursor.execute("PRAGMA optimize")
        cursor.close()

def _setup_performance_monitoring(engine):
    """
    Set up performance monitoring for database operations.

    Registers event listeners to track the execution time of SQL queries. If a query
    takes longer than 1 second, this mechanism logs a warning with a snippet of the query
    to help diagnose potential performance issues.

    Args:
        engine (Engine): The SQLAlchemy engine instance to monitor.
    """
    
    @event.listens_for(engine, "before_cursor_execute")
    def receive_before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        context._query_start_time = time.time()
    
    @event.listens_for(engine, "after_cursor_execute")
    def receive_after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        total = time.time() - context._query_start_time
        if total > 1.0:  # Log slow queries
            logger.warning(f"Slow query ({total:.2f}s): {statement[:100]}...")

def _create_indexes(engine):
    """
    Create performance-enhancing indexes for frequently accessed database columns.

    Executes SQL commands to create indexes on tables such as daily_prices, company_info,
    balance_sheet, income_statement, cash_flow, refresh_state, and signals. The indexes
    are designed to optimize common query patterns. If an index cannot be created due to
    it already existing or another operational issue, the event is logged as a warning.

    Args:
        engine (Engine): The SQLAlchemy engine instance where indexes are applied.
    """
    indexes = [
        # Price data indexes
        "CREATE INDEX IF NOT EXISTS idx_daily_prices_ticker_date ON daily_prices(ticker, date)",
        "CREATE INDEX IF NOT EXISTS idx_daily_prices_date ON daily_prices(date)",
        "CREATE INDEX IF NOT EXISTS idx_daily_prices_ticker ON daily_prices(ticker)",
        
        # Company info indexes
        "CREATE INDEX IF NOT EXISTS idx_company_info_ticker ON company_info(ticker)",
        "CREATE INDEX IF NOT EXISTS idx_company_info_updated_at ON company_info(updated_at)",
        
        # Financial statements indexes
        "CREATE INDEX IF NOT EXISTS idx_balance_sheet_ticker_date ON balance_sheet(ticker, date)",
        "CREATE INDEX IF NOT EXISTS idx_income_statement_ticker_date ON income_statement(ticker, date)",
        "CREATE INDEX IF NOT EXISTS idx_cash_flow_ticker_date ON cash_flow(ticker, date)",
        
        # Refresh state indexes
        "CREATE INDEX IF NOT EXISTS idx_refresh_state_ticker ON refresh_state(ticker)",
        "CREATE INDEX IF NOT EXISTS idx_refresh_state_last_full_refresh ON refresh_state(last_full_refresh)",
        
        # Signal storage indexes
        "CREATE INDEX IF NOT EXISTS idx_signals_ticker_date ON signals(ticker, date)",
        "CREATE INDEX IF NOT EXISTS idx_signals_strategy ON signals(strategy_name)",
    ]
    
    with engine.connect() as conn:
        transaction = conn.begin() # Start a transaction
        try:
            for index_sql in indexes:
                try:
                    conn.execute(text(index_sql))
                except exc.OperationalError as e:
                    if "already exists" not in str(e).lower() and "duplicate" not in str(e).lower():
                        logger.warning(f"Failed to create index: {e}")
            transaction.commit() # Commit all changes
        except Exception:
            transaction.rollback() # Rollback on any error
            raise

